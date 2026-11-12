//! C-STORE / C-ECHO service class provider.
//!
//! One tokio task per association. Command PDVs are reassembled by
//! [`PdvAssembler`]; data PDVs stream straight into a [`TempBlob`] behind a
//! Part-10 prefix, so an instance is never held in memory.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dicom_dictionary_std::uids;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::Pdu;
use dicom_ul::association::server::{AccessControl, DefaultNegotiation};
use dicom_ul::association::{Association, AsyncServerAssociation, ServerAssociationOptions};
use dicom_ul::pdu::{
    AssociationRJServiceUserReason, PDataValue, PDataValueType, PresentationContextNegotiated,
    PresentationContextResultReason, UserIdentity,
};
use slicestream_store::{Expectations, IngestSource, Ingestor, TempBlob, write_part10_prefix};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, field, info, info_span, warn};

use crate::{
    DimseRequest, PdvAssembler, PdvEvent, ScpConfig, echo_rsp, failure_rsp, parse_command,
    store_rsp,
};

/// How long in-flight associations may run after shutdown is requested.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// Largest command set we reassemble.
const MAX_COMMAND_BYTES: usize = 64 * 1024;
/// Back-off after a failed `accept` (e.g. out of file descriptors).
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

const STATUS_SUCCESS: u16 = 0x0000;
const STATUS_OUT_OF_RESOURCES: u16 = 0xA700;
const STATUS_CANNOT_UNDERSTAND: u16 = 0xC000;
const STATUS_UNRECOGNIZED_OPERATION: u16 = 0x0211;

type Options = ServerAssociationOptions<'static, PeerAllowList, DefaultNegotiation>;
type ServerAssoc = AsyncServerAssociation<TcpStream>;

/// AE title checks: Called AE Title (optional) and a Calling AE allow-list.
#[derive(Debug, Clone)]
pub struct PeerAllowList {
    require_called_ae_title: bool,
    allowed_peers: Vec<String>,
}

impl AccessControl for PeerAllowList {
    fn check_access(
        &self,
        this_ae_title: &str,
        calling_ae_title: &str,
        called_ae_title: &str,
        _user_identity: Option<&UserIdentity>,
    ) -> Result<(), AssociationRJServiceUserReason> {
        if self.require_called_ae_title && called_ae_title.trim() != this_ae_title.trim() {
            return Err(AssociationRJServiceUserReason::CalledAETitleNotRecognized);
        }
        if !self.allowed_peers.is_empty()
            && !self
                .allowed_peers
                .iter()
                .any(|p| p.trim() == calling_ae_title.trim())
        {
            return Err(AssociationRJServiceUserReason::CallingAETitleNotRecognized);
        }
        Ok(())
    }
}

/// A bound, not yet running, storage SCP.
pub struct StoreScp {
    cfg: ScpConfig,
    ingestor: Arc<Ingestor>,
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl StoreScp {
    pub async fn bind(cfg: ScpConfig, ingestor: Arc<Ingestor>) -> io::Result<StoreScp> {
        let listener = TcpListener::bind(cfg.addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(StoreScp {
            cfg,
            ingestor,
            listener,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept associations until `shutdown` is cancelled, then let in-flight
    /// associations finish for up to 30 s before aborting them.
    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        let options = Arc::new(build_options(&self.cfg));
        let timeouts = Timeouts {
            handshake: Duration::from_secs(self.cfg.handshake_timeout_secs),
            idle: Duration::from_secs(self.cfg.idle_timeout_secs),
        };
        let permits = Arc::new(Semaphore::new(self.cfg.max_associations));
        let mut tasks = JoinSet::new();
        info!(addr = %self.local_addr, ae_title = %self.cfg.ae_title, "DIMSE SCP listening");

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                Some(res) = tasks.join_next(), if !tasks.is_empty() => log_join(res),
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                            warn!(%peer, max = self.cfg.max_associations,
                                "association limit reached; dropping connection");
                            drop(stream);
                            continue;
                        };
                        tasks.spawn(serve(
                            stream,
                            peer,
                            Arc::clone(&options),
                            timeouts,
                            Arc::clone(&self.ingestor),
                            permit,
                        ));
                    }
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                    }
                },
            }
        }

        drop(self.listener);
        if !tasks.is_empty() {
            info!(in_flight = tasks.len(), "DIMSE SCP draining associations");
        }
        let drain = async {
            while let Some(res) = tasks.join_next().await {
                log_join(res);
            }
        };
        if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
            warn!(
                remaining = tasks.len(),
                "shutdown grace expired; aborting associations"
            );
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        info!("DIMSE SCP stopped");
        Ok(())
    }
}

fn log_join(res: Result<(), tokio::task::JoinError>) {
    if let Err(e) = res
        && e.is_panic()
    {
        warn!(error = %e, "association task panicked");
    }
}

fn build_options(cfg: &ScpConfig) -> Options {
    let mut opts = ServerAssociationOptions::new()
        .ae_title(cfg.ae_title.clone())
        .ae_access_control(PeerAllowList {
            require_called_ae_title: cfg.require_called_ae_title,
            allowed_peers: cfg.allowed_peers.clone(),
        })
        .promiscuous(true)
        .with_abstract_syntax(uids::VERIFICATION)
        .max_pdu_length(cfg.max_pdu_length)
        .read_timeout(Duration::from_secs(cfg.read_timeout_secs))
        .write_timeout(Duration::from_secs(cfg.write_timeout_secs));
    for ts in TransferSyntaxRegistry.iter() {
        opts = opts.with_transfer_syntax(ts.uid());
    }
    opts
}

/// Association-level timeouts (see [`ScpConfig`]).
#[derive(Debug, Clone, Copy)]
struct Timeouts {
    handshake: Duration,
    idle: Duration,
}

async fn serve(
    stream: TcpStream,
    peer: SocketAddr,
    options: Arc<Options>,
    timeouts: Timeouts,
    ingestor: Arc<Ingestor>,
    _permit: OwnedSemaphorePermit,
) {
    let span = info_span!(
        "association",
        %peer,
        calling_ae = field::Empty,
        called_ae = field::Empty
    );
    async move {
        // The permit is already held, so bound the whole handshake: a silent
        // connection must not keep its slot for longer than this.
        let assoc =
            match tokio::time::timeout(timeouts.handshake, options.establish_async(stream)).await {
                Ok(Ok(a)) => a,
                Ok(Err(e)) => {
                    info!(error = %e, "association not established");
                    return;
                }
                Err(_) => {
                    info!("handshake timed out; dropping connection");
                    return;
                }
            };
        let calling_ae = assoc.peer_ae_title().trim().to_owned();
        let called_ae = assoc.called_ae_title().trim().to_owned();
        let span = tracing::Span::current();
        span.record("calling_ae", calling_ae.as_str());
        span.record("called_ae", called_ae.as_str());
        info!("association established");
        let session = Session {
            ingestor,
            contexts: assoc.presentation_contexts().to_vec(),
            calling_ae,
            called_ae,
            assembler: PdvAssembler::new(MAX_COMMAND_BYTES),
            state: State::Idle,
            idle_timeout: timeouts.idle,
            commands: 0,
        };
        session.run(assoc).await;
    }
    .instrument(span)
    .await
}

/// Per-association DIMSE state.
enum State {
    Idle,
    /// Spooling the data set of a C-STORE.
    Receiving {
        pc_id: u8,
        temp: Box<TempBlob>,
        req: StoreRequest,
    },
    /// Reading and dropping a data set; `response` goes out after the last PDV.
    Discarding {
        pc_id: u8,
        response: Vec<u8>,
    },
}

struct StoreRequest {
    message_id: u16,
    sop_class_uid: String,
    sop_instance_uid: String,
}

impl StoreRequest {
    fn response(&self, status: u16) -> Vec<u8> {
        store_rsp(
            self.message_id,
            &self.sop_class_uid,
            &self.sop_instance_uid,
            status,
        )
    }
}

/// The peer broke the DIMSE protocol; the association is aborted.
struct Violation(&'static str);

struct Session {
    ingestor: Arc<Ingestor>,
    contexts: Vec<PresentationContextNegotiated>,
    calling_ae: String,
    called_ae: String,
    assembler: PdvAssembler,
    state: State,
    /// Abort when no DIMSE command arrives for this long while idle.
    idle_timeout: Duration,
    /// Complete command sets received so far.
    commands: u64,
}

impl Session {
    async fn run(mut self, mut assoc: ServerAssoc) {
        // The idle clock runs only between operations (state `Idle`, which
        // includes a partially received command). While a data set is being
        // received, the per-PDU read timeout applies instead.
        let mut idle_deadline = Instant::now() + self.idle_timeout;
        loop {
            let received = if matches!(self.state, State::Idle) {
                match tokio::time::timeout_at(idle_deadline, assoc.receive()).await {
                    Ok(r) => r,
                    Err(_) => {
                        warn!(
                            idle_timeout_secs = self.idle_timeout.as_secs(),
                            "no DIMSE command within the idle timeout; aborting association"
                        );
                        let _ = assoc.abort().await;
                        return;
                    }
                }
            } else {
                assoc.receive().await
            };
            let pdu = match received {
                Ok(pdu) => pdu,
                Err(e) => {
                    // Peer disconnect or timeout; any TempBlob is dropped.
                    info!(error = %e, "association ended");
                    return;
                }
            };
            let commands_before = self.commands;
            let was_idle = matches!(self.state, State::Idle);
            match pdu {
                Pdu::PData { data } => {
                    for pdv in data {
                        match self.on_pdv(pdv).await {
                            Ok(None) => {}
                            Ok(Some((pc_id, bytes))) => {
                                if let Err(e) = assoc.send(&command_pdu(pc_id, bytes)).await {
                                    info!(error = %e, "failed to send response");
                                    return;
                                }
                            }
                            Err(Violation(reason)) => {
                                warn!(reason, "protocol violation; aborting association");
                                let _ = assoc.abort().await;
                                return;
                            }
                        }
                    }
                    // Restart the idle clock after any DIMSE activity: a
                    // complete command, or a PDU of an operation in progress
                    // (including the one that finished it).
                    if !was_idle
                        || self.commands != commands_before
                        || !matches!(self.state, State::Idle)
                    {
                        idle_deadline = Instant::now() + self.idle_timeout;
                    }
                }
                Pdu::ReleaseRQ => {
                    if !matches!(self.state, State::Idle) {
                        warn!("release requested mid-instance; discarding it");
                    }
                    let _ = assoc.send(&Pdu::ReleaseRP).await;
                    info!("association released");
                    return;
                }
                Pdu::AbortRQ { source } => {
                    info!(?source, "association aborted by peer");
                    return;
                }
                other => {
                    warn!(pdu = ?pdu_kind(&other), "unexpected PDU; aborting association");
                    let _ = assoc.abort().await;
                    return;
                }
            }
        }
    }

    /// Handle one PDV; returns a command response to send, if any.
    async fn on_pdv(&mut self, pdv: PDataValue) -> Result<Option<(u8, Vec<u8>)>, Violation> {
        let idle = matches!(self.state, State::Idle);
        match pdv.value_type {
            PDataValueType::Command if !idle => {
                return Err(Violation("command PDV while a data set is pending"));
            }
            // Covers data before any command and data while a command is
            // still being reassembled (the assembler passes data through).
            PDataValueType::Data if idle => {
                return Err(Violation("data PDV without a preceding command"));
            }
            _ => {}
        }
        let event = match self.assembler.push(pdv) {
            Ok(Some(ev)) => ev,
            Ok(None) => return Ok(None),
            Err(e) => {
                warn!(error = %e, "bad command PDV");
                return Err(Violation("malformed command fragments"));
            }
        };
        match event {
            PdvEvent::Command { pc_id, bytes } => {
                self.commands += 1;
                self.on_command(pc_id, &bytes).await
            }
            PdvEvent::Data { pc_id, chunk, last } => self.on_data(pc_id, &chunk, last).await,
        }
    }

    async fn on_command(
        &mut self,
        pc_id: u8,
        bytes: &[u8],
    ) -> Result<Option<(u8, Vec<u8>)>, Violation> {
        let ts = self.accepted_ts(pc_id).ok_or(Violation(
            "command on a presentation context that was not accepted",
        ))?;
        let req = parse_command(bytes).map_err(|e| {
            warn!(error = %e, "unparseable command set");
            Violation("unparseable command set")
        })?;
        match req {
            DimseRequest::Echo {
                message_id,
                sop_class_uid,
            } => {
                debug!(message_id, "C-ECHO");
                Ok(Some((pc_id, echo_rsp(message_id, &sop_class_uid))))
            }
            DimseRequest::Store {
                message_id,
                sop_class_uid,
                sop_instance_uid,
                has_dataset,
            } => {
                let req = StoreRequest {
                    message_id,
                    sop_class_uid,
                    sop_instance_uid,
                };
                debug!(message_id, sop_instance_uid = %req.sop_instance_uid, "C-STORE");
                if !has_dataset {
                    return Ok(Some((pc_id, req.response(STATUS_CANNOT_UNDERSTAND))));
                }
                let ts = ts.to_owned();
                self.state = match self.start_spool(&req, &ts).await {
                    Ok(temp) => State::Receiving {
                        pc_id,
                        temp: Box::new(temp),
                        req,
                    },
                    Err(status) => State::Discarding {
                        pc_id,
                        response: req.response(status),
                    },
                };
                Ok(None)
            }
            DimseRequest::Other {
                command_field,
                message_id,
                sop_class_uid,
                has_dataset,
            } => {
                info!(command_field, message_id, "unsupported DIMSE command");
                let response = failure_rsp(
                    command_field,
                    message_id,
                    sop_class_uid.as_deref(),
                    STATUS_UNRECOGNIZED_OPERATION,
                );
                if has_dataset {
                    self.state = State::Discarding { pc_id, response };
                    Ok(None)
                } else {
                    Ok(Some((pc_id, response)))
                }
            }
        }
    }

    /// Open a spool file and write the Part-10 prefix. On error returns the
    /// C-STORE status to answer with once the data set has been drained.
    async fn start_spool(&self, req: &StoreRequest, ts: &str) -> Result<TempBlob, u16> {
        let mut temp = self.ingestor.create_temp().map_err(|e| {
            warn!(error = %e, "cannot create spool file");
            STATUS_OUT_OF_RESOURCES
        })?;
        write_part10_prefix(
            &mut temp,
            &req.sop_class_uid,
            &req.sop_instance_uid,
            ts,
            Some(&self.calling_ae),
        )
        .await
        .map_err(|e| {
            warn!(error = %e, "cannot write Part-10 prefix");
            STATUS_OUT_OF_RESOURCES
        })?;
        Ok(temp)
    }

    async fn on_data(
        &mut self,
        pc_id: u8,
        chunk: &[u8],
        last: bool,
    ) -> Result<Option<(u8, Vec<u8>)>, Violation> {
        let expected = match &self.state {
            State::Receiving { pc_id, .. } | State::Discarding { pc_id, .. } => *pc_id,
            State::Idle => unreachable!("checked in on_pdv"),
        };
        if pc_id != expected {
            return Err(Violation("data PDV on a different presentation context"));
        }

        if let State::Receiving { temp, req, .. } = &mut self.state
            && let Err(e) = temp.write_chunk(chunk).await
        {
            warn!(error = %e, sop_instance_uid = %req.sop_instance_uid, "spooling failed; discarding instance");
            let response = req.response(STATUS_OUT_OF_RESOURCES);
            // Dropping the TempBlob removes its file.
            self.state = State::Discarding { pc_id, response };
        }

        if !last {
            return Ok(None);
        }
        let response = match std::mem::replace(&mut self.state, State::Idle) {
            State::Receiving { temp, req, .. } => {
                let status = self.finish_store(*temp, &req).await;
                req.response(status)
            }
            State::Discarding { response, .. } => response,
            State::Idle => unreachable!("state was not idle above"),
        };
        Ok(Some((pc_id, response)))
    }

    async fn finish_store(&self, temp: TempBlob, req: &StoreRequest) -> u16 {
        let spooled = match temp.finish().await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "finishing spool file failed");
                return STATUS_OUT_OF_RESOURCES;
            }
        };
        let source = IngestSource::Dimse {
            calling_ae: self.calling_ae.clone(),
            called_ae: self.called_ae.clone(),
        };
        let expect = Expectations {
            sop_class_uid: Some(req.sop_class_uid.clone()),
            sop_instance_uid: Some(req.sop_instance_uid.clone()),
            ..Expectations::default()
        };
        match self.ingestor.ingest(spooled, source, expect).await {
            Ok(stored) => {
                info!(
                    sop_instance_uid = %stored.sop_instance_uid,
                    duplicate = stored.duplicate,
                    "instance stored"
                );
                STATUS_SUCCESS
            }
            Err(e) => {
                warn!(error = %e, sop_instance_uid = %req.sop_instance_uid, "instance rejected");
                e.status()
            }
        }
    }

    /// Transfer syntax UID of an accepted presentation context.
    fn accepted_ts(&self, pc_id: u8) -> Option<&str> {
        self.contexts
            .iter()
            .find(|pc| pc.id == pc_id && pc.reason == PresentationContextResultReason::Acceptance)
            .map(|pc| pc.transfer_syntax.trim_end_matches(['\0', ' ']))
    }
}

fn command_pdu(pc_id: u8, data: Vec<u8>) -> Pdu {
    Pdu::PData {
        data: vec![PDataValue {
            presentation_context_id: pc_id,
            value_type: PDataValueType::Command,
            is_last: true,
            data,
        }],
    }
}

fn pdu_kind(pdu: &Pdu) -> &'static str {
    match pdu {
        Pdu::AssociationRQ(_) => "A-ASSOCIATE-RQ",
        Pdu::AssociationAC(_) => "A-ASSOCIATE-AC",
        Pdu::AssociationRJ(_) => "A-ASSOCIATE-RJ",
        Pdu::PData { .. } => "P-DATA-TF",
        Pdu::ReleaseRQ => "A-RELEASE-RQ",
        Pdu::ReleaseRP => "A-RELEASE-RP",
        Pdu::AbortRQ { .. } => "A-ABORT",
        _ => "unknown",
    }
}
