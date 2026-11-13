//! End-to-end tests for the C-STORE / C-ECHO SCP, driven by a dicom-ul SCU.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use dicom_core::{DataElement, VR, dicom_value};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{InMemDicomObject, open_file};
use dicom_transfer_syntax_registry::entries::{
    EXPLICIT_VR_LITTLE_ENDIAN, IMPLICIT_VR_LITTLE_ENDIAN,
};
use dicom_ul::association::client::AsyncClientAssociation;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::{ClientAssociationOptions, Pdu};
use slicestream_dimse::{ScpConfig, StoreScp};
use slicestream_store::{Ingestor, StorageConfig};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type Scu = AsyncClientAssociation<TcpStream>;

struct Harness {
    addr: SocketAddr,
    ingestor: Arc<Ingestor>,
    root: PathBuf,
    shutdown: CancellationToken,
    server: JoinHandle<std::io::Result<()>>,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn start(
        cfg: impl FnOnce(&mut ScpConfig),
        storage: impl FnOnce(&mut StorageConfig),
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("store");
        let mut scfg = StorageConfig {
            root: root.clone(),
            ..StorageConfig::default()
        };
        storage(&mut scfg);
        let ingestor = Ingestor::open(scfg).await.unwrap();
        let mut c = ScpConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            ..ScpConfig::default()
        };
        cfg(&mut c);
        let scp = StoreScp::bind(c, Arc::clone(&ingestor)).await.unwrap();
        let addr = scp.local_addr();
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(scp.run(shutdown.clone()));
        Harness {
            addr,
            ingestor,
            root,
            shutdown,
            server,
            _dir: dir,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(40), self.server)
            .await
            .expect("server stops")
            .unwrap()
            .unwrap();
    }
}

fn options(calling: &str, called: &str) -> ClientAssociationOptions<'static> {
    ClientAssociationOptions::new()
        .calling_ae_title(calling.to_string())
        .called_ae_title(called.to_string())
        .with_presentation_context(
            uids::CT_IMAGE_STORAGE,
            vec![EXPLICIT_VR_LITTLE_ENDIAN.uid()],
        )
        .with_presentation_context(uids::VERIFICATION, vec![IMPLICIT_VR_LITTLE_ENDIAN.uid()])
        .read_timeout(Duration::from_secs(10))
        .connection_timeout(Duration::from_secs(10))
}

async fn connect(addr: SocketAddr) -> Scu {
    options("TESTSCU", "SLICESTREAM")
        .establish_async(addr)
        .await
        .expect("association accepted")
}

fn pc_for(scu: &Scu, abstract_syntax: &str) -> u8 {
    scu.presentation_contexts()
        .iter()
        .find(|pc| pc.abstract_syntax.trim_end_matches('\0') == abstract_syntax)
        .expect("presentation context accepted")
        .id
}

fn command_bytes(elems: Vec<DataElement<InMemDicomObject>>) -> Vec<u8> {
    let mut buf = Vec::new();
    InMemDicomObject::command_from_element_iter(elems)
        .write_dataset_with_ts(&mut buf, &IMPLICIT_VR_LITTLE_ENDIAN.erased())
        .unwrap();
    buf
}

fn us(tag: dicom_core::Tag, v: u16) -> DataElement<InMemDicomObject> {
    DataElement::new(tag, VR::US, dicom_value!(U16, [v]))
}

fn ui(tag: dicom_core::Tag, v: &str) -> DataElement<InMemDicomObject> {
    DataElement::new(tag, VR::UI, dicom_value!(Str, v))
}

fn store_rq(message_id: u16, sop_class: &str, sop_instance: &str) -> Vec<u8> {
    command_bytes(vec![
        ui(tags::AFFECTED_SOP_CLASS_UID, sop_class),
        us(tags::COMMAND_FIELD, 0x0001),
        us(tags::MESSAGE_ID, message_id),
        us(tags::PRIORITY, 0),
        us(tags::COMMAND_DATA_SET_TYPE, 0x0000),
        ui(tags::AFFECTED_SOP_INSTANCE_UID, sop_instance),
    ])
}

fn echo_rq(message_id: u16) -> Vec<u8> {
    command_bytes(vec![
        ui(tags::AFFECTED_SOP_CLASS_UID, uids::VERIFICATION),
        us(tags::COMMAND_FIELD, 0x0030),
        us(tags::MESSAGE_ID, message_id),
        us(tags::COMMAND_DATA_SET_TYPE, 0x0101),
    ])
}

fn pdv(pc_id: u8, value_type: PDataValueType, is_last: bool, data: Vec<u8>) -> Pdu {
    Pdu::PData {
        data: vec![PDataValue {
            presentation_context_id: pc_id,
            value_type,
            is_last,
            data,
        }],
    }
}

/// A phantom instance: SOP class, SOP instance and dataset bytes (Explicit VR LE).
struct Instance {
    sop_class: String,
    sop_instance: String,
    dataset: Vec<u8>,
}

fn load_instances(dir: &Path) -> Vec<Instance> {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            let obj = open_file(&p).unwrap();
            let sop_class = obj.meta().media_storage_sop_class_uid().to_string();
            let sop_instance = obj.meta().media_storage_sop_instance_uid().to_string();
            let mut dataset = Vec::new();
            obj.write_dataset_with_ts(&mut dataset, &EXPLICIT_VR_LITTLE_ENDIAN.erased())
                .unwrap();
            Instance {
                sop_class: sop_class.trim_end_matches('\0').to_string(),
                sop_instance: sop_instance.trim_end_matches('\0').to_string(),
                dataset,
            }
        })
        .collect()
}

fn phantom(dir: &Path, slices: usize, size: u16) -> Vec<Instance> {
    phantom::write_phantom(dir, slices, size).unwrap();
    load_instances(dir)
}

async fn send_dataset(scu: &mut Scu, pc_id: u8, dataset: &[u8]) {
    // Leave room for the PDV item header (4-byte length + id + flags).
    let max = (scu.acceptor_max_pdu_length() as usize - 6).min(512);
    let chunks: Vec<&[u8]> = dataset.chunks(max).collect();
    let n = chunks.len();
    for (i, c) in chunks.into_iter().enumerate() {
        scu.send(&pdv(pc_id, PDataValueType::Data, i + 1 == n, c.to_vec()))
            .await
            .unwrap();
    }
}

/// Read the next DIMSE response and return its Status.
async fn read_status(scu: &mut Scu) -> u16 {
    let mut cmd = Vec::new();
    loop {
        match scu.receive().await.unwrap() {
            Pdu::PData { data } => {
                for v in data {
                    assert_eq!(v.value_type, PDataValueType::Command);
                    cmd.extend_from_slice(&v.data);
                    if v.is_last {
                        let obj = InMemDicomObject::read_dataset_with_ts(
                            &cmd[..],
                            &IMPLICIT_VR_LITTLE_ENDIAN.erased(),
                        )
                        .unwrap();
                        return obj.element(tags::STATUS).unwrap().to_int::<u16>().unwrap();
                    }
                }
            }
            other => panic!("unexpected PDU {other:?}"),
        }
    }
}

/// Send a C-STORE with the given command SOP Instance UID and return the status.
async fn store(scu: &mut Scu, msg_id: u16, inst: &Instance, command_sop_instance: &str) -> u16 {
    let pc_id = pc_for(scu, &inst.sop_class);
    let cmd = store_rq(msg_id, &inst.sop_class, command_sop_instance);
    // Split the command across two PDVs to exercise reassembly.
    let half = cmd.len() / 2;
    scu.send(&pdv(
        pc_id,
        PDataValueType::Command,
        false,
        cmd[..half].to_vec(),
    ))
    .await
    .unwrap();
    scu.send(&pdv(
        pc_id,
        PDataValueType::Command,
        true,
        cmd[half..].to_vec(),
    ))
    .await
    .unwrap();
    send_dataset(scu, pc_id, &inst.dataset).await;
    read_status(scu).await
}

fn dir_entries(p: &Path) -> usize {
    fn walk(p: &Path, n: &mut usize) {
        for e in std::fs::read_dir(p).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                walk(&e.path(), n);
            } else {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(p, &mut n);
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_echo_succeeds() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let mut scu = connect(h.addr).await;
    let pc_id = pc_for(&scu, uids::VERIFICATION);
    scu.send(&pdv(pc_id, PDataValueType::Command, true, echo_rq(1)))
        .await
        .unwrap();
    assert_eq!(read_status(&mut scu).await, 0x0000);
    scu.release().await.unwrap();
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_store_three_slices() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let src = tempfile::tempdir().unwrap();
    let instances = phantom(src.path(), 3, 16);
    let mut scu = connect(h.addr).await;
    for (i, inst) in instances.iter().enumerate() {
        let status = store(&mut scu, i as u16 + 1, inst, &inst.sop_instance).await;
        assert_eq!(status, 0x0000, "instance {i}");
    }
    scu.release().await.unwrap();

    let series = h.ingestor.index().list_series().await.unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].instance_count, 3);
    for inst in &instances {
        let row = h
            .ingestor
            .index()
            .instance(&inst.sop_instance)
            .await
            .unwrap()
            .expect("indexed");
        assert_eq!(row.source, "dimse");
        assert_eq!(row.calling_ae.as_deref(), Some("TESTSCU"));
    }
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_called_ae_rejected() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let res = options("TESTSCU", "NOPE").establish_async(h.addr).await;
    assert!(res.is_err(), "association must be rejected");
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_not_allowed_rejected() {
    let h = Harness::start(|c| c.allowed_peers = vec!["CT01".into()], |_| {}).await;
    let res = options("TESTSCU", "SLICESTREAM")
        .establish_async(h.addr)
        .await;
    assert!(res.is_err(), "association must be rejected");
    // The listed peer is accepted.
    let ok = options("CT01", "SLICESTREAM").establish_async(h.addr).await;
    ok.expect("allowed peer accepted").release().await.unwrap();
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uid_mismatch_returns_a900() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let src = tempfile::tempdir().unwrap();
    let instances = phantom(src.path(), 1, 16);
    let mut scu = connect(h.addr).await;
    let status = store(&mut scu, 1, &instances[0], "2.25.999999").await;
    assert_eq!(status, 0xA900);
    scu.release().await.unwrap();

    assert!(
        h.ingestor
            .index()
            .instance(&instances[0].sop_instance)
            .await
            .unwrap()
            .is_none()
    );
    assert!(h.ingestor.index().list_series().await.unwrap().is_empty());
    assert_eq!(dir_entries(&h.root.join("objects")), 0);
    assert_eq!(dir_entries(&h.root.join("tmp")), 0);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversize_then_normal_on_same_association() {
    const LIMIT: u64 = 1600;
    let h = Harness::start(|_| {}, |s| s.max_instance_bytes = LIMIT).await;
    let big_dir = tempfile::tempdir().unwrap();
    let small_dir = tempfile::tempdir().unwrap();
    let big = phantom(big_dir.path(), 1, 32);
    let small = phantom(small_dir.path(), 1, 8);
    // The spooled size is the dataset plus a Part-10 prefix of a few hundred
    // bytes (132-byte preamble + meta group).
    assert!(
        big[0].dataset.len() as u64 > LIMIT,
        "big must exceed the limit"
    );
    assert!(
        small[0].dataset.len() as u64 + 400 < LIMIT,
        "small ({} bytes) must fit with its prefix",
        small[0].dataset.len()
    );

    let mut scu = connect(h.addr).await;
    assert_eq!(
        store(&mut scu, 1, &big[0], &big[0].sop_instance).await,
        0xA700
    );
    assert_eq!(dir_entries(&h.root.join("tmp")), 0);
    assert_eq!(
        store(&mut scu, 2, &small[0], &small[0].sop_instance).await,
        0x0000
    );
    scu.release().await.unwrap();

    let series = h.ingestor.index().list_series().await.unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].instance_count, 1);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_mid_instance_leaves_nothing() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let src = tempfile::tempdir().unwrap();
    let instances = phantom(src.path(), 1, 32);
    let inst = &instances[0];
    let mut scu = connect(h.addr).await;
    let pc_id = pc_for(&scu, &inst.sop_class);
    scu.send(&pdv(
        pc_id,
        PDataValueType::Command,
        true,
        store_rq(1, &inst.sop_class, &inst.sop_instance),
    ))
    .await
    .unwrap();
    scu.send(&pdv(
        pc_id,
        PDataValueType::Data,
        false,
        inst.dataset[..256].to_vec(),
    ))
    .await
    .unwrap();
    scu.abort().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    // Poll briefly in case the server is still unwinding on a slow machine.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while dir_entries(&h.root.join("tmp")) != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(dir_entries(&h.root.join("tmp")), 0);
    assert_eq!(dir_entries(&h.root.join("objects")), 0);
    assert!(h.ingestor.index().list_series().await.unwrap().is_empty());
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_associations_enforced() {
    let h = Harness::start(|c| c.max_associations = 1, |_| {}).await;
    let first = connect(h.addr).await;
    let second = options("TESTSCU", "SLICESTREAM")
        .establish_async(h.addr)
        .await;
    assert!(second.is_err(), "second association must fail");
    first.release().await.unwrap();
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_idle_association_completes() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let mut scu = connect(h.addr).await;
    let pc_id = pc_for(&scu, uids::VERIFICATION);
    scu.send(&pdv(pc_id, PDataValueType::Command, true, echo_rq(1)))
        .await
        .unwrap();
    assert_eq!(read_status(&mut scu).await, 0x0000);
    // Shut down while the association is still open, then release it: the
    // in-flight association finishes within the grace period.
    h.shutdown.cancel();
    scu.release().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), h.server)
        .await
        .expect("server stops")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_before_command_aborts() {
    let h = Harness::start(|_| {}, |_| {}).await;
    let mut scu = connect(h.addr).await;
    let pc_id = pc_for(&scu, uids::CT_IMAGE_STORAGE);
    // Start a command but send data before its last fragment.
    let cmd = store_rq(1, uids::CT_IMAGE_STORAGE, "2.25.1");
    scu.send(&pdv(
        pc_id,
        PDataValueType::Command,
        false,
        cmd[..10].to_vec(),
    ))
    .await
    .unwrap();
    scu.send(&pdv(pc_id, PDataValueType::Data, true, vec![0; 16]))
        .await
        .unwrap();
    match scu.receive().await {
        Ok(Pdu::AbortRQ { .. }) => {}
        other => panic!("expected A-ABORT, got {other:?}"),
    }
    assert_eq!(dir_entries(&h.root.join("tmp")), 0);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_connection_dropped_after_handshake_timeout() {
    use tokio::io::AsyncReadExt;

    let h = Harness::start(
        |c| {
            c.max_associations = 1;
            c.handshake_timeout_secs = 1;
            c.read_timeout_secs = 30;
        },
        |_| {},
    )
    .await;
    // A TCP connection that never sends an A-ASSOCIATE-RQ holds the only slot.
    let mut silent = TcpStream::connect(h.addr).await.unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), silent.read(&mut buf))
        .await
        .expect("server drops the silent connection after the handshake timeout")
        .unwrap_or(0);
    assert_eq!(n, 0, "expected EOF");
    // The slot is free again.
    let scu = connect(h.addr).await;
    scu.release().await.unwrap();
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_association_is_aborted() {
    let h = Harness::start(
        |c| {
            c.max_associations = 1;
            c.idle_timeout_secs = 1;
        },
        |_| {},
    )
    .await;
    let mut scu = connect(h.addr).await;
    let pc_id = pc_for(&scu, uids::VERIFICATION);
    // Commands arriving more often than the idle timeout keep it open.
    for id in 1..=4 {
        tokio::time::sleep(Duration::from_millis(600)).await;
        scu.send(&pdv(pc_id, PDataValueType::Command, true, echo_rq(id)))
            .await
            .unwrap();
        assert_eq!(read_status(&mut scu).await, 0x0000);
    }
    // Then silence: the server aborts the association.
    match tokio::time::timeout(Duration::from_secs(5), scu.receive()).await {
        Ok(Ok(Pdu::AbortRQ { .. })) => {}
        other => panic!("expected A-ABORT after idle timeout, got {other:?}"),
    }
    drop(scu);
    // The slot is free again.
    let scu = connect(h.addr).await;
    scu.release().await.unwrap();
    h.stop().await;
}

async fn wait_for_count(count: &std::sync::atomic::AtomicUsize, want: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while count.load(std::sync::atomic::Ordering::SeqCst) != want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "active associations never reached {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_associations_tracks_open_association() {
    let dir = tempfile::tempdir().unwrap();
    let scfg = StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    };
    let ingestor = Ingestor::open(scfg).await.unwrap();
    let cfg = ScpConfig {
        addr: "127.0.0.1:0".parse().unwrap(),
        ..ScpConfig::default()
    };
    let scp = StoreScp::bind(cfg, ingestor).await.unwrap();
    let addr = scp.local_addr();
    let active = scp.active_associations();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(scp.run(shutdown.clone()));

    assert_eq!(active.load(std::sync::atomic::Ordering::SeqCst), 0);
    let scu = connect(addr).await;
    wait_for_count(&active, 1).await;
    scu.release().await.unwrap();
    wait_for_count(&active, 0).await;

    shutdown.cancel();
    server.await.unwrap().unwrap();
}
