//! `[dimse]` configuration.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// DIMSE storage SCP settings. Every field has a default, so `[dimse]` may be empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScpConfig {
    /// Start the SCP at all.
    pub enabled: bool,
    /// Our AE title.
    pub ae_title: String,
    /// Listen address.
    pub addr: SocketAddr,
    /// Reject associations whose Called AE Title is not `ae_title`.
    pub require_called_ae_title: bool,
    /// Accepted Calling AE Titles; empty accepts any.
    pub allowed_peers: Vec<String>,
    /// Concurrent associations; further connections are dropped.
    pub max_associations: usize,
    /// Largest PDU we accept, in bytes.
    pub max_pdu_length: u32,
    /// Per-PDU read timeout.
    pub read_timeout_secs: u64,
    /// Per-PDU write timeout.
    pub write_timeout_secs: u64,
    /// Time allowed from TCP accept to an established association
    /// (A-ASSOCIATE-RQ received and answered). The connection is dropped,
    /// freeing its slot, when it runs out.
    pub handshake_timeout_secs: u64,
    /// An association that receives no DIMSE command for this long while no
    /// operation is in progress is aborted.
    pub idle_timeout_secs: u64,
}

impl Default for ScpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ae_title: "SLICESTREAM".to_owned(),
            addr: SocketAddr::from(([0, 0, 0, 0], 11112)),
            require_called_ae_title: true,
            allowed_peers: Vec::new(),
            max_associations: 64,
            max_pdu_length: 116_794,
            read_timeout_secs: 60,
            write_timeout_secs: 60,
            handshake_timeout_secs: 30,
            idle_timeout_secs: 300,
        }
    }
}
