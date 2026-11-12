//! `[http]` configuration.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// HTTP server settings. Every field has a default, so `[http]` may be empty.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// Listen address.
    pub addr: SocketAddr,
    /// Largest accepted STOW-RS request body, in bytes.
    pub max_request_bytes: u64,
    /// If set, ingest endpoints require `Authorization: Bearer <token>`.
    pub stow_token: Option<String>,
    /// Static files for the demo viewer.
    pub web_dir: PathBuf,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 8080)),
            max_request_bytes: 17_179_869_184,
            stow_token: None,
            web_dir: PathBuf::from("web"),
        }
    }
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("addr", &self.addr)
            .field("max_request_bytes", &self.max_request_bytes)
            .field(
                "stow_token",
                &self.stow_token.as_ref().map(|_| "<redacted>"),
            )
            .field("web_dir", &self.web_dir)
            .finish()
    }
}
