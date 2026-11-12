//! Server configuration: one TOML file with `[storage]`, `[http]` and `[dimse]`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use slicestream_dimse::ScpConfig;
use slicestream_store::StorageConfig;

/// The whole configuration file. Every section and key has a default, and
/// unknown keys are rejected so typos do not silently fall back to defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub storage: StorageConfig,
    pub http: HttpConfig,
    pub dimse: ScpConfig,
}

impl Config {
    /// Read `path` as TOML, or return the defaults when `path` is `None`.
    pub fn load(path: Option<&Path>) -> anyhow::Result<Config> {
        let Some(path) = path else {
            return Ok(Config::default());
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
    }
}

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
    /// Origins (e.g. `https://viewer.example.org`) allowed to read the API
    /// cross-origin. Empty: no CORS headers anywhere. Ingest routes never
    /// get CORS.
    pub cors_allowed_origins: Vec<String>,
    /// Accepted `Host` header values (`host` or `host:port`); others get
    /// 421. Empty: `localhost`, `127.0.0.1`, `[::1]` and the host of `addr`,
    /// each with or without the port of `addr`.
    pub allowed_hosts: Vec<String>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 8080)),
            max_request_bytes: 17_179_869_184,
            stow_token: None,
            web_dir: PathBuf::from("web"),
            cors_allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
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
            .field("cors_allowed_origins", &self.cors_allowed_origins)
            .field("allowed_hosts", &self.allowed_hosts)
            .finish()
    }
}
