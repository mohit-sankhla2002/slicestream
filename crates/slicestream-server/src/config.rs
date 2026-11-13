//! Server configuration: one TOML file with `[storage]`, `[http]`,
//! `[dimse]` and `[archives]`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use slicestream_archive::Limits;
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
    pub archives: ArchivesConfig,
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
    /// Serve `/api/metrics`, `/api/metrics/history` and `/api/console/series`
    /// and run the background sampler.
    pub metrics_enabled: bool,
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
            metrics_enabled: true,
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
            .field("metrics_enabled", &self.metrics_enabled)
            .finish()
    }
}

/// Archive / NIfTI uploads (`POST /api/archives`). Every field has a
/// default, so `[archives]` may be empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArchivesConfig {
    /// Mount `POST /api/archives` and `GET /api/archives/{job}`.
    pub enabled: bool,
    /// Nesting levels allowed below the uploaded file.
    pub max_depth: u32,
    /// Files seen across one whole upload.
    pub max_entries: u64,
    /// Decompressed bytes written to `tmp/` per upload.
    pub max_expanded_bytes: u64,
    /// Free space to keep on the storage volume: uploads are refused (507)
    /// or aborted below it.
    pub min_free_bytes: u64,
    /// Jobs processed at once; further uploads wait in a queue of
    /// `4 × max_concurrent_jobs` (queued and running).
    pub max_concurrent_jobs: usize,
    /// Finished jobs kept in memory for `GET /api/archives/{job}`.
    pub job_retention: usize,
}

impl Default for ArchivesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 4,
            max_entries: 200_000,
            max_expanded_bytes: 68_719_476_736,
            min_free_bytes: 2_147_483_648,
            max_concurrent_jobs: 2,
            job_retention: 50,
        }
    }
}

impl ArchivesConfig {
    /// The walker limits for one upload.
    pub fn limits(&self) -> Limits {
        Limits {
            max_depth: self.max_depth,
            max_entries: self.max_entries,
            max_expanded_bytes: self.max_expanded_bytes,
            min_free_bytes: self.min_free_bytes,
        }
    }
}
