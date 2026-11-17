//! Server configuration: one TOML file with `[storage]`, `[http]`,
//! `[dimse]`, `[archives]`, `[dicomweb]` and `[htj2k]`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
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
    pub dicomweb: DicomwebConfig,
    pub htj2k: Htj2kConfig,
}

impl Config {
    /// Read `path` as TOML, or return the defaults when `path` is `None`.
    pub fn load(path: Option<&Path>) -> anyhow::Result<Config> {
        let Some(path) = path else {
            return Ok(Config::default());
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("checking config {}", path.display()))?;
        Ok(config)
    }

    /// Check values that parse but make no sense.
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(url) = &self.http.public_url {
            let rest = url
                .strip_prefix("http://")
                .or_else(|| url.strip_prefix("https://"));
            if rest.is_none_or(|r| r.is_empty() || r.starts_with('/')) {
                bail!(
                    "[http] public_url must start with http:// or https:// and name a host: {url:?}"
                );
            }
        }
        if self.dicomweb.max_results == 0 {
            bail!("[dicomweb] max_results must be at least 1");
        }
        if self.htj2k.preview_size == 0 {
            bail!("[htj2k] preview_size must be at least 1");
        }
        Ok(())
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
    /// Base of Retrieve URLs and BulkDataURIs (for example
    /// `https://pacs.example.org` behind a reverse proxy). Unset:
    /// `http://<Host header>`.
    pub public_url: Option<String>,
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
            public_url: None,
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
            .field("public_url", &self.public_url)
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
    /// Finished jobs kept in memory for `GET /api/archives/{job}` (at
    /// least 1).
    pub job_retention: usize,
    /// Seconds an upload body may go without delivering data before it is
    /// refused (408) and its queue place released (at least 1).
    pub body_idle_timeout_secs: u64,
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
            body_idle_timeout_secs: 60,
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

/// DICOMweb read routes (QIDO-RS and WADO-RS). Every field has a default, so
/// `[dicomweb]` may be empty. STOW-RS is not affected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DicomwebConfig {
    /// Mount the `/dicom-web` GET routes.
    pub enabled: bool,
    /// QIDO-RS `limit`: its default and its maximum (at least 1).
    pub max_results: u32,
}

impl Default for DicomwebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_results: 1000,
        }
    }
}

/// When HTJ2K copies of stored instances are made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Htj2kMode {
    /// A background worker copies every eligible instance, newest first
    /// (instances a request asked for go first).
    Background,
    /// A request that wants HTJ2K frames without a copy encodes them on the
    /// spot; the worker then stores the copy of that instance. Instances no
    /// request asked for get no copy.
    OnDemand,
    /// No copies are made.
    Off,
}

/// Progressive HTJ2K copies. Every field has a default, so `[htj2k]` may be
/// empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Htj2kConfig {
    /// When copies are made.
    pub mode: Htj2kMode,
    /// Instances encoded at once by the copy worker, and frames encoded at
    /// once on demand across all requests (a separate limit of the same
    /// size); 0 is half the cores (at least 1).
    pub workers: usize,
    /// Longest side, in pixels, of the first preview a progressive stream
    /// sends (at least 1).
    pub preview_size: u32,
    /// Slices whose preview is sent before any final stage (other than the
    /// final of the slice in view) on the progressive stream.
    pub preview_ahead: usize,
    /// Answer `transfer-syntax=*`, `*/*` and a missing `Accept` with HTJ2K
    /// copies where one exists.
    pub prefer_for_wildcard: bool,
}

impl Default for Htj2kConfig {
    fn default() -> Self {
        Self {
            mode: Htj2kMode::Background,
            workers: 0,
            preview_size: 256,
            preview_ahead: 32,
            prefer_for_wildcard: false,
        }
    }
}

impl Htj2kConfig {
    /// Instances the copy worker encodes at once, and the limit of
    /// on-demand encodes.
    pub fn worker_count(&self) -> usize {
        if self.workers > 0 {
            return self.workers;
        }
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        (cores / 2).max(1)
    }
}

#[cfg(test)]
mod htj2k_tests {
    use super::*;

    #[test]
    fn htj2k_defaults() {
        let c: Config = toml::from_str("[htj2k]\n").unwrap();
        assert_eq!(c.htj2k.mode, Htj2kMode::Background);
        assert_eq!(c.htj2k.workers, 0);
        assert_eq!(c.htj2k.preview_size, 256);
        assert_eq!(c.htj2k.preview_ahead, 32);
        assert!(!c.htj2k.prefer_for_wildcard);
        assert_eq!(c.htj2k, Config::default().htj2k);
    }

    #[test]
    fn htj2k_mode_values() {
        let c: Config = toml::from_str("[htj2k]\nmode = \"on-demand\"").unwrap();
        assert_eq!(c.htj2k.mode, Htj2kMode::OnDemand);
        let c: Config = toml::from_str("[htj2k]\nmode = \"off\"").unwrap();
        assert_eq!(c.htj2k.mode, Htj2kMode::Off);
        let e = toml::from_str::<Config>("[htj2k]\nmode = \"sometimes\"").unwrap_err();
        assert!(e.to_string().contains("mode"), "{e}");
    }

    #[test]
    fn worker_count_zero_means_half_cores() {
        assert!(Htj2kConfig::default().worker_count() >= 1);
        let c = Htj2kConfig {
            workers: 3,
            ..Default::default()
        };
        assert_eq!(c.worker_count(), 3);
    }

    #[test]
    fn validate_rejects_zero_preview_size() {
        let mut c = Config::default();
        c.htj2k.preview_size = 0;
        assert!(c.validate().is_err());
    }
}
