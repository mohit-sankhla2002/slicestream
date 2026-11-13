//! Reference server for slicestream.
//!
//! ```sh
//! slicestream-server import data/phantom          # ingest a folder of files
//! slicestream-server import scans.zip              # or an archive, NIfTI or DICOM file
//! slicestream-server --config slicestream.toml   # same as `serve --config ...`
//! ```
//!
//! `serve` is the default subcommand. Without `--config`, every setting has
//! its default (see `slicestream.example.toml`). Open http://127.0.0.1:8080
//! for the demo viewer.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use slicestream_server::archive_ingest::{
    EntryOutcome, EntryResult, Progress, UploadSource, process_dir, process_upload,
};
use slicestream_server::config::Config;
use slicestream_server::start;
use slicestream_store::{IngestError, Ingestor};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "DICOM server that streams slices in scroll order")]
struct Cli {
    /// TOML configuration file (`import` uses `[storage]` and `[archives]`).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, PartialEq, Subcommand)]
enum Command {
    /// Run the HTTP/WebSocket server and the DIMSE storage SCP (default).
    Serve,
    /// Ingest DICOM, NIfTI and archives (zip, tar, tar.gz, 7z, rar) into
    /// storage. PATH is a folder or a single file; archives are opened,
    /// nested ones too, within the `[archives]` limits.
    Import {
        /// Folder or file to ingest.
        path: PathBuf,
        /// Descend into sub-directories (folders only).
        #[arg(long)]
        recursive: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    // Logs go to stderr; stdout carries command output.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let config = Config::load(cli.config.as_deref())?;
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => serve(config).await,
        Command::Import { path, recursive } => import(config, path, recursive).await,
    }
}

async fn serve(config: Config) -> anyhow::Result<ExitCode> {
    let shutdown = CancellationToken::new();
    let running = start(config, shutdown.clone()).await?;
    tracing::info!("listening on http://{}", running.http_addr);
    if let Some(addr) = running.dimse_addr {
        tracing::info!("DIMSE storage SCP on {addr}");
    }

    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown requested; finishing in-flight work");
        shutdown.cancel();
    });

    running.task.await.context("server task panicked")??;
    tracing::info!("stopped");
    Ok(ExitCode::SUCCESS)
}

/// Logs failed entries; stdout stays free for the summary.
struct WarnFailures;

impl Progress for WarnFailures {
    fn entry(&self, r: &EntryResult) {
        if r.outcome == EntryOutcome::Failed {
            tracing::warn!(
                path = %r.path,
                "failed: {}",
                r.reason.as_deref().unwrap_or("unknown error")
            );
        }
    }

    fn stats(&self, _entries_seen: u64, _bytes_expanded: u64) {}
}

async fn import(config: Config, path: PathBuf, recursive: bool) -> anyhow::Result<ExitCode> {
    let meta = std::fs::metadata(&path).with_context(|| format!("reading {}", path.display()))?;
    // `import` owns the storage root like `serve` does, so it cannot run
    // beside a server on the same root.
    let root = config.storage.root.clone();
    let limits = config.archives.limits();
    let ingestor = match Ingestor::open(config.storage).await {
        Ok(i) => i,
        Err(IngestError::Locked { .. }) => bail!(
            "a server is running on the storage root {}; stop it, or upload via \
             STOW-RS / POST /api/instances instead",
            root.display()
        ),
        Err(e) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("opening storage at {}", root.display()));
        }
    };
    let report = ingestor.recover().await.context("storage recovery")?;
    tracing::info!(
        tmp_removed = report.tmp_removed,
        orphans_removed = report.orphans_removed,
        "storage ready"
    );
    let name = path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let upload_id = ingestor
        .index()
        .create_upload("import", &name, None)
        .await
        .context("recording the upload")?;
    let progress: Arc<dyn Progress> = Arc::new(WarnFailures);
    let outcome = if meta.is_dir() {
        process_dir(ingestor, &path, recursive, upload_id, limits, progress).await
    } else {
        let source = UploadSource::Import { root: path.clone() };
        process_upload(ingestor, path, name, upload_id, source, limits, progress).await
    };

    for p in &outcome.patients {
        let images: u64 = p.studies.iter().map(|s| s.instances).sum();
        println!(
            "{} {}: {} studies, {} images",
            p.patient_id,
            p.patient_name.as_deref().unwrap_or(""),
            p.studies.len(),
            images
        );
    }
    let c = &outcome.counts;
    println!(
        "stored {} \u{b7} duplicate {} \u{b7} nifti {} (linked {}, unassigned {}) \u{b7} skipped {} \u{b7} failed {}",
        c.dicom_stored,
        c.dicom_duplicate,
        c.nifti_stored + c.nifti_duplicate,
        outcome.nifti.linked,
        outcome.nifti.unassigned,
        c.skipped,
        c.failed
    );
    if outcome.status == "aborted" {
        println!(
            "aborted: {}",
            outcome.reason.as_deref().unwrap_or("unknown")
        );
    }
    Ok(if c.failed > 0 || outcome.status == "aborted" {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

/// Resolve on Ctrl-C, or on SIGTERM on Unix.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("cannot listen for Ctrl-C: {e}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!("cannot listen for SIGTERM: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).unwrap()
    }

    #[test]
    fn config_without_subcommand_means_serve() {
        let cli = parse(&["slicestream-server", "--config", "x"]);
        assert_eq!(cli.config, Some(PathBuf::from("x")));
        assert_eq!(cli.command.unwrap_or(Command::Serve), Command::Serve);
    }

    #[test]
    fn config_accepted_in_either_position() {
        let expected = Command::Import {
            path: PathBuf::from("d"),
            recursive: false,
        };
        for args in [
            ["slicestream-server", "import", "d", "--config", "x"],
            ["slicestream-server", "--config", "x", "import", "d"],
        ] {
            let cli = parse(&args);
            assert_eq!(cli.config, Some(PathBuf::from("x")));
            assert_eq!(cli.command, Some(expected.clone()));
        }
        let cli = parse(&["slicestream-server", "serve", "--config", "y"]);
        assert_eq!(cli.config, Some(PathBuf::from("y")));
        assert_eq!(cli.command, Some(Command::Serve));
    }
}
