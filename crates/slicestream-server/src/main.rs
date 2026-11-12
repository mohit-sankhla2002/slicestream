//! Reference server for slicestream.
//!
//! ```sh
//! slicestream-server import data/phantom          # ingest existing files
//! slicestream-server --config slicestream.toml   # same as `serve --config ...`
//! ```
//!
//! `serve` is the default subcommand. Without `--config`, every setting has
//! its default (see `slicestream.example.toml`). Open http://127.0.0.1:8080
//! for the demo viewer.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use slicestream_server::config::Config;
use slicestream_server::{import_dir, start};
use slicestream_store::{IngestError, Ingestor};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "DICOM server that streams slices in scroll order")]
struct Cli {
    /// TOML configuration file (`import` uses only `[storage]`).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, PartialEq, Subcommand)]
enum Command {
    /// Run the HTTP/WebSocket server and the DIMSE storage SCP (default).
    Serve,
    /// Ingest existing DICOM files into storage.
    Import {
        /// Directory holding Part-10 files.
        dir: PathBuf,
        /// Descend into sub-directories.
        #[arg(long)]
        recursive: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let config = Config::load(cli.config.as_deref())?;
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => serve(config).await,
        Command::Import { dir, recursive } => import(config, dir, recursive).await,
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

async fn import(config: Config, dir: PathBuf, recursive: bool) -> anyhow::Result<ExitCode> {
    if !dir.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    // `import` owns the storage root like `serve` does, so it cannot run
    // beside a server on the same root.
    let root = config.storage.root.clone();
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
    let r = import_dir(&ingestor, &dir, recursive).await;
    println!(
        "stored {} / duplicate {} / failed {} (skipped {} non-DICOM)",
        r.stored, r.duplicate, r.failed, r.skipped
    );
    Ok(if r.failed > 0 {
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
            dir: PathBuf::from("d"),
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
