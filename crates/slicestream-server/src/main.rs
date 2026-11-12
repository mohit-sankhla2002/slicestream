//! Reference server for slicestream.
//!
//! ```sh
//! slicestream-server import data/phantom          # ingest existing files
//! slicestream-server serve --config slicestream.toml
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
use slicestream_store::Ingestor;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "DICOM server that streams slices in scroll order")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the HTTP/WebSocket server and the DIMSE storage SCP (default).
    Serve {
        /// TOML configuration file.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Ingest existing DICOM files into storage.
    Import {
        /// Directory holding Part-10 files.
        dir: PathBuf,
        /// TOML configuration file (only `[storage]` is used).
        #[arg(long)]
        config: Option<PathBuf>,
        /// Descend into sub-directories.
        #[arg(long)]
        recursive: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    tracing_subscriber::fmt::init();
    match Cli::parse()
        .command
        .unwrap_or(Command::Serve { config: None })
    {
        Command::Serve { config } => serve(Config::load(config.as_deref())?).await,
        Command::Import {
            dir,
            config,
            recursive,
        } => import(Config::load(config.as_deref())?, dir, recursive).await,
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
    // No `recover()` here: a server may be running on the same storage root,
    // and recovery would remove its in-flight spool files.
    let root = config.storage.root.clone();
    let ingestor = Ingestor::open(config.storage)
        .await
        .with_context(|| format!("opening storage at {}", root.display()))?;
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
