use std::path::Path;

use anyhow::Context;
use clap::Parser;
use miscreant::{AppState, Config, app};
use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    reset_staging_root(&config.staging_root)
        .await
        .with_context(|| {
            format!(
                "failed to reset staging root {}",
                config.staging_root.display()
            )
        })?;

    let listener = TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("failed to bind {}", config.bind_addr))?;
    tracing::info!(addr = %config.bind_addr, "listening");

    let state = AppState::new(config)
        .await
        .context("failed to open storage")?;
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    Ok(())
}

/// Delete and recreate the staging root. Pack staging is request-scoped, so
/// anything present at startup is leftover garbage from a previous process
/// (see `docs/0001-init.md` §Receive API: the staging root is wiped on
/// startup).
async fn reset_staging_root(root: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(root).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    tokio::fs::create_dir_all(root).await
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to install ctrl-c handler");
    }
    tracing::info!("shutting down");
}
