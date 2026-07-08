use std::path::Path;

use anyhow::Context;
use clap::{Parser, Subcommand};
use metrics_exporter_prometheus::PrometheusBuilder;
use miscreant::git::walk::Walker;
use miscreant::{AppState, Config, app};
use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// `miscreant` with no subcommand serves the git HTTP(S) protocol (the
/// README quickstart's bare `cargo run`); `rebuild-graph` instead
/// recomputes one repository's commit-graph segment offline.
#[derive(Parser, Debug)]
#[command(
    name = "miscreant",
    version,
    about = "a git server backed by object storage"
)]
struct Cli {
    #[command(flatten)]
    config: Config,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the smart-HTTP git server. The default when no subcommand is given.
    Serve,
    /// Recompute a repository's commit-graph segment from its objects,
    /// overwriting any existing records, without starting the HTTP server.
    RebuildGraph {
        /// Name of the repository to rebuild.
        #[arg(long)]
        repo: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Cli { config, command } = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    match command.unwrap_or(Command::Serve) {
        Command::Serve => serve(config).await,
        Command::RebuildGraph { repo } => rebuild_graph(config, &repo).await,
    }
}

/// Bind the configured address and serve the git HTTP(S) protocol until
/// shutdown.
async fn serve(config: Config) -> anyhow::Result<()> {
    tracing::info!(
        storage_url = %redact_storage_url(&config.storage_url),
        bind_addr = %config.bind_addr,
        inline_threshold = config.inline_threshold,
        auto_create_repos = config.auto_create_repos,
        staging_root = %config.staging_root.display(),
        "starting"
    );

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

    // Install the process-wide recorder before anything records through the
    // `metrics` facade, and describe every metric only after installing it:
    // `describe_*` targets whichever recorder is currently global, so an
    // earlier call would register descriptions on the default no-op recorder
    // and lose them.
    let builder = miscreant::telemetry::configure_byte_buckets(PrometheusBuilder::new())
        .context("failed to configure metrics buckets")?;
    let metrics_handle = builder
        .install_recorder()
        .context("failed to install metrics recorder")?;
    miscreant::telemetry::describe();

    let state = AppState::with_metrics(config, metrics_handle)
        .await
        .context("failed to open storage")?;
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    Ok(())
}

/// Open the store backing `config.storage_url` offline (no HTTP server) and
/// rebuild `repo`'s commit-graph segment, printing the number of records
/// written.
async fn rebuild_graph(config: Config, repo: &str) -> anyhow::Result<()> {
    let state = AppState::new(config)
        .await
        .context("failed to open storage")?;
    let Some(meta) = state
        .store
        .lookup_repo(repo)
        .await
        .context("failed to look up repository")?
    else {
        anyhow::bail!("repository {repo:?} does not exist");
    };
    let walker = Walker::new(state.store, state.objectdb, meta.id);
    let count = walker
        .rebuild_commit_graph()
        .await
        .context("failed to rebuild commit graph")?;
    println!("rebuilt {count} commit-graph record(s) for repository {repo:?}");
    Ok(())
}

/// Strip any credentials from a storage URL before it is logged. A storage
/// URL may one day carry a secret in its userinfo (e.g. `s3://key:secret@…`);
/// the username and password are removed so the startup event never records
/// them. A value that does not parse as a URL is logged verbatim (it has no
/// userinfo to leak).
fn redact_storage_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw) else {
        return raw.to_owned();
    };
    if url.username().is_empty() && url.password().is_none() {
        return url.to_string();
    }
    // If either setter refuses (a URL that cannot hold userinfo), fall back to
    // the scheme alone rather than risk emitting the credentials.
    if url.set_username("").is_err() || url.set_password(None).is_err() {
        return format!("{}://<redacted>", url.scheme());
    }
    url.to_string()
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
