use std::path::Path;
use std::time::Instant;

use anyhow::Context;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use metrics_exporter_prometheus::PrometheusBuilder;
use miscreant::config::{self, FileConfig};
use miscreant::git::walk::Walker;
use miscreant::{AppState, Config, app};
use tokio::net::TcpListener;
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
    // Parsed by hand (rather than `Cli::parse()`) so the `ArgMatches` survive
    // past parsing: merging a config file needs `value_source` per flattened
    // `Config` field, which the `Cli`/`Config` values alone cannot tell us.
    let matches = Cli::command().get_matches();
    let Cli { config, command } = Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());

    let file_config = config
        .config_path
        .as_ref()
        .map(|path| {
            FileConfig::load(path)
                .with_context(|| format!("failed to load config file {}", path.display()))
        })
        .transpose()?;
    let config = match &file_config {
        // `Config` is flattened directly into `Cli` (see its doc comment), so
        // its fields' ids live on `matches` itself regardless of whether a
        // subcommand was also given — verified empirically for both
        // `miscreant --flag serve` and `miscreant --flag rebuild-graph
        // --repo <name>` (a flag placed after the subcommand name is
        // rejected as unrecognized, since flattened args are not `global`).
        Some(file) => config::merge_file_config(&matches, config, file),
        None => config,
    };

    // Reject an out-of-range compression level from any source (CLI, env, or
    // file) before opening storage, so a bad value fails loudly at startup
    // rather than on the first object write.
    if config.object_compression_level > 9 {
        anyhow::bail!(
            "object_compression_level must be between 0 and 9, got {}",
            config.object_compression_level
        );
    }

    // `log_filter` is not a `Config` field: it only ever feeds the
    // subscriber's default directive, chosen here before anything about
    // `Config` is read. `EnvFilter::from_env_lossy` already gives `RUST_LOG`
    // priority over whatever default directive we pass it.
    let log_filter = file_config
        .as_ref()
        .and_then(|file| file.log_filter.clone())
        .unwrap_or_else(|| config::DEFAULT_LOG_FILTER.to_owned());
    let default_directive = log_filter
        .parse()
        .with_context(|| format!("invalid log_filter directive {log_filter:?}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(default_directive)
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
    // Warming at startup only pays off against a persistent (hybrid) cache; the
    // default in-memory cache is empty every boot, so reject the combination
    // rather than silently doing nothing useful.
    if config.warm_on_start && config.cache_dir.is_none() {
        anyhow::bail!(
            "--warm-on-start requires --cache-dir: there is no persistent block \
             cache to warm otherwise"
        );
    }

    tracing::info!(
        storage_url = %redact_storage_url(&config.storage_url),
        bind_addr = %config.bind_addr,
        inline_threshold = config.inline_threshold,
        auto_create_repos = config.auto_create_repos,
        staging_root = %config.staging_root.display(),
        cache_dir = ?config.cache_dir,
        warm_on_start = config.warm_on_start,
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

    // Install the process-wide recorder before opening the store: SlateDB's
    // metrics bridge and any cache warming below both publish through the
    // `metrics` facade, and `describe_*` targets whichever recorder is
    // currently global, so an earlier describe would land on the default no-op
    // recorder and be lost.
    let builder = miscreant::telemetry::configure_byte_buckets(PrometheusBuilder::new())
        .context("failed to configure metrics buckets")?;
    let metrics_handle = builder
        .install_recorder()
        .context("failed to install metrics recorder")?;
    miscreant::telemetry::describe();

    let state = AppState::with_metrics(config, metrics_handle)
        .await
        .context("failed to open storage")?;

    // Warm the cache before binding so the first request is served fully hot.
    if state.config.warm_on_start {
        let start = Instant::now();
        let ssts = state
            .store
            .warm_cache()
            .await
            .context("failed to warm block cache")?;
        tracing::info!(
            ssts,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "warmed block cache"
        );
    }

    let listener = TcpListener::bind(state.config.bind_addr)
        .await
        .with_context(|| format!("failed to bind {}", state.config.bind_addr))?;
    tracing::info!(addr = %state.config.bind_addr, "listening");

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
