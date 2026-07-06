use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use clap::Parser;

/// Server configuration, populated from CLI flags and `MISCREANT_*`
/// environment variables.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "miscreant",
    version,
    about = "a git server backed by object storage"
)]
pub struct Config {
    /// Address the HTTP server listens on.
    #[arg(long, env = "MISCREANT_BIND_ADDR", default_value_t = default_bind_addr())]
    pub bind_addr: SocketAddr,

    /// Object storage URL backing all repository data
    /// (e.g. `file://./miscreant-data`, `memory://`, `s3://bucket/prefix`).
    #[arg(
        long,
        env = "MISCREANT_STORAGE_URL",
        default_value = "file://./miscreant-data"
    )]
    pub storage_url: String,

    /// Blob contents at or below this many bytes are stored inline;
    /// larger blobs are offloaded to object storage.
    #[arg(long, env = "MISCREANT_INLINE_THRESHOLD", default_value_t = 65536)]
    pub inline_threshold: usize,

    /// Create unknown repositories on first push.
    #[arg(
        long,
        env = "MISCREANT_AUTO_CREATE_REPOS",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    pub auto_create_repos: bool,

    /// Local directory for per-request pack staging.
    #[arg(
        long,
        env = "MISCREANT_STAGING_ROOT",
        default_value_os_t = default_staging_root()
    )]
    pub staging_root: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            storage_url: "file://./miscreant-data".to_owned(),
            inline_threshold: 65536,
            auto_create_repos: true,
            staging_root: default_staging_root(),
        }
    }
}

fn default_bind_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8470)
}

fn default_staging_root() -> PathBuf {
    std::env::temp_dir().join("miscreant-staging")
}
