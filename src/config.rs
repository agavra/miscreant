use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};

use clap::Args;
use url::Url;

/// Server configuration, populated from CLI flags and `MISCREANT_*`
/// environment variables. Flattened into every subcommand of the top-level
/// CLI (see `main.rs`), so both `serve` and `rebuild-graph` share the same
/// storage flags/env; some fields (e.g. `bind_addr`) are meaningful only to
/// `serve`.
#[derive(Args, Debug, Clone)]
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

/// Normalize a storage URL into a form `object_store::parse_url` accepts.
///
/// `object_store` only parses the `file` scheme when it has no host and an
/// absolute path, so a relative `file` URL (e.g. the default
/// `file://./miscreant-data`, whose host parses as `.`) or a bare relative
/// path is resolved against the current directory and re-emitted as an
/// absolute `file:///…` URL. Non-`file` URLs (`memory://`, `s3://…`) pass
/// through unchanged.
pub fn normalize_storage_url(raw: &str) -> io::Result<String> {
    let path_part = if let Some(rest) = raw.strip_prefix("file://") {
        rest
    } else if let Some(rest) = raw.strip_prefix("file:") {
        rest
    } else if raw.contains("://") || raw.starts_with("memory:") {
        return Ok(raw.to_owned());
    } else {
        // No scheme at all: treat the whole string as a filesystem path.
        raw
    };

    let path = Path::new(path_part);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let cleaned = clean_path(&absolute);

    Url::from_file_path(&cleaned)
        .map(|url| url.to_string())
        .map_err(|()| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot build a file url from {}", cleaned.display()),
            )
        })
}

/// Collapse `.` and `..` components so a joined relative path yields a tidy
/// absolute path (`/cwd/./data` → `/cwd/data`).
fn clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_pass_through_non_file_storage_urls() {
        // given/when/then
        assert_eq!(normalize_storage_url("memory://").unwrap(), "memory://");
        assert_eq!(
            normalize_storage_url("s3://bucket/prefix").unwrap(),
            "s3://bucket/prefix"
        );
    }

    #[test]
    fn should_normalize_relative_file_url_to_absolute() {
        // given
        let cwd = std::env::current_dir().unwrap();
        let expected = Url::from_file_path(cwd.join("miscreant-data"))
            .unwrap()
            .to_string();

        // when/then: the default host-`.` form and a bare relative path both
        // resolve to the same absolute `file:///…` URL.
        assert_eq!(
            normalize_storage_url("file://./miscreant-data").unwrap(),
            expected
        );
        assert_eq!(normalize_storage_url("miscreant-data").unwrap(), expected);
    }

    #[test]
    fn should_preserve_absolute_file_url() {
        // given
        let expected = Url::from_file_path("/srv/miscreant").unwrap().to_string();

        // when/then
        assert_eq!(
            normalize_storage_url("file:///srv/miscreant").unwrap(),
            expected
        );
    }

    #[test]
    fn should_normalize_the_default_storage_url() {
        // given/when: the shipped default must be openable.
        let normalized = normalize_storage_url(&Config::default().storage_url).unwrap();

        // then
        assert!(normalized.starts_with("file:///"));
        assert!(normalized.ends_with("/miscreant-data"));
    }
}
