use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};

use clap::Args;
use clap::parser::ValueSource;
use serde::Deserialize;
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

    /// Directory for the disk part cache (created if missing). Set it to cache
    /// object parts on local disk: one cache, shared by SlateDB's SST reads and
    /// offloaded-blob reads, under a single byte budget. Unset uses SlateDB's
    /// default in-memory block cache and installs no part cache.
    #[arg(long, env = "MISCREANT_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Capacity of the in-memory SlateDB block cache (parsed SST blocks), in
    /// bytes. Meaningful only when `cache_dir` is set; SST byte caching lives
    /// in the disk part cache.
    #[arg(
        long,
        env = "MISCREANT_CACHE_MEMORY_BYTES",
        default_value_t = 536870912
    )]
    pub cache_memory_bytes: u64,

    /// Byte budget for the whole disk part cache directory. Meaningful only
    /// when `cache_dir` is set.
    #[arg(
        long,
        env = "MISCREANT_CACHE_DISK_BYTES",
        default_value_t = 34359738368
    )]
    pub cache_disk_bytes: u64,

    /// Warm the block cache from the manifest at startup, before the listener
    /// binds, so the first request is served hot. Requires `cache_dir`.
    #[arg(
        long,
        env = "MISCREANT_WARM_ON_START",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    pub warm_on_start: bool,

    /// zlib level (0–9) at which object content is deflated when written, so
    /// serving a clone copies the stored stream out without recompressing.
    #[arg(long, env = "MISCREANT_OBJECT_COMPRESSION_LEVEL", default_value_t = 6)]
    pub object_compression_level: u32,

    /// TOML file supplying defaults for any of the flags/env vars above that
    /// were left unset (see `FileConfig`). A path that cannot be read or
    /// parsed is a startup error; there is no implicit discovery.
    #[arg(long = "config", env = "MISCREANT_CONFIG", value_name = "PATH")]
    pub config_path: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            storage_url: "file://./miscreant-data".to_owned(),
            inline_threshold: 65536,
            auto_create_repos: true,
            staging_root: default_staging_root(),
            cache_dir: None,
            cache_memory_bytes: 536870912,
            cache_disk_bytes: 34359738368,
            warm_on_start: false,
            object_compression_level: 6,
            config_path: None,
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

/// Default `log_filter` when neither `RUST_LOG` nor a config file set one.
pub const DEFAULT_LOG_FILTER: &str = "info";

/// TOML mirror of [`Config`], loaded from `--config`/`MISCREANT_CONFIG`.
/// Every field is optional so [`merge_file_config`] can tell "the file left
/// this unset" from any concrete value (including a falsy one like `false`
/// or `0`); unknown keys are rejected so a typo fails loudly at startup
/// rather than being silently ignored.
///
/// `log_filter` has no [`Config`] counterpart: it is a
/// `tracing_subscriber::EnvFilter` directive consumed in `main` to pick the
/// subscriber's default directive, before any handler ever reads `Config`.
/// `RUST_LOG`, when set, always wins over it (see `main.rs`).
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub bind_addr: Option<SocketAddr>,
    pub storage_url: Option<String>,
    pub inline_threshold: Option<usize>,
    pub auto_create_repos: Option<bool>,
    pub staging_root: Option<PathBuf>,
    pub cache_dir: Option<PathBuf>,
    pub cache_memory_bytes: Option<u64>,
    pub cache_disk_bytes: Option<u64>,
    pub warm_on_start: Option<bool>,
    pub object_compression_level: Option<u32>,
    pub log_filter: Option<String>,
}

/// A `--config`/`MISCREANT_CONFIG` path that could not be read or parsed.
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
}

impl FileConfig {
    /// Read and parse `path`. Neither a missing file nor a parse failure
    /// (including an unknown key) is ever silently ignored.
    pub fn load(path: &Path) -> Result<Self, ConfigFileError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigFileError::Read {
            path: path.to_owned(),
            source,
        })?;
        toml::from_str(&text).map_err(|source| ConfigFileError::Parse {
            path: path.to_owned(),
            source,
        })
    }
}

/// Merge `file` into `config`, one field at a time, but only where `matches`
/// reports the CLI/env value came from `Config`'s built-in default: an
/// explicit `--flag` or `MISCREANT_*` value must always win over the file,
/// even when it happens to equal the default (clap reports that case as
/// `ValueSource::CommandLine`/`EnvVariable`, never `DefaultValue`, so the
/// check below preserves it correctly). `matches` must be the `ArgMatches`
/// that `config` itself was built from — see `main.rs` for why that is the
/// top-level `Cli` matches even when a subcommand was also given.
pub fn merge_file_config(
    matches: &clap::ArgMatches,
    mut config: Config,
    file: &FileConfig,
) -> Config {
    let left_at_default = |id: &str| matches.value_source(id) == Some(ValueSource::DefaultValue);

    if left_at_default("bind_addr")
        && let Some(value) = file.bind_addr
    {
        config.bind_addr = value;
    }
    if left_at_default("storage_url")
        && let Some(value) = &file.storage_url
    {
        config.storage_url = value.clone();
    }
    if left_at_default("inline_threshold")
        && let Some(value) = file.inline_threshold
    {
        config.inline_threshold = value;
    }
    if left_at_default("auto_create_repos")
        && let Some(value) = file.auto_create_repos
    {
        config.auto_create_repos = value;
    }
    if left_at_default("staging_root")
        && let Some(value) = &file.staging_root
    {
        config.staging_root = value.clone();
    }
    // `cache_dir` is an `Option` with no built-in default, so clap reports its
    // source as absent (not `DefaultValue`) when neither `--cache-dir` nor
    // `MISCREANT_CACHE_DIR` was given; the file supplies it only in that case,
    // never overriding an explicit CLI/env value.
    if !matches!(
        matches.value_source("cache_dir"),
        Some(ValueSource::CommandLine | ValueSource::EnvVariable)
    ) && let Some(value) = &file.cache_dir
    {
        config.cache_dir = Some(value.clone());
    }
    if left_at_default("cache_memory_bytes")
        && let Some(value) = file.cache_memory_bytes
    {
        config.cache_memory_bytes = value;
    }
    if left_at_default("cache_disk_bytes")
        && let Some(value) = file.cache_disk_bytes
    {
        config.cache_disk_bytes = value;
    }
    if left_at_default("warm_on_start")
        && let Some(value) = file.warm_on_start
    {
        config.warm_on_start = value;
    }
    if left_at_default("object_compression_level")
        && let Some(value) = file.object_compression_level
    {
        config.object_compression_level = value;
    }
    config
}

#[cfg(test)]
mod tests {
    use clap::FromArgMatches;

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

    /// Parse `args` (program name included, e.g. `["test", "--bind-addr",
    /// "…"]`) the same way `main.rs` parses `Cli`: build a `Command` from
    /// `Config`'s own arg definitions, then keep the `ArgMatches` alongside
    /// the parsed struct so a test can inspect `value_source` exactly like
    /// [`merge_file_config`] does.
    fn parse_config(args: &[&str]) -> (Config, clap::ArgMatches) {
        let command = Config::augment_args(clap::Command::new("test"));
        let matches = command.try_get_matches_from(args).expect("parse args");
        let config = Config::from_arg_matches(&matches).expect("build config");
        (config, matches)
    }

    /// Serializes every test that parses `Config` or touches `MISCREANT_*`
    /// process environment variables. `Config`'s fields are declared with
    /// `env = "MISCREANT_*"`, so any test that calls [`parse_config`]
    /// observes whatever another thread currently has set — cargo runs
    /// tests in one process on multiple threads by default, so this lock
    /// (not a `serial-test` dependency) is what actually gives each test an
    /// isolated view of the environment.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Acquire [`ENV_LOCK`], recovering from a poisoned lock (an earlier
    /// test panicked while holding it) so one failure cannot cascade into
    /// spurious failures elsewhere.
    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Sets `MISCREANT_*` environment variables for the duration of a test
    /// and scrubs them on drop (including on an assertion panic mid-test).
    /// Only sound while the caller also holds [`ENV_LOCK`] for at least as
    /// long as this guard lives (drop order: declare the lock first, this
    /// guard second, so the scrub runs before the lock is released).
    struct EnvVarGuard {
        keys: Vec<&'static str>,
    }

    impl EnvVarGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            for (key, value) in vars {
                // SAFETY: guarded by ENV_LOCK (see the struct doc comment).
                unsafe { std::env::set_var(key, value) };
            }
            Self {
                keys: vars.iter().map(|(key, _)| *key).collect(),
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            for key in &self.keys {
                // SAFETY: see `EnvVarGuard::set`.
                unsafe { std::env::remove_var(key) };
            }
        }
    }

    #[test]
    fn should_apply_file_only_values_left_at_their_default() {
        // given: no CLI/env overrides, and a file setting every merged field
        let _env_lock = lock_env();
        let (config, matches) = parse_config(&["test"]);
        let file = FileConfig {
            bind_addr: Some("127.0.0.1:9000".parse().unwrap()),
            storage_url: Some("memory://".to_owned()),
            inline_threshold: Some(1024),
            auto_create_repos: Some(false),
            staging_root: Some(PathBuf::from("/tmp/from-file")),
            cache_dir: Some(PathBuf::from("/var/cache/from-file")),
            cache_memory_bytes: Some(1234),
            cache_disk_bytes: Some(5678),
            warm_on_start: Some(true),
            object_compression_level: Some(1),
            log_filter: None,
        };

        // when
        let merged = merge_file_config(&matches, config, &file);

        // then
        assert_eq!(merged.bind_addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(merged.storage_url, "memory://");
        assert_eq!(merged.inline_threshold, 1024);
        assert!(!merged.auto_create_repos);
        assert_eq!(merged.staging_root, PathBuf::from("/tmp/from-file"));
        assert_eq!(
            merged.cache_dir,
            Some(PathBuf::from("/var/cache/from-file"))
        );
        assert_eq!(merged.cache_memory_bytes, 1234);
        assert_eq!(merged.cache_disk_bytes, 5678);
        assert!(merged.warm_on_start);
        assert_eq!(merged.object_compression_level, 1);
    }

    #[test]
    fn should_prefer_an_explicit_cli_value_over_the_file_even_when_it_equals_the_default() {
        // given: bind_addr given explicitly on the CLI, equal to the built-in
        // default (so DefaultValue vs. CommandLine is the only distinguisher)
        let _env_lock = lock_env();
        let default_addr = default_bind_addr();
        let (config, matches) = parse_config(&["test", "--bind-addr", &default_addr.to_string()]);
        let file = FileConfig {
            bind_addr: Some("127.0.0.1:9000".parse().unwrap()),
            ..FileConfig::default()
        };

        // when
        let merged = merge_file_config(&matches, config, &file);

        // then: the explicit CLI value wins, not the file's
        assert_eq!(merged.bind_addr, default_addr);
    }

    #[test]
    fn should_prefer_environment_variables_over_the_file() {
        // given: every MISCREANT_* env var set to a non-default value, and a
        // file that disagrees with all of them. Every env-precedence
        // assertion lives in this one test function because process env is
        // global to the test binary; `EnvVarGuard` scrubs the vars on drop
        // before `_env_lock` releases the module-wide environment lock.
        let _env_lock = lock_env();
        let _env = EnvVarGuard::set(&[
            ("MISCREANT_BIND_ADDR", "127.0.0.1:6000"),
            ("MISCREANT_STORAGE_URL", "memory://"),
            ("MISCREANT_INLINE_THRESHOLD", "2048"),
            ("MISCREANT_AUTO_CREATE_REPOS", "false"),
            ("MISCREANT_STAGING_ROOT", "/tmp/from-env"),
            ("MISCREANT_CACHE_DIR", "/var/cache/from-env"),
            ("MISCREANT_CACHE_MEMORY_BYTES", "4096"),
            ("MISCREANT_CACHE_DISK_BYTES", "8192"),
            ("MISCREANT_WARM_ON_START", "true"),
            ("MISCREANT_OBJECT_COMPRESSION_LEVEL", "9"),
        ]);
        let (config, matches) = parse_config(&["test"]);
        let file = FileConfig {
            bind_addr: Some("127.0.0.1:9000".parse().unwrap()),
            storage_url: Some("file:///from-file".to_owned()),
            inline_threshold: Some(1),
            auto_create_repos: Some(true),
            staging_root: Some(PathBuf::from("/tmp/from-file")),
            cache_dir: Some(PathBuf::from("/var/cache/from-file")),
            cache_memory_bytes: Some(1),
            cache_disk_bytes: Some(2),
            warm_on_start: Some(false),
            object_compression_level: Some(1),
            log_filter: None,
        };

        // when
        let merged = merge_file_config(&matches, config, &file);

        // then: the environment values win over the file for every field
        assert_eq!(merged.bind_addr, "127.0.0.1:6000".parse().unwrap());
        assert_eq!(merged.storage_url, "memory://");
        assert_eq!(merged.inline_threshold, 2048);
        assert!(!merged.auto_create_repos);
        assert_eq!(merged.staging_root, PathBuf::from("/tmp/from-env"));
        assert_eq!(merged.cache_dir, Some(PathBuf::from("/var/cache/from-env")));
        assert_eq!(merged.cache_memory_bytes, 4096);
        assert_eq!(merged.cache_disk_bytes, 8192);
        assert!(merged.warm_on_start);
        assert_eq!(merged.object_compression_level, 9);
    }

    #[test]
    fn should_reject_an_unknown_key_in_the_config_file() {
        // given: a file with a field name miscreant does not recognize
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "bind_addr = \"127.0.0.1:9000\"\nbogus = true\n")
            .expect("write config file");

        // when
        let err = FileConfig::load(&path).expect_err("unknown key must be rejected");

        // then: the error names the offending key, not just "parse failed"
        assert!(matches!(err, ConfigFileError::Parse { .. }));
        assert!(err.to_string().contains("bogus"), "error: {err}");
    }

    #[test]
    fn should_error_when_the_config_file_is_missing() {
        // given: a path with no file on disk
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist.toml");

        // when
        let err = FileConfig::load(&path).expect_err("missing file must error");

        // then
        assert!(matches!(err, ConfigFileError::Read { .. }));
    }

    #[test]
    fn should_parse_the_example_toml_config_matching_built_in_defaults() {
        // given: the example file checked in at the repository root
        let example_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("miscreant.example.toml");

        // when
        let file = FileConfig::load(&example_path).expect("example file parses");

        // then: every documented default matches `Config::default()`, except
        // staging_root, whose parent (the system temp directory) is
        // environment-specific — only its final path component is pinned — and
        // cache_dir, which has no built-in default and so is shown only as a
        // commented-out line (left unset when the file is parsed).
        let defaults = Config::default();
        assert_eq!(file.bind_addr, Some(defaults.bind_addr));
        assert_eq!(file.storage_url, Some(defaults.storage_url));
        assert_eq!(file.inline_threshold, Some(defaults.inline_threshold));
        assert_eq!(file.auto_create_repos, Some(defaults.auto_create_repos));
        assert_eq!(
            file.staging_root.as_ref().and_then(|path| path.file_name()),
            defaults.staging_root.file_name(),
        );
        assert_eq!(file.cache_memory_bytes, Some(defaults.cache_memory_bytes));
        assert_eq!(file.cache_disk_bytes, Some(defaults.cache_disk_bytes));
        assert_eq!(file.warm_on_start, Some(defaults.warm_on_start));
        assert_eq!(
            file.object_compression_level,
            Some(defaults.object_compression_level)
        );
        assert_eq!(file.log_filter.as_deref(), Some(DEFAULT_LOG_FILTER));
    }
}
