use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use gix_hash::ObjectId;
use gix_object::Kind;

use metrics_exporter_prometheus::PrometheusHandle;
use miscreant::storage::Store;
use miscreant::{AppState, Config, app};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub mod invariant;
#[allow(unused_imports)]
pub use invariant::*;

pub mod oracle;
#[allow(unused_imports)]
pub use oracle::*;

pub mod wire;
#[allow(unused_imports)]
pub use wire::*;

/// A `Config` backed by an isolated in-memory store, suitable for tests that
/// must not touch the filesystem or collide with other tests.
#[allow(dead_code)]
pub fn test_config() -> Config {
    Config {
        storage_url: "memory://".to_owned(),
        ..Config::default()
    }
}

/// An in-process server instance bound to an ephemeral port, plus a scratch
/// directory for the duration of the test.
#[allow(dead_code)]
pub struct TestServer {
    addr: SocketAddr,
    tempdir: TempDir,
    store: Store,
    handle: JoinHandle<()>,
}

#[allow(dead_code)]
impl TestServer {
    /// Bind `127.0.0.1:0` and serve the app in a background task.
    pub async fn spawn(config: Config) -> TestServer {
        let state = AppState::new(config).await.expect("build app state");
        Self::from_state(state).await
    }

    /// Like [`TestServer::spawn`], but builds application state around
    /// `metrics` instead of a private, uninstalled handle — for tests that
    /// scrape `/metrics` and need it wired to the process's global recorder.
    #[allow(dead_code)]
    pub async fn spawn_with_metrics(config: Config, metrics: PrometheusHandle) -> TestServer {
        let state = AppState::with_metrics(config, metrics)
            .await
            .expect("build app state");
        Self::from_state(state).await
    }

    async fn from_state(state: AppState) -> TestServer {
        let tempdir = TempDir::new().expect("create test tempdir");
        let store = state.store.clone();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("read listener addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app(state)).await.expect("serve app");
        });
        TestServer {
            addr,
            tempdir,
            store,
            handle,
        }
    }

    /// Base URL of the running server, without a trailing slash.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The store shared with the running app, for asserting on persisted state.
    #[allow(dead_code)]
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Scratch directory owned by this server instance.
    #[allow(dead_code)]
    pub fn tempdir(&self) -> &Path {
        self.tempdir.path()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The git wire protocol version a command negotiates, mapping to the
/// `protocol.version=N` config value git is invoked with.
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub enum Protocol {
    V0,
    V2,
}

impl Protocol {
    /// The `protocol.version=N` config value for this protocol.
    fn version(self) -> &'static str {
        match self {
            Protocol::V0 => "0",
            Protocol::V2 => "2",
        }
    }
}

/// Build a real `git` CLI invocation in `dir` with a hermetic environment:
/// `protocol` forced, system config disabled, and `HOME` pointed at `dir`
/// so host git configuration cannot leak into tests.
fn git_command(dir: &Path, protocol: Protocol, args: &[&str]) -> Command {
    let mut command = Command::new("git");
    let version = format!("protocol.version={}", protocol.version());
    command
        .args(["-c", version.as_str(), "-c", "advice.detachedHead=false"])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir);
    command
}

/// Run the real `git` CLI in `dir` under protocol v2 (see [`git_command`] for
/// the environment).
#[allow(dead_code)]
pub fn git(dir: &Path, args: &[&str]) -> Output {
    git_proto(dir, Protocol::V2, args)
}

/// Like [`git`], but under a chosen `protocol`.
#[allow(dead_code)]
pub fn git_proto(dir: &Path, protocol: Protocol, args: &[&str]) -> Output {
    git_command(dir, protocol, args).output().expect("run git")
}

/// Run `git` in `dir` under protocol v2, asserting success and returning
/// stdout.
#[allow(dead_code)]
pub fn git_ok(dir: &Path, args: &[&str]) -> Vec<u8> {
    git_ok_proto(dir, Protocol::V2, args)
}

/// Like [`git_ok`], but under a chosen `protocol`.
#[allow(dead_code)]
pub fn git_ok_proto(dir: &Path, protocol: Protocol, args: &[&str]) -> Vec<u8> {
    let output = git_proto(dir, protocol, args);
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

/// Like [`git_ok`], but feeding `input` to stdin (e.g. `pack-objects --revs`
/// or `cat-file --batch`).
#[allow(dead_code)]
pub fn git_ok_with_input(dir: &Path, args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut child = git_command(dir, Protocol::V2, args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn git");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(input)
        .expect("write git stdin");
    let output = child.wait_with_output().expect("wait for git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

/// Clone `url` under `protocol` into `<server tempdir>/<name>`, asserting
/// success, and return the clone's path.
#[allow(dead_code)]
pub fn clone_proto(server: &TestServer, protocol: Protocol, url: &str, name: &str) -> PathBuf {
    let clone_dir = server.tempdir().join(name);
    let clone = git_proto(
        server.tempdir(),
        protocol,
        &["clone", url, clone_dir.to_str().expect("utf-8 clone path")],
    );
    assert!(
        clone.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    clone_dir
}

/// Create an empty git repository at `dir` (made on demand) with `main` as
/// the initial branch.
#[allow(dead_code)]
pub fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).expect("create repo dir");
    git_ok(dir, &["init", "-q", "-b", "main"]);
}

/// Write `contents` to `path` (relative to the repo), stage exactly that
/// file, and commit it. Returns the new commit's hex id.
#[allow(dead_code)]
pub fn commit_file(dir: &Path, path: &str, contents: &[u8], message: &str) -> String {
    let file = dir.join(path);
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent).expect("create file parent dir");
    }
    std::fs::write(&file, contents).expect("write fixture file");
    git_ok(dir, &["add", path]);
    git_ok(
        dir,
        &[
            "-c",
            "user.name=miscreant",
            "-c",
            "user.email=miscreant@example.com",
            "commit",
            "-q",
            "-m",
            message,
        ],
    );
    String::from_utf8(git_ok(dir, &["rev-parse", "HEAD"]))
        .expect("utf-8 commit id")
        .trim()
        .to_owned()
}

/// Build a pack with `git pack-objects --stdout --revs`, feeding one rev
/// argument per stdin line (prefix a rev with `^` to exclude its closure).
/// `extra` is appended to the argument list (e.g. `["--thin"]`).
#[allow(dead_code)]
pub fn pack_revs(dir: &Path, extra: &[&str], revs: &[&str]) -> Vec<u8> {
    let mut args = vec!["pack-objects", "-q", "--stdout", "--revs"];
    args.extend_from_slice(extra);
    let mut input = revs.join("\n");
    input.push('\n');
    git_ok_with_input(dir, &args, input.as_bytes())
}

/// Build a pack of exactly the listed objects with `git pack-objects
/// --stdout` (one oid per stdin line). Unlike [`pack_revs`], no reachability
/// closure is added: the pack holds precisely `oids`, which lets a test stage
/// an object whose referents are deliberately absent.
#[allow(dead_code)]
pub fn pack_objects(dir: &Path, oids: &[&str]) -> Vec<u8> {
    let mut input = oids.join("\n");
    input.push('\n');
    git_ok_with_input(dir, &["pack-objects", "-q", "--stdout"], input.as_bytes())
}

/// Resolve a revision to its full hex object id with `git rev-parse`.
#[allow(dead_code)]
pub fn rev_parse(dir: &Path, rev: &str) -> String {
    String::from_utf8(git_ok(dir, &["rev-parse", rev]))
        .expect("utf-8 rev-parse output")
        .trim()
        .to_owned()
}

/// List every object reachable per `git rev-list --objects <revs>` as
/// `(oid, kind, body)`, with bodies read via `git cat-file --batch`.
#[allow(dead_code)]
pub fn rev_objects(dir: &Path, revs: &[&str]) -> Vec<(ObjectId, Kind, Vec<u8>)> {
    let mut args = vec!["rev-list", "--objects"];
    args.extend_from_slice(revs);
    let listing = String::from_utf8(git_ok(dir, &args)).expect("utf-8 rev-list output");
    let mut batch_input = String::new();
    for line in listing.lines() {
        let oid = line.split_whitespace().next().expect("oid per line");
        batch_input.push_str(oid);
        batch_input.push('\n');
    }
    let batch = git_ok_with_input(dir, &["cat-file", "--batch"], batch_input.as_bytes());
    parse_cat_file_batch(&batch)
}

/// Parse `git cat-file --batch` output: repeated `<oid> <type> <size>\n`
/// headers, each followed by `<size>` body bytes and a newline.
fn parse_cat_file_batch(mut bytes: &[u8]) -> Vec<(ObjectId, Kind, Vec<u8>)> {
    let mut objects = Vec::new();
    while !bytes.is_empty() {
        let header_end = bytes
            .iter()
            .position(|&b| b == b'\n')
            .expect("batch header line");
        let header = std::str::from_utf8(&bytes[..header_end]).expect("utf-8 batch header");
        let mut fields = header.split(' ');
        let oid = ObjectId::from_hex(fields.next().expect("batch oid").as_bytes())
            .expect("valid batch oid");
        let kind =
            Kind::from_bytes(fields.next().expect("batch type").as_bytes()).expect("known kind");
        let size: usize = fields
            .next()
            .expect("batch size")
            .parse()
            .expect("numeric batch size");
        let body_start = header_end + 1;
        let body = bytes[body_start..body_start + size].to_vec();
        objects.push((oid, kind, body));
        // Skip the body and the trailing newline that `--batch` appends.
        bytes = &bytes[body_start + size + 1..];
    }
    objects
}
