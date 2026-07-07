use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Output};

use miscreant::storage::Store;
use miscreant::{AppState, Config, app};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

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
pub struct TestServer {
    addr: SocketAddr,
    tempdir: TempDir,
    store: Store,
    handle: JoinHandle<()>,
}

impl TestServer {
    /// Bind `127.0.0.1:0` and serve the app in a background task.
    pub async fn spawn(config: Config) -> TestServer {
        let tempdir = TempDir::new().expect("create test tempdir");
        let state = AppState::new(config).await.expect("build app state");
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

/// Run the real `git` CLI in `dir` with a hermetic environment: protocol v2
/// forced, system config disabled, and `HOME` pointed at `dir` so host git
/// configuration cannot leak into tests.
#[allow(dead_code)]
pub fn git(dir: &Path, args: &[&str]) -> Output {
    Command::new("git")
        .args([
            "-c",
            "protocol.version=2",
            "-c",
            "advice.detachedHead=false",
        ])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .output()
        .expect("run git")
}
