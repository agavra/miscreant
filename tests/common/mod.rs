use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Output};

use miscreant::{AppState, Config, app};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// An in-process server instance bound to an ephemeral port, plus a scratch
/// directory for the duration of the test.
pub struct TestServer {
    addr: SocketAddr,
    tempdir: TempDir,
    handle: JoinHandle<()>,
}

impl TestServer {
    /// Bind `127.0.0.1:0` and serve the app in a background task.
    pub async fn spawn(config: Config) -> TestServer {
        let tempdir = TempDir::new().expect("create test tempdir");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("read listener addr");
        let state = AppState::new(config);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app(state)).await.expect("serve app");
        });
        TestServer {
            addr,
            tempdir,
            handle,
        }
    }

    /// Base URL of the running server, without a trailing slash.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
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
