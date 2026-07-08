use std::net::TcpListener as StdTcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Bind an ephemeral port and immediately release it, for a `bind_addr` we
/// can put in a config file before the server under test ever starts.
fn free_addr() -> String {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    format!(
        "127.0.0.1:{}",
        listener.local_addr().expect("local addr").port()
    )
}

/// Poll `GET http://<bind_addr>/healthz` until it answers or `deadline`
/// passes, for the window between spawning the server process and it
/// finishing storage setup and binding its listener.
async fn wait_for_healthz(bind_addr: &str) -> reqwest::Response {
    let url = format!("http://{bind_addr}/healthz");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match reqwest::get(&url).await {
            Ok(response) => return response,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("server at {url} never became ready: {err}"),
        }
    }
}

#[tokio::test]
async fn should_serve_healthz_on_a_bind_addr_configured_purely_by_file() {
    // given: a TOML file setting a non-default bind_addr and memory storage,
    // with no CLI flags or MISCREANT_* env vars supplying either
    let dir = tempfile::tempdir().expect("tempdir");
    let bind_addr = free_addr();
    let config_path = dir.path().join("miscreant.toml");
    std::fs::write(
        &config_path,
        format!("bind_addr = \"{bind_addr}\"\nstorage_url = \"memory://\"\n"),
    )
    .expect("write config file");

    // when: the compiled binary boots from `--config` alone
    let mut child = Command::new(env!("CARGO_BIN_EXE_miscreant"))
        .args(["--config", config_path.to_str().expect("utf-8 config path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn miscreant binary");
    let response = wait_for_healthz(&bind_addr).await;

    // then: the server is listening on the file-configured address, proving
    // the file actually drove startup rather than the built-in default
    child.kill().expect("kill server process");
    let _ = child.wait();
    assert_eq!(response.status(), 200);
}
