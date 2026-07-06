mod common;

use common::TestServer;
use miscreant::Config;

#[tokio::test]
async fn healthz_returns_ok() {
    let server = TestServer::spawn(Config::default()).await;

    let response = reqwest::get(format!("{}/healthz", server.base_url()))
        .await
        .expect("GET /healthz");

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.expect("read body"), "ok");
}
