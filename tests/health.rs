mod common;

use common::TestServer;
use miscreant::Config;

#[tokio::test]
async fn should_return_ok_from_healthz() {
    // given
    let server = TestServer::spawn(Config::default()).await;

    // when
    let response = reqwest::get(format!("{}/healthz", server.base_url()))
        .await
        .expect("GET /healthz");

    // then
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.expect("read body"), "ok");
}
