mod common;

use common::{TestServer, git, test_config};
use miscreant::Config;
use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE};

/// Build a data pkt-line: a 4-hex length prefix over `data` (length includes
/// the prefix), followed by `data` verbatim. Mirrors the wire format the
/// server produces, so golden assertions pin exact bytes.
fn pkt(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data);
    out
}

const FLUSH: &[u8] = b"0000";

fn agent_capability() -> String {
    format!("agent=miscreant/{}", env!("CARGO_PKG_VERSION"))
}

/// The exact expected bytes of the protocol-v2 upload-pack advertisement.
fn expected_upload_advert() -> Vec<u8> {
    let mut expected = Vec::new();
    expected.extend(pkt(b"# service=git-upload-pack\n"));
    expected.extend_from_slice(FLUSH);
    expected.extend(pkt(b"version 2\n"));
    expected.extend(pkt(format!("{}\n", agent_capability()).as_bytes()));
    expected.extend(pkt(b"ls-refs=unborn\n"));
    expected.extend(pkt(b"fetch=filter\n"));
    expected.extend(pkt(b"object-format=sha1\n"));
    expected.extend_from_slice(FLUSH);
    expected
}

/// The exact expected bytes of the v0 receive-pack advertisement for an empty
/// repository (the synthetic `capabilities^{}` line).
fn expected_empty_receive_advert() -> Vec<u8> {
    let caps = format!(
        "report-status delete-refs side-band-64k ofs-delta {}",
        agent_capability()
    );
    let zeros = "0".repeat(40);
    let cap_line = format!("{zeros} capabilities^{{}}\0{caps}\n");
    let mut expected = Vec::new();
    expected.extend(pkt(b"# service=git-receive-pack\n"));
    expected.extend_from_slice(FLUSH);
    expected.extend(pkt(cap_line.as_bytes()));
    expected.extend_from_slice(FLUSH);
    expected
}

#[tokio::test]
async fn should_advertise_upload_pack_capabilities_for_v2_request() {
    // given: an existing repository (upload-pack never auto-creates).
    let server = TestServer::spawn(test_config()).await;
    server
        .store()
        .create_repo("acme/widgets")
        .await
        .expect("create");

    // when
    let response = reqwest::Client::new()
        .get(format!(
            "{}/acme/widgets/info/refs?service=git-upload-pack",
            server.base_url()
        ))
        .header("Git-Protocol", "version=2")
        .send()
        .await
        .expect("send request");

    // then
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/x-git-upload-pack-advertisement"
    );
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
    let body = response.bytes().await.expect("body");
    assert_eq!(body.as_ref(), expected_upload_advert().as_slice());
}

#[tokio::test]
async fn should_advertise_receive_pack_capabilities_for_fresh_repo() {
    // given
    let server = TestServer::spawn(test_config()).await;

    // when: a receive-pack advertisement for an unknown repo auto-creates it.
    let response = reqwest::get(format!(
        "{}/new/repo/info/refs?service=git-receive-pack",
        server.base_url()
    ))
    .await
    .expect("send request");

    // then
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/x-git-receive-pack-advertisement"
    );
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
    let body = response.bytes().await.expect("body");
    assert_eq!(body.as_ref(), expected_empty_receive_advert().as_slice());

    // the repository now exists.
    assert!(
        server
            .store()
            .lookup_repo("new/repo")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn should_reject_upload_pack_advertisement_without_protocol_v2() {
    // given
    let server = TestServer::spawn(test_config()).await;

    // when: no Git-Protocol header.
    let response = reqwest::get(format!(
        "{}/any/repo/info/refs?service=git-upload-pack",
        server.base_url()
    ))
    .await
    .expect("send request");

    // then: 400 with an ERR pkt-line body.
    assert_eq!(response.status(), 400);
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
    let body = response.bytes().await.expect("body");
    let message = b"git protocol version 2 required";
    let mut expected = format!("{:04x}", 4 + 4 + message.len()).into_bytes();
    expected.extend_from_slice(b"ERR ");
    expected.extend_from_slice(message);
    assert_eq!(body.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn should_return_404_for_upload_pack_advertisement_of_unknown_repo() {
    // given: auto-create is on, but it never applies to upload-pack.
    let server = TestServer::spawn(test_config()).await;

    // when
    let response = reqwest::Client::new()
        .get(format!(
            "{}/never/created/info/refs?service=git-upload-pack",
            server.base_url()
        ))
        .header("Git-Protocol", "version=2")
        .send()
        .await
        .expect("send request");

    // then
    assert_eq!(response.status(), 404);
    // upload-pack must not have created the repo.
    assert!(
        server
            .store()
            .lookup_repo("never/created")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn should_return_404_for_receive_pack_advertisement_when_auto_create_disabled() {
    // given
    let config = Config {
        auto_create_repos: false,
        ..test_config()
    };
    let server = TestServer::spawn(config).await;

    // when
    let response = reqwest::get(format!(
        "{}/unknown/info/refs?service=git-receive-pack",
        server.base_url()
    ))
    .await
    .expect("send request");

    // then
    assert_eq!(response.status(), 404);
    assert!(
        server
            .store()
            .lookup_repo("unknown")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn should_route_git_suffix_and_nested_names_to_the_same_repo() {
    // given
    let server = TestServer::spawn(test_config()).await;

    // when: the plain name auto-creates, and the `.git` form resolves to it.
    let plain = reqwest::get(format!(
        "{}/org/repo/info/refs?service=git-receive-pack",
        server.base_url()
    ))
    .await
    .expect("plain request");
    let suffixed = reqwest::get(format!(
        "{}/org/repo.git/info/refs?service=git-receive-pack",
        server.base_url()
    ))
    .await
    .expect("suffixed request");

    // then: both succeed and address a single repository.
    assert_eq!(plain.status(), 200);
    assert_eq!(suffixed.status(), 200);
    let plain_id = server
        .store()
        .lookup_repo("org/repo")
        .await
        .unwrap()
        .unwrap()
        .id;
    // The `.git` form did not allocate a second repository.
    assert!(
        server
            .store()
            .lookup_repo("org/repo.git")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(plain_id, miscreant::storage::keys::RepoId(1));
}

#[tokio::test]
async fn should_reject_upload_pack_rpc_without_protocol_v2() {
    // given
    let server = TestServer::spawn(test_config()).await;

    // when: the upload-pack RPC is served only for protocol v2, so a POST
    // without the Git-Protocol header is refused before it is even parsed.
    let response = reqwest::Client::new()
        .post(format!("{}/some/repo/git-upload-pack", server.base_url()))
        .body("")
        .send()
        .await
        .expect("send request");

    // then
    assert_eq!(response.status(), 400);
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
}

// The in-process server and the blocking `git` subprocess must run
// concurrently: `git()` blocks its thread on the child process while that
// child drives HTTP against the server, so a multi-threaded runtime is
// required (a single-threaded one would deadlock).
#[tokio::test(flavor = "multi_thread")]
async fn should_accept_push_dry_run_against_receive_advertisement() {
    // given: an empty local repository with one commit.
    let server = TestServer::spawn(test_config()).await;
    let repo_dir = server.tempdir().join("local");
    std::fs::create_dir_all(&repo_dir).expect("create local repo dir");
    let init = git(&repo_dir, &["init", "-q"]);
    assert!(init.status.success());
    let commit = git(
        &repo_dir,
        &[
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=test",
            "commit",
            "--allow-empty",
            "-q",
            "-m",
            "init",
        ],
    );
    assert!(commit.status.success());

    // when: a dry-run push only needs the receive-pack advertisement.
    let url = format!("{}/pushed.git", server.base_url());
    let push = git(
        &repo_dir,
        &["push", "--dry-run", &url, "HEAD:refs/heads/main"],
    );

    // then
    assert!(
        push.status.success(),
        "push --dry-run failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );
    // the advertisement auto-created the target repository.
    assert!(
        server
            .store()
            .lookup_repo("pushed")
            .await
            .unwrap()
            .is_some()
    );
}

// Multi-threaded for the same reason as the push smoke test above.
#[tokio::test(flavor = "multi_thread")]
async fn should_serve_an_upload_advertisement_a_real_client_accepts() {
    // given: an existing (empty) repository.
    let server = TestServer::spawn(test_config()).await;
    server
        .store()
        .create_repo("empty/repo")
        .await
        .expect("create");

    // when: `git ls-remote` performs the real v2 handshake — it parses the
    // advertisement, then issues the `ls-refs` command over POST.
    let url = format!("{}/empty/repo.git", server.base_url());
    let out = git(server.tempdir(), &["ls-remote", &url]);

    // then: the handshake and command succeed. An empty repository has only an
    // unborn HEAD (a symref to a branch that does not exist yet), which ls-refs
    // omits, so the client lists no refs and still exits 0.
    assert!(
        out.status.success(),
        "ls-remote failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.is_empty(),
        "expected no refs from an empty repository, got: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}
