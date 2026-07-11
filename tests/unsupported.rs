//! Client actions miscreant does not implement must fail cleanly: a non-zero
//! git exit or an in-band `ERR` pkt-line, never a hang, a panic, or a
//! half-applied result.

mod common;

use std::path::PathBuf;

use common::{
    DELIM, FLUSH, Pkt, TestServer, commit_file, fetch_body, git, init_repo, parse_pkts, pkt,
    post_upload_pack, rev_parse, test_config,
};

/// Initialize a local repository under the server's tempdir and push its
/// `main` to the named server repo. Returns the local repo path and URL.
fn push_fixture(server: &TestServer, repo: &str) -> (PathBuf, String) {
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    let url = format!("{}/{repo}.git", server.base_url());
    common::git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    (local, url)
}

/// Frame a raw protocol-v2 command request naming an arbitrary `object-format`
/// capability value (`fetch_body` in the shared harness hardcodes `sha1`,
/// which is exactly the value these tests need to vary).
fn command_body_with_format(command: &str, object_format: &str, args: &[&str]) -> Vec<u8> {
    let mut body = pkt(format!("command={command}\n").as_bytes());
    body.extend(pkt(format!("object-format={object_format}\n").as_bytes()));
    body.extend_from_slice(DELIM);
    for arg in args {
        body.extend(pkt(format!("{arg}\n").as_bytes()));
    }
    body.extend_from_slice(FLUSH);
    body
}

// The `git` subprocess blocks its thread on HTTP the in-process server must
// answer concurrently, so the CLI-driven tests use a multi-threaded runtime.

#[tokio::test(flavor = "multi_thread")]
async fn should_fail_a_shallow_clone_cleanly_without_a_usable_result() {
    // given: a small pushed history
    let server = TestServer::spawn(test_config()).await;
    let (_local, url) = push_fixture(&server, "proj");

    // when: a depth-limited clone, which asks for `deepen` — a feature
    // miscreant's advertisement never offers
    let clone_dir = server.tempdir().join("shallow-clone");
    let clone = git(
        server.tempdir(),
        &[
            "clone",
            "--depth=1",
            &url,
            clone_dir.to_str().expect("utf-8 clone path"),
        ],
    );

    // then: the clone terminates (it does not hang) and fails outright,
    // leaving no working `HEAD` behind for the client to mistake as usable
    assert!(
        !clone.status.success(),
        "expected a shallow clone to fail cleanly; stdout={} stderr={}",
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr)
    );
    assert!(
        !clone_dir.join("HEAD").exists(),
        "a shallow clone left a usable ref behind"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_a_tree_depth_filter_clone_cleanly() {
    // given: a small pushed history
    let server = TestServer::spawn(test_config()).await;
    let (_local, url) = push_fixture(&server, "proj");

    // when: a partial clone naming a filter kind miscreant does not implement
    let clone_dir = server.tempdir().join("tree-filter-clone");
    let clone = git(
        server.tempdir(),
        &[
            "clone",
            "--filter=tree:0",
            &url,
            clone_dir.to_str().expect("utf-8 clone path"),
        ],
    );

    // then: the clone fails cleanly and the client surfaces the server's
    // named rejection rather than silently degrading to a full clone
    assert!(
        !clone.status.success(),
        "expected the tree:0 filter to be rejected; stdout={} stderr={}",
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr)
    );
    assert!(
        String::from_utf8_lossy(&clone.stderr).contains("tree:0"),
        "expected the rejected filter spec in stderr: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    assert!(
        !clone_dir.join("HEAD").exists(),
        "a rejected filter clone left a usable ref behind"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_a_sparse_filter_clone_cleanly() {
    // given: a small pushed history, so a real blob oid is available to name
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let blob_oid = rev_parse(&local, "HEAD:a.txt");

    // when: a partial clone naming a sparse filter, which miscreant also does
    // not implement
    let clone_dir = server.tempdir().join("sparse-filter-clone");
    let clone = git(
        server.tempdir(),
        &[
            "clone",
            &format!("--filter=sparse:oid={blob_oid}"),
            &url,
            clone_dir.to_str().expect("utf-8 clone path"),
        ],
    );

    // then: the clone fails cleanly, again with no usable result behind
    assert!(
        !clone.status.success(),
        "expected the sparse:oid filter to be rejected; stdout={} stderr={}",
        String::from_utf8_lossy(&clone.stdout),
        String::from_utf8_lossy(&clone.stderr)
    );
    assert!(
        !clone_dir.join("HEAD").exists(),
        "a rejected filter clone left a usable ref behind"
    );
}

#[tokio::test]
async fn should_reject_a_fetch_naming_an_unsupported_object_format() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");
    let bogus = "f".repeat(64);

    // when: a raw v2 fetch naming sha256 instead of the sha1 every miscreant
    // repository actually uses — the server is width-aware and must not
    // half-accept a request against the wrong hash space
    let body = command_body_with_format("fetch", "sha256", &[&format!("want {bogus}"), "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: an in-band ERR naming the unsupported format, not a 200 that
    // quietly falls back to sha1 semantics
    assert_eq!(response.status(), 200);
    let text =
        String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8 body");
    assert!(
        text.contains("ERR unsupported object format: sha256"),
        "body: {text}"
    );
}

#[tokio::test]
async fn should_not_advertise_ref_in_want() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: the protocol-v2 upload-pack capability advertisement
    let response = reqwest::Client::new()
        .get(format!(
            "{}/proj/info/refs?service=git-upload-pack",
            server.base_url()
        ))
        .header("Git-Protocol", "version=2")
        .send()
        .await
        .expect("send request");

    // then: `ref-in-want` appears neither as its own capability nor inside
    // the `fetch=` feature list
    assert_eq!(response.status(), 200);
    let body =
        String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8 body");
    assert!(
        !body.contains("ref-in-want"),
        "ref-in-want must not be advertised: {body}"
    );
}

#[tokio::test]
async fn should_reject_a_want_ref_fetch_argument_cleanly() {
    // given: `ref-in-want` is unadvertised (see above), so a client sending it
    // anyway must be rejected rather than served
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: a raw v2 fetch using `want-ref` in place of an oid `want`
    let body = fetch_body(&["want-ref refs/heads/main", "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: an in-band ERR, not a served (and inevitably wrong) response
    assert_eq!(response.status(), 200);
    let text =
        String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8 body");
    assert!(
        text.contains("ERR") && text.contains("want-ref"),
        "body: {text}"
    );
}

#[tokio::test]
async fn should_return_err_for_an_unimplemented_v2_command() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: a well-formed v2 command request naming a command miscreant does
    // not implement
    let body = command_body_with_format("bogus", "sha1", &[]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: a single ERR pkt-line, not a hang or a malformed-request failure
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("body");
    let pkts = parse_pkts(&bytes);
    assert_eq!(
        pkts,
        vec![Pkt::Data(b"ERR unknown command: bogus".to_vec())]
    );
}
