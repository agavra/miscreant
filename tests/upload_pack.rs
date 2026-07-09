mod common;

use std::collections::BTreeMap;

use common::{TestServer, commit_file, git, git_ok, init_repo, rev_parse, test_config};
use gix_hash::ObjectId;
use miscreant::storage::values::RefTarget;

/// A distinct SHA-1 object id built from a single hex nibble, for seeding refs
/// whose targets need not be real objects (ls-refs resolves refs without
/// reading the objects they point at, unless peeling is requested).
fn oid(nibble: u8) -> ObjectId {
    ObjectId::from_hex(&[nibble; 40]).expect("valid sha1 hex")
}

/// Build a data pkt-line: a 4-hex length prefix (covering itself) then the
/// payload verbatim.
fn pkt(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data);
    out
}

const DELIM: &[u8] = b"0001";
const FLUSH: &[u8] = b"0000";

/// Frame an `ls-refs` command request: the command, the `object-format`
/// capability, a delimiter, then one pkt-line per argument, then a flush.
fn ls_refs_body(args: &[&str]) -> Vec<u8> {
    let mut body = pkt(b"command=ls-refs\n");
    body.extend(pkt(b"object-format=sha1\n"));
    body.extend_from_slice(DELIM);
    for arg in args {
        body.extend(pkt(format!("{arg}\n").as_bytes()));
    }
    body.extend_from_slice(FLUSH);
    body
}

/// Frame an `object-format` command request: the command and the
/// `object-format` capability, terminated directly by a flush — the command
/// takes no arguments, so there is no delimiter or argument section.
fn object_format_body() -> Vec<u8> {
    let mut body = pkt(b"command=object-format\n");
    body.extend(pkt(b"object-format=sha1\n"));
    body.extend_from_slice(FLUSH);
    body
}

/// Frame an `object-info` command request: the command, the `object-format`
/// capability, a delimiter, then one pkt-line per argument, then a flush.
fn object_info_body(args: &[String]) -> Vec<u8> {
    let mut body = pkt(b"command=object-info\n");
    body.extend(pkt(b"object-format=sha1\n"));
    body.extend_from_slice(DELIM);
    for arg in args {
        body.extend(pkt(format!("{arg}\n").as_bytes()));
    }
    body.extend_from_slice(FLUSH);
    body
}

/// POST a protocol-v2 command request to a repo's upload-pack endpoint.
async fn post_upload_pack(base_url: &str, repo: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base_url}/{repo}/git-upload-pack"))
        .header("Git-Protocol", "version=2")
        .body(body)
        .send()
        .await
        .expect("send request")
}

/// Parse `git ls-remote` stdout (`<oid>\t<refname>` per line) into a name→oid
/// map.
fn ls_remote_map(stdout: &[u8]) -> BTreeMap<String, String> {
    String::from_utf8(stdout.to_vec())
        .expect("utf-8 ls-remote output")
        .lines()
        .filter_map(|line| {
            let (oid, name) = line.split_once('\t')?;
            Some((name.to_owned(), oid.to_owned()))
        })
        .collect()
}

/// Commit identity flags for the hermetic `git` helper.
const IDENTITY: [&str; 4] = [
    "-c",
    "user.name=miscreant",
    "-c",
    "user.email=miscreant@example.com",
];

// The `git` subprocess blocks its thread on HTTP the in-process server must
// answer concurrently, so the CLI-driven tests use a multi-threaded runtime.

#[tokio::test(flavor = "multi_thread")]
async fn should_list_heads_and_tags_and_resolve_head_via_ls_remote() {
    // given: a repo pushed with a branch and an annotated tag
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    let mut tag_args = IDENTITY.to_vec();
    tag_args.extend_from_slice(&["tag", "-a", "v1", "-m", "release", "HEAD"]);
    assert!(
        git(&local, &tag_args).status.success(),
        "tag creation failed"
    );
    let url = format!("{}/proj.git", server.base_url());
    let push = git(
        &local,
        &["push", &url, "main:refs/heads/main", "v1:refs/tags/v1"],
    );
    assert!(
        push.status.success(),
        "push failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );

    // when
    let out = git(server.tempdir(), &["ls-remote", &url]);

    // then: heads, tags, and a symref-resolved HEAD are all listed
    assert!(
        out.status.success(),
        "ls-remote failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let refs = ls_remote_map(&out.stdout);
    let main = rev_parse(&local, "refs/heads/main");
    let tag = rev_parse(&local, "refs/tags/v1");
    assert_eq!(refs.get("refs/heads/main"), Some(&main));
    assert_eq!(refs.get("refs/tags/v1"), Some(&tag));
    // HEAD is advertised and resolves through its symref to the branch tip.
    assert_eq!(refs.get("HEAD"), Some(&main));
}

#[tokio::test(flavor = "multi_thread")]
async fn should_show_peeled_annotated_tag_via_ls_remote_tags() {
    // given: a repo with an annotated tag pushed to it
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    let mut tag_args = IDENTITY.to_vec();
    tag_args.extend_from_slice(&["tag", "-a", "v1", "-m", "release", "HEAD"]);
    assert!(
        git(&local, &tag_args).status.success(),
        "tag creation failed"
    );
    let url = format!("{}/proj.git", server.base_url());
    assert!(
        git(&local, &["push", &url, "v1:refs/tags/v1"])
            .status
            .success()
    );

    // when
    let out = git(server.tempdir(), &["ls-remote", "--tags", &url]);

    // then: both the tag ref and its peeled commit line appear
    assert!(
        out.status.success(),
        "ls-remote --tags failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let refs = ls_remote_map(&out.stdout);
    let tag = rev_parse(&local, "refs/tags/v1");
    let peeled = rev_parse(&local, "refs/tags/v1^{}");
    assert_eq!(refs.get("refs/tags/v1"), Some(&tag));
    assert_eq!(refs.get("refs/tags/v1^{}"), Some(&peeled));
}

#[tokio::test]
async fn should_exclude_tags_when_ref_prefix_limits_to_heads() {
    // given: a repo with both a head and a tag ref
    let server = TestServer::spawn(test_config()).await;
    let repo = server.store().create_repo("proj").await.expect("create").id;
    server
        .store()
        .put_ref(repo, "refs/heads/main", &RefTarget::Direct(oid(b'a')))
        .await
        .expect("put head");
    server
        .store()
        .put_ref(repo, "refs/tags/v1", &RefTarget::Direct(oid(b'b')))
        .await
        .expect("put tag");

    // when: the client restricts the advertisement to the heads namespace
    let response = post_upload_pack(
        &server.base_url(),
        "proj",
        ls_refs_body(&["ref-prefix refs/heads/"]),
    )
    .await;

    // then: only the head is listed; the tag and HEAD are excluded
    assert_eq!(response.status(), 200);
    let body = String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8");
    assert!(body.contains("refs/heads/main"), "body: {body}");
    assert!(!body.contains("refs/tags/"), "tags leaked: {body}");
    assert!(!body.contains(" HEAD"), "HEAD leaked: {body}");
}

#[tokio::test]
async fn should_report_symref_target_for_head() {
    // given: a repo whose HEAD symrefs to a populated branch
    let server = TestServer::spawn(test_config()).await;
    let repo = server.store().create_repo("proj").await.expect("create").id;
    server
        .store()
        .put_ref(repo, "refs/heads/main", &RefTarget::Direct(oid(b'a')))
        .await
        .expect("put head");

    // when
    let response = post_upload_pack(&server.base_url(), "proj", ls_refs_body(&["symrefs"])).await;

    // then: HEAD carries its immediate symref target
    assert_eq!(response.status(), 200);
    let body = String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8");
    assert!(
        body.contains("HEAD symref-target:refs/heads/main"),
        "body: {body}"
    );
}

#[tokio::test]
async fn should_advertise_unborn_head_on_an_empty_repo_when_requested() {
    // given: a freshly auto-created, empty repository — HEAD symrefs to
    // refs/heads/main, which does not exist yet
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: the client asks for both unborn and symref-target reporting
    let response = post_upload_pack(
        &server.base_url(),
        "proj",
        ls_refs_body(&["unborn", "symrefs"]),
    )
    .await;

    // then: the body is exactly the unborn HEAD line plus the flush
    assert_eq!(response.status(), 200);
    let body = response.bytes().await.expect("body");
    let mut expected = pkt(b"unborn HEAD symref-target:refs/heads/main\n");
    expected.extend_from_slice(FLUSH);
    assert_eq!(body.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn should_omit_unborn_head_on_an_empty_repo_when_not_requested() {
    // given: a freshly auto-created, empty repository
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: symrefs is requested but unborn is not
    let response = post_upload_pack(&server.base_url(), "proj", ls_refs_body(&["symrefs"])).await;

    // then: HEAD dangles and is omitted, leaving just the flush
    assert_eq!(response.status(), 200);
    let body = response.bytes().await.expect("body");
    assert_eq!(body.as_ref(), FLUSH);
}

// Multi-threaded: the real `git clone` subprocess blocks its thread on HTTP
// the in-process server must answer concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn should_clone_an_empty_repo_onto_the_servers_default_branch() {
    // given: an empty repository and a client configured to default new
    // clones to `master` instead of the server's `main`
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");
    let url = format!("{}/proj.git", server.base_url());
    let clone_dir = server.tempdir().join("clone");

    // when
    let clone = git(
        server.tempdir(),
        &[
            "-c",
            "init.defaultBranch=master",
            "clone",
            &url,
            clone_dir.to_str().expect("utf-8 clone dir path"),
        ],
    );

    // then: the clone succeeds and its HEAD names the server's default
    // branch, not the client's configured default
    assert!(
        clone.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    let head = String::from_utf8(git_ok(&clone_dir, &["symbolic-ref", "HEAD"]))
        .expect("utf-8 symbolic-ref output");
    assert_eq!(head.trim(), "refs/heads/main");
}

#[tokio::test]
async fn should_reject_a_v2_command_body_sent_without_protocol_v2() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: a well-formed v2 ls-refs command body but no Git-Protocol header,
    // so it reaches the classic (v0) handler, which does not speak v2 command
    // framing
    let response = reqwest::Client::new()
        .post(format!("{}/proj/git-upload-pack", server.base_url()))
        .body(ls_refs_body(&[]))
        .send()
        .await
        .expect("send request");

    // then: 400 with a malformed-request ERR pkt-line
    assert_eq!(response.status(), 400);
    let body = response.bytes().await.expect("body");
    let message = b"malformed upload-pack request";
    let mut expected = format!("{:04x}", 4 + 4 + message.len()).into_bytes();
    expected.extend_from_slice(b"ERR ");
    expected.extend_from_slice(message);
    assert_eq!(body.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn should_return_the_repos_object_format() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when
    let response = post_upload_pack(&server.base_url(), "proj", object_format_body()).await;

    // then: the body is exactly the sha1 line plus the flush
    assert_eq!(response.status(), 200);
    let body = response.bytes().await.expect("body");
    let mut expected = pkt(b"sha1\n");
    expected.extend_from_slice(FLUSH);
    assert_eq!(body.as_ref(), expected.as_slice());
}

// Multi-threaded: the real `git push` subprocess blocks its thread on HTTP
// the in-process server must answer concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn should_report_sizes_for_inline_and_offloaded_blobs_via_object_info() {
    // given: a repo pushed with a small blob (stored inline) and a blob
    // larger than the inline threshold (offloaded to the blob store)
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    let small = b"small blob content".to_vec();
    let large = vec![b'x'; 70_000]; // exceeds the 65536-byte inline threshold
    commit_file(&local, "small.txt", &small, "add small");
    commit_file(&local, "large.bin", &large, "add large");
    let url = format!("{}/proj.git", server.base_url());
    let push = git(&local, &["push", &url, "main:refs/heads/main"]);
    assert!(
        push.status.success(),
        "push failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );
    let small_oid = rev_parse(&local, "HEAD:small.txt");
    let large_oid = rev_parse(&local, "HEAD:large.bin");

    // when
    let args = vec![
        "size".to_owned(),
        format!("oid {small_oid}"),
        format!("oid {large_oid}"),
    ];
    let response = post_upload_pack(&server.base_url(), "proj", object_info_body(&args)).await;

    // then: both sizes are reported, the offloaded blob's true content size
    // included even though its content never lived in SlateDB
    assert_eq!(response.status(), 200);
    let body = String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8");
    assert!(body.contains("size"), "body: {body}");
    assert!(
        body.contains(&format!("{small_oid} {}", small.len())),
        "body: {body}"
    );
    assert!(
        body.contains(&format!("{large_oid} {}", large.len())),
        "body: {body}"
    );
}

#[tokio::test]
async fn should_reject_object_info_for_an_unknown_oid() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: the requested oid has no object record in the repository
    let args = vec!["size".to_owned(), format!("oid {}", oid(b'a'))];
    let response = post_upload_pack(&server.base_url(), "proj", object_info_body(&args)).await;

    // then: an in-band ERR pkt-line, not a hard failure of the RPC itself
    assert_eq!(response.status(), 200);
    let body = String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8");
    assert!(
        body.contains("ERR ") && body.contains("unknown object"),
        "body: {body}"
    );
}

#[tokio::test]
async fn should_reject_an_unknown_object_info_argument() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when
    let args = vec!["deepen 1".to_owned()];
    let response = post_upload_pack(&server.base_url(), "proj", object_info_body(&args)).await;

    // then
    let body = String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8");
    assert!(
        body.contains("ERR ") && body.contains("unexpected object-info argument: deepen 1"),
        "body: {body}"
    );
}

#[tokio::test]
async fn should_return_404_for_ls_refs_on_unknown_repo() {
    // given: auto-create is on, but upload-pack never applies it
    let server = TestServer::spawn(test_config()).await;

    // when
    let response = post_upload_pack(&server.base_url(), "never/created", ls_refs_body(&[])).await;

    // then
    assert_eq!(response.status(), 404);
    assert!(
        server
            .store()
            .lookup_repo("never/created")
            .await
            .unwrap()
            .is_none()
    );
}
