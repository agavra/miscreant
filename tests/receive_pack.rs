mod common;

use std::path::Path;

use common::{
    TestServer, commit_file, git, init_repo, pack_revs, rev_objects, rev_parse, test_config,
};
use gix_hash::ObjectId;
use miscreant::Config;
use miscreant::storage::keys::RepoId;
use miscreant::storage::values::RefTarget;

// Every test drives the blocking `git` subprocess against the in-process
// server, so a multi-threaded runtime is required: the subprocess blocks its
// thread while making HTTP calls the server must answer concurrently.

/// The id of an already-created repository.
async fn repo_id(server: &TestServer, name: &str) -> RepoId {
    server
        .store()
        .lookup_repo(name)
        .await
        .expect("lookup repo")
        .expect("repo exists")
        .id
}

/// The direct oid a ref points at, or `None` if it does not exist.
async fn ref_oid(server: &TestServer, repo: RepoId, name: &str) -> Option<ObjectId> {
    match server.store().get_ref(repo, name).await.expect("get ref") {
        Some(RefTarget::Direct(oid)) => Some(oid),
        _ => None,
    }
}

fn oid_of(hex: &str) -> ObjectId {
    ObjectId::from_hex(hex.as_bytes()).expect("valid hex")
}

/// Build a data pkt-line: a 4-hex length prefix (covering itself) then the
/// payload verbatim.
fn pkt(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data);
    out
}

/// Push `refspec` from `repo_dir` to `url`, returning the process output.
fn push(repo_dir: &Path, url: &str, extra: &[&str], refspec: &str) -> std::process::Output {
    let mut args = vec!["push"];
    args.extend_from_slice(extra);
    args.push(url);
    args.push(refspec);
    git(repo_dir, &args)
}

#[tokio::test(flavor = "multi_thread")]
async fn should_promote_objects_refs_and_graph_when_pushing_two_commits() {
    // given: a fresh local repo with two commits on main
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    let head = commit_file(&local, "b/nested.txt", b"beta\n", "add b");
    let url = format!("{}/proj.git", server.base_url());

    // when
    let out = push(&local, &url, &[], "main:refs/heads/main");

    // then: the git CLI reports success
    assert!(
        out.status.success(),
        "push failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // then: the ref, every object in HEAD's closure, and each commit's graph
    // record are all present in committed storage
    let repo = repo_id(&server, "proj").await;
    assert_eq!(
        ref_oid(&server, repo, "refs/heads/main").await,
        Some(oid_of(&head))
    );
    for (oid, _, _) in rev_objects(&local, &[&head]) {
        assert!(
            server
                .store()
                .get_object(repo, &oid)
                .await
                .expect("get object")
                .is_some(),
            "object {oid} missing from store"
        );
    }
    assert!(
        server
            .store()
            .get_commit_graph(repo, &oid_of(&head))
            .await
            .expect("get graph")
            .is_some(),
        "HEAD has no commit-graph record"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_land_an_incremental_second_push() {
    // given: a repo whose first commit is already pushed
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    let c1 = commit_file(&local, "f.txt", b"one\n", "one");
    let url = format!("{}/proj.git", server.base_url());
    assert!(
        push(&local, &url, &[], "main:refs/heads/main")
            .status
            .success()
    );

    // when: a second commit is pushed incrementally (git sends only the new
    // objects, delta-ing against the base already on the server)
    let c2 = commit_file(&local, "f.txt", b"two\n", "two");
    let out = push(&local, &url, &[], "main:refs/heads/main");

    // then: the push succeeds and the ref advances to the new tip
    assert!(
        out.status.success(),
        "incremental push failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let repo = repo_id(&server, "proj").await;
    assert_eq!(
        ref_oid(&server, repo, "refs/heads/main").await,
        Some(oid_of(&c2))
    );
    // then: the new commit got a generation-2 record atop the pushed base
    assert_eq!(
        server
            .store()
            .get_commit_graph(repo, &oid_of(&c2))
            .await
            .expect("get graph")
            .expect("c2 graph record")
            .generation,
        2
    );
    // sanity: the base is still there too
    assert!(
        server
            .store()
            .get_object(repo, &oid_of(&c1))
            .await
            .expect("get c1")
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_offload_a_blob_pushed_over_the_inline_threshold() {
    // given: a file:// backed server (so the offloaded blob is inspectable on
    // disk) and a local repo with a file above the 64KiB inline threshold
    let storage = tempfile::TempDir::new().expect("storage dir");
    let config = Config {
        storage_url: format!("file://{}", storage.path().display()),
        ..Config::default()
    };
    let server = TestServer::spawn(config).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    let big = vec![b'x'; 70_000];
    let head = commit_file(&local, "big.bin", &big, "add big");
    let url = format!("{}/proj.git", server.base_url());

    // when
    let out = push(&local, &url, &[], "main:refs/heads/main");

    // then: the push succeeds
    assert!(
        out.status.success(),
        "push failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // then: the blob is offloaded — its record is a size-only pointer and the
    // content lands on disk under `blobs/<xx>/<rest>`, never inline
    let repo = repo_id(&server, "proj").await;
    let blob = rev_objects(&local, &[&head])
        .into_iter()
        .find(|(_, kind, _)| *kind == gix_object::Kind::Blob)
        .expect("one blob")
        .0;
    assert_eq!(
        server
            .store()
            .get_object(repo, &blob)
            .await
            .expect("get object"),
        Some(miscreant::storage::values::ObjectRecord::BlobPointer { size: 70_000 })
    );
    let hex = blob.to_hex().to_string();
    let blob_path = storage.path().join("blobs").join(&hex[..2]).join(&hex[2..]);
    assert!(
        blob_path.exists(),
        "offloaded blob not found at {blob_path:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_delete_a_branch_via_empty_source_refspec() {
    // given: a repo with main and a feature branch both pushed
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "f.txt", b"one\n", "one");
    let url = format!("{}/proj.git", server.base_url());
    assert!(
        push(&local, &url, &[], "main:refs/heads/main")
            .status
            .success()
    );
    assert!(
        push(&local, &url, &[], "main:refs/heads/feature")
            .status
            .success()
    );
    let repo = repo_id(&server, "proj").await;
    assert!(ref_oid(&server, repo, "refs/heads/feature").await.is_some());

    // when: pushing an empty source deletes the branch
    let out = push(&local, &url, &[], ":refs/heads/feature");

    // then: the push succeeds and only the feature branch is gone
    assert!(
        out.status.success(),
        "delete push failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(ref_oid(&server, repo, "refs/heads/feature").await, None);
    assert!(ref_oid(&server, repo, "refs/heads/main").await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_a_non_fast_forward_push_then_accept_a_force_push() {
    // given: one repo has published main; a second, unrelated repo has a
    // divergent main
    let server = TestServer::spawn(test_config()).await;
    let url = format!("{}/proj.git", server.base_url());
    let a = server.tempdir().join("a");
    init_repo(&a);
    let c1 = commit_file(&a, "f.txt", b"from a\n", "a");
    assert!(push(&a, &url, &[], "main:refs/heads/main").status.success());

    let b = server.tempdir().join("b");
    init_repo(&b);
    let d1 = commit_file(&b, "f.txt", b"from b\n", "b");
    let repo = repo_id(&server, "proj").await;

    // when: b pushes without force — the client reconciles against the
    // advertised tip and refuses a non-fast-forward update
    let rejected = push(&b, &url, &[], "main:refs/heads/main");

    // then: the push fails and the published ref is untouched
    assert!(
        !rejected.status.success(),
        "non-fast-forward push unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("rejected") || stderr.contains("fetch first"),
        "expected a rejection message, got: {stderr}"
    );
    assert_eq!(
        ref_oid(&server, repo, "refs/heads/main").await,
        Some(oid_of(&c1))
    );

    // when: b force-pushes — the compare-and-swap holds (old-oid is the
    // advertised tip) so the server overwrites the ref
    let forced = push(&b, &url, &["--force"], "main:refs/heads/main");

    // then
    assert!(
        forced.status.success(),
        "force push failed: {}",
        String::from_utf8_lossy(&forced.stderr)
    );
    assert_eq!(
        ref_oid(&server, repo, "refs/heads/main").await,
        Some(oid_of(&d1))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_auto_create_an_unknown_repository_on_push() {
    // given: a repo pushed to a name the server has never seen
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local");
    init_repo(&local);
    let head = commit_file(&local, "f.txt", b"hi\n", "hi");
    let url = format!("{}/brand/new.git", server.base_url());
    assert!(
        server
            .store()
            .lookup_repo("brand/new")
            .await
            .expect("lookup")
            .is_none()
    );

    // when
    let out = push(&local, &url, &[], "main:refs/heads/main");

    // then: the repository was auto-created and now holds the ref
    assert!(
        out.status.success(),
        "push to unknown repo failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let repo = repo_id(&server, "brand/new").await;
    assert_eq!(
        ref_oid(&server, repo, "refs/heads/main").await,
        Some(oid_of(&head))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_report_ng_for_a_stale_old_oid() {
    // given: main published at c1 by a real push
    let server = TestServer::spawn(test_config()).await;
    let url = format!("{}/proj.git", server.base_url());
    let a = server.tempdir().join("a");
    init_repo(&a);
    let c1 = commit_file(&a, "f.txt", b"one\n", "one");
    assert!(push(&a, &url, &[], "main:refs/heads/main").status.success());
    let repo = repo_id(&server, "proj").await;

    // given: a self-contained pack for a new commit in an unrelated repo
    let b = server.tempdir().join("b");
    init_repo(&b);
    let d1 = commit_file(&b, "f.txt", b"two\n", "two");
    let pack = pack_revs(&b, &[], &[&d1]);

    // given: an update command whose claimed old-oid does not match the
    // server's current tip (a stale/racing client, which the git CLI never
    // sends because it reconciles against the advert first)
    let stale_old = "a".repeat(40);
    let command = format!(
        "{stale_old} {} refs/heads/main\0report-status",
        rev_parse(&b, &d1)
    );
    let mut body = pkt(command.as_bytes());
    body.extend_from_slice(b"0000");
    body.extend_from_slice(&pack);

    // when
    let response = reqwest::Client::new()
        .post(format!("{url}/git-receive-pack"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(body)
        .send()
        .await
        .expect("send receive-pack request");

    // then: HTTP 200 with the pack unpacked but the ref rejected as non-ff
    assert_eq!(response.status(), 200);
    let report = response.bytes().await.expect("report body");
    assert!(
        contains(&report, b"unpack ok"),
        "expected `unpack ok`, got {:?}",
        String::from_utf8_lossy(&report)
    );
    assert!(
        contains(&report, b"ng refs/heads/main non-fast-forward"),
        "expected a non-fast-forward ng, got {:?}",
        String::from_utf8_lossy(&report)
    );
    // then: the published ref is unchanged
    assert_eq!(
        ref_oid(&server, repo, "refs/heads/main").await,
        Some(oid_of(&c1))
    );
}

/// Whether `needle` appears anywhere in `haystack`.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
