mod common;

use std::path::PathBuf;

use gix_hash::ObjectId;

use common::{TestServer, commit_file, git, git_ok, init_repo, test_config};
use miscreant::Config;
use miscreant::git::walk::Walker;
use miscreant::storage::keys::RepoId;
use miscreant::storage::values::CommitGraphRecord;
use miscreant::storage::{BlobStore, Durability, ObjectDb};

/// Push three linear commits to a fresh server-side repo named `proj`.
/// Returns the local repo path, the push URL, and the three commit ids
/// oldest-first.
fn push_three_commits(server: &TestServer) -> (PathBuf, String, Vec<String>) {
    let local = server.tempdir().join("local");
    init_repo(&local);
    let commits = vec![
        commit_file(&local, "a.txt", b"alpha\n", "add a"),
        commit_file(&local, "b.txt", b"beta\n", "add b"),
        commit_file(&local, "c.txt", b"gamma\n", "add c"),
    ];
    let url = format!("{}/proj.git", server.base_url());
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    (local, url, commits)
}

/// Build a [`Walker`] over the server's store for `repo`. The inline
/// threshold on this freshly-built [`ObjectDb`] need not match the server's:
/// rebuilding the commit graph only ever reads commit and tag bodies, never
/// blob content, so the threshold cannot affect the outcome.
fn walker_for(server: &TestServer, repo: RepoId) -> Walker {
    let store = server.store().clone();
    let blobs = BlobStore::new(store.object_store());
    let objectdb = ObjectDb::new(store.clone(), blobs, 65536);
    Walker::new(store, objectdb, repo)
}

fn oid(hex: &str) -> ObjectId {
    ObjectId::from_hex(hex.as_bytes()).expect("valid hex")
}

#[tokio::test(flavor = "multi_thread")]
async fn should_rebuild_commit_graph_identical_to_pre_wipe_snapshot() {
    // given: three pushed commits and their pre-wipe commit-graph records
    let server = TestServer::spawn(test_config()).await;
    let (_, _, commits) = push_three_commits(&server);
    let repo = server
        .store()
        .lookup_repo("proj")
        .await
        .expect("lookup")
        .expect("repo exists")
        .id;
    let mut snapshot = Vec::new();
    for hex in &commits {
        let record = server
            .store()
            .get_commit_graph(repo, &oid(hex))
            .await
            .expect("get graph")
            .expect("record exists before wipe");
        snapshot.push(record);
    }

    // when: every commit-graph record is deleted, then rebuilt from the objects
    server
        .store()
        .wipe_commit_graph_for_test(repo)
        .await
        .expect("wipe");
    for hex in &commits {
        assert_eq!(
            server
                .store()
                .get_commit_graph(repo, &oid(hex))
                .await
                .expect("get graph"),
            None
        );
    }
    let count = walker_for(&server, repo)
        .rebuild_commit_graph()
        .await
        .expect("rebuild");

    // then: the rebuilt records are identical to the pre-wipe snapshot
    assert_eq!(count, commits.len());
    for (hex, expected) in commits.iter().zip(snapshot) {
        assert_eq!(
            server
                .store()
                .get_commit_graph(repo, &oid(hex))
                .await
                .expect("get graph"),
            Some(expected)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_successfully_after_a_wipe_without_rebuilding() {
    // given: pushed history whose commit-graph is wiped, with no rebuild run
    let server = TestServer::spawn(test_config()).await;
    let (_, url, _) = push_three_commits(&server);
    let repo = server
        .store()
        .lookup_repo("proj")
        .await
        .expect("lookup")
        .expect("repo exists")
        .id;
    server
        .store()
        .wipe_commit_graph_for_test(repo)
        .await
        .expect("wipe");

    // when: a fresh clone is served purely by fetch's lazy backfill
    let clone_dir = server.tempdir().join("clone");
    let output = git(
        server.tempdir(),
        &["clone", &url, clone_dir.to_str().expect("utf-8 clone path")],
    );

    // then
    assert!(
        output.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_overwrite_a_corrupted_generation_with_the_correct_value() {
    // given: a pushed chain whose middle commit's generation is corrupted
    let server = TestServer::spawn(test_config()).await;
    let (_, _, commits) = push_three_commits(&server);
    let repo = server
        .store()
        .lookup_repo("proj")
        .await
        .expect("lookup")
        .expect("repo exists")
        .id;
    let middle = oid(&commits[1]);
    let mut corrupted: CommitGraphRecord = server
        .store()
        .get_commit_graph(repo, &middle)
        .await
        .expect("get graph")
        .expect("record exists");
    let correct_generation = corrupted.generation;
    corrupted.generation = 9999; // deliberately wrong
    server
        .store()
        .put_commit_graph(repo, &middle, &corrupted, Durability::Durable)
        .await
        .expect("corrupt record");

    // when
    walker_for(&server, repo)
        .rebuild_commit_graph()
        .await
        .expect("rebuild");

    // then: the rebuild recomputed the correct generation from the objects
    let fixed = server
        .store()
        .get_commit_graph(repo, &middle)
        .await
        .expect("get graph")
        .expect("record exists");
    assert_eq!(fixed.generation, correct_generation);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_print_the_rebuilt_record_count_from_the_cli() {
    // given: a file-backed store with three pushed commits, closed so the
    // offline CLI process below can open the same path
    let dir = tempfile::tempdir().expect("tempdir");
    let storage_url = format!("file://{}", dir.path().display());
    let config = Config {
        storage_url: storage_url.clone(),
        ..Config::default()
    };
    let server = TestServer::spawn(config).await;
    push_three_commits(&server);
    server.store().close().await.expect("close store");
    drop(server);

    // when: the compiled binary is run offline against the same store
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_miscreant"))
        .args([
            "--storage-url",
            &storage_url,
            "rebuild-graph",
            "--repo",
            "proj",
        ])
        .output()
        .expect("run miscreant binary");

    // then
    assert!(
        output.status.success(),
        "rebuild-graph failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(
        stdout.contains('3'),
        "expected a record count of 3 in: {stdout}"
    );
}

#[tokio::test]
async fn should_exit_nonzero_for_an_unknown_repo() {
    // given: an empty file-backed store with no repositories
    let dir = tempfile::tempdir().expect("tempdir");
    let storage_url = format!("file://{}", dir.path().display());

    // when
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_miscreant"))
        .args([
            "--storage-url",
            &storage_url,
            "rebuild-graph",
            "--repo",
            "nope",
        ])
        .output()
        .expect("run miscreant binary");

    // then
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("nope"), "stderr: {stderr}");
}
