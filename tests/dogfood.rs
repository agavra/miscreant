mod common;

use std::path::{Path, PathBuf};

use common::{
    Protocol, TestServer, assert_fsck_strict, assert_reproduces_tip, clone_proto, git, git_ok,
    init_repo, rev_parse,
};
use miscreant::Config;

/// The ref pinned by default when `MISCREANT_DOGFOOD_GIT_REF` is unset — a
/// stable git/git release tag, chosen so any reasonably current mirror has it.
const DEFAULT_PINNED_REF: &str = "v2.40.0";

/// Resolve `git_ref` to a commit oid in `mirror`, or `None` if the mirror
/// does not have it. Read-only: `rev-parse` never writes to the repository.
fn resolve_pinned_commit(mirror: &Path, git_ref: &str) -> Option<String> {
    let output = git(
        mirror,
        &["rev-parse", "--verify", &format!("{git_ref}^{{commit}}")],
    );
    if !output.status.success() {
        return None;
    }
    Some(
        String::from_utf8(output.stdout)
            .expect("utf-8 rev-parse output")
            .trim()
            .to_owned(),
    )
}

/// Clone `url` back from miscreant under `protocol` and assert the round
/// trip landed cleanly: a strictly well-formed object graph, the pinned
/// commit at HEAD, and a tree identical to the mirror's.
fn assert_round_trip(
    server: &TestServer,
    url: &str,
    protocol: Protocol,
    name: &str,
    pinned: &str,
    mirror_tree: &str,
) {
    let clone_dir = clone_proto(server, protocol, url, name);
    assert_fsck_strict(&clone_dir);
    assert_reproduces_tip(&clone_dir, pinned);
    let clone_tree = rev_parse(&clone_dir, "HEAD^{tree}");
    assert_eq!(
        clone_tree, mirror_tree,
        "{name}: clone tree did not match the mirror's tree"
    );
}

// This test drives real, complex history through miscreant end to end to
// shake out pack/delta/negotiation bugs that small synthetic fixtures cannot
// reach. It never runs implicitly: it is `#[ignore]`d, and even under
// `--ignored` it skips cleanly unless pointed at a local mirror, so no test
// run ever requires the network or a multi-hundred-megabyte checkout.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn should_round_trip_a_pinned_git_git_commit_through_both_protocols() {
    // given: a local git/git mirror and a pinned commit that resolves in it
    let mirror = std::env::var("MISCREANT_DOGFOOD_GIT_MIRROR").unwrap_or_default();
    if mirror.trim().is_empty() {
        eprintln!("set MISCREANT_DOGFOOD_GIT_MIRROR to a local git/git mirror to run this test");
        return;
    }
    let mirror = PathBuf::from(mirror);
    let git_ref = std::env::var("MISCREANT_DOGFOOD_GIT_REF")
        .unwrap_or_else(|_| DEFAULT_PINNED_REF.to_owned());
    let Some(pinned) = resolve_pinned_commit(&mirror, &git_ref) else {
        eprintln!("{git_ref} does not resolve to a commit in {mirror:?}; skipping");
        return;
    };
    let mirror_tree = rev_parse(&mirror, &format!("{pinned}^{{tree}}"));

    // given: a file-backed miscreant server (the volume here is too large for
    // the in-memory store) and the pinned commit's closure fetched into a
    // scratch source repo — the mirror itself is only ever read, never
    // written: fetch and push both run with the scratch repo as cwd
    let storage = tempfile::TempDir::new().expect("storage dir");
    let config = Config {
        storage_url: format!("file://{}", storage.path().display()),
        ..Config::default()
    };
    let server = TestServer::spawn(config).await;
    let scratch = server.tempdir().join("source");
    init_repo(&scratch);
    let mirror_str = mirror.to_str().expect("utf-8 mirror path");
    git_ok(&scratch, &["fetch", "--no-tags", mirror_str, &git_ref]);
    let url = format!("{}/proj.git", server.base_url());

    // when: the pinned commit is pushed into miscreant as main
    let push = git(
        &scratch,
        &["push", &url, &format!("{pinned}:refs/heads/main")],
    );
    assert!(
        push.status.success(),
        "push of pinned commit failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );

    // then: cloning it back under each protocol version reproduces an
    // fsck-clean, tree-identical repository
    assert_round_trip(
        &server,
        &url,
        Protocol::V0,
        "clone-v0",
        &pinned,
        &mirror_tree,
    );
    assert_round_trip(
        &server,
        &url,
        Protocol::V2,
        "clone-v2",
        &pinned,
        &mirror_tree,
    );
}
