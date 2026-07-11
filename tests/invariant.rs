mod common;

use std::path::PathBuf;

use common::{
    CloneExpectation, Protocol, TestServer, assert_clone_invariants, assert_index_pack_strict,
    assert_reachability_closed, clone_proto, commit_file, git_ok, init_repo, pack_revs, rev_parse,
    test_config,
};

/// Initialize a local repository under the server's tempdir and push its
/// `main` to the named server repo. Returns the local repo path and URL.
fn push_fixture(server: &TestServer, repo: &str) -> (PathBuf, String) {
    let local = server.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    let url = format!("{}/{repo}.git", server.base_url());
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    (local, url)
}

/// Clone `url` into `<server tempdir>/<name>` with extra flags (e.g.
/// `--filter=blob:none --no-checkout`) inserted before the url, asserting
/// success.
fn clone_with(server: &TestServer, url: &str, name: &str, extra: &[&str]) -> PathBuf {
    let clone_dir = server.tempdir().join(name);
    let mut args = vec!["clone"];
    args.extend_from_slice(extra);
    let dir_str = clone_dir.to_str().expect("utf-8 clone path").to_owned();
    args.push(url);
    args.push(&dir_str);
    git_ok(server.tempdir(), &args);
    clone_dir
}

// The `git` subprocess blocks its thread on HTTP the in-process server must
// answer concurrently, so the CLI-driven tests use a multi-threaded runtime.

#[tokio::test(flavor = "multi_thread")]
async fn should_pass_the_full_invariant_battery_on_a_clean_clone() {
    // given: a pushed fixture with two commits on main
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a fresh full clone
    let clone_dir = clone_proto(&server, Protocol::V2, &url, "clone");

    // then: the clone satisfies every invariant at once — a strictly clean
    // fsck, the pushed tip, both HEAD and main advertised at that tip, a
    // reachability-closed object set, and a matching, clean working tree
    assert_clone_invariants(
        &clone_dir,
        &CloneExpectation {
            url: &url,
            refs: &[("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())],
            tip: tip.as_str(),
            files: Some(&[("a.txt", b"alpha\n"), ("b.txt", b"beta\n")]),
            promisor: false,
        },
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_close_reachability_only_under_the_promisor_variant_for_a_partial_clone() {
    // given: two commits, so history has two distinct blobs, cloned
    // partially (blob content omitted) and without a checkout
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    let clone_dir = clone_with(
        &server,
        &url,
        "partial",
        &["--filter=blob:none", "--no-checkout"],
    );

    // when/then: the promisor variant tolerates the blobs the filter
    // deliberately dropped and the traversal still succeeds
    assert_reachability_closed(&clone_dir, true);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_accept_a_full_closure_pack_under_index_pack_strict() {
    // given: a local repo with two commits, so pack-objects has a nontrivial
    // closure to walk
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local-pack");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    let tip = rev_parse(&local, "HEAD");

    // when: a pack of the tip's full reachability closure
    let pack = pack_revs(&local, &[], &[&tip]);

    // then: index-pack accepts it under --strict
    assert_index_pack_strict(&server, &pack, "strict-index");
}
