mod common;

use common::{
    OracleServer, Protocol, TestServer, assert_same_ls_remote, assert_same_object_set,
    assert_same_refs, clone_proto, commit_file, git_ok, init_repo, test_config,
};

// The blocking `git` subprocess drives HTTP that the two in-process servers
// answer concurrently, so this uses a multi-threaded runtime.
#[tokio::test(flavor = "multi_thread")]
async fn should_agree_with_the_reference_server_on_a_cloned_repository() {
    // given: a miscreant server and a git-http-backend reference server, each
    // holding the very same pushed history
    let miscreant = TestServer::spawn(test_config()).await;
    let oracle = OracleServer::spawn().await;
    oracle.create_bare_repo("proj");

    let local = miscreant.tempdir().join("local");
    init_repo(&local);
    commit_file(&local, "a.txt", b"alpha\n", "add a");
    commit_file(&local, "b.txt", b"beta\n", "add b");

    let miscreant_url = format!("{}/proj.git", miscreant.base_url());
    let oracle_url = format!("{}/proj.git", oracle.base_url());
    git_ok(&local, &["push", &miscreant_url, "main:refs/heads/main"]);
    git_ok(&local, &["push", &oracle_url, "main:refs/heads/main"]);

    // when: each server is cloned (the miscreant tempdir is a scratch area for
    // both clones)
    let from_miscreant = clone_proto(&miscreant, Protocol::V2, &miscreant_url, "from-miscreant");
    let from_oracle = clone_proto(&miscreant, Protocol::V2, &oracle_url, "from-oracle");

    // then: both servers advertise identical refs, and both clones carry the
    // same objects and refs
    assert_same_ls_remote(&local, &miscreant_url, &oracle_url);
    assert_same_object_set(&from_miscreant, &from_oracle);
    assert_same_refs(&from_miscreant, &from_oracle);
}
