use super::*;

/// Assert `git fsck --full --strict` finds a well-formed object graph in
/// `dir`: exit 0, and no `missing` or `dangling` object noted on stderr.
#[allow(dead_code)]
pub fn assert_fsck_strict(dir: &Path) {
    let output = git(dir, &["fsck", "--full", "--strict"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "git fsck --full --strict failed: {stderr}"
    );
    assert!(
        !stderr.contains("missing") && !stderr.contains("dangling"),
        "git fsck --full --strict reported a problem: {stderr}"
    );
}

/// Pipe `pack_bytes` through `git index-pack --strict` in a fresh scratch
/// repository under `server`'s tempdir, asserting the pack is well formed and
/// every object it contains resolves within it. `name` seeds both the
/// scratch repo's directory name and the generated index file's basename.
#[allow(dead_code)]
pub fn assert_index_pack_strict(server: &TestServer, pack_bytes: &[u8], name: &str) {
    let dir = server.tempdir().join(name);
    init_repo(&dir);
    let idx_path = dir.join(format!("{name}.idx"));
    git_ok_with_input(
        &dir,
        &[
            "index-pack",
            "--strict",
            "--stdin",
            "-o",
            idx_path.to_str().expect("utf-8 idx path"),
        ],
        pack_bytes,
    );
}

/// Assert `clone_dir` sits exactly on `expected_oid`.
#[allow(dead_code)]
pub fn assert_reproduces_tip(clone_dir: &Path, expected_oid: &str) {
    assert_eq!(rev_parse(clone_dir, "HEAD"), expected_oid);
}

/// Assert `git ls-remote url` (run from `dir`) advertises exactly
/// `expected_refs`, compared as a sorted set since `ls-remote` does not
/// guarantee ref order.
#[allow(dead_code)]
pub fn assert_ls_remote_matches(dir: &Path, url: &str, expected_refs: &[(&str, &str)]) {
    let output = git_ok(dir, &["ls-remote", url]);
    let mut actual: Vec<(String, String)> = String::from_utf8(output)
        .expect("utf-8 ls-remote output")
        .lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let oid = fields.next().expect("ls-remote oid").to_owned();
            let name = fields.next().expect("ls-remote ref name").to_owned();
            (name, oid)
        })
        .collect();
    actual.sort();
    let mut expected: Vec<(String, String)> = expected_refs
        .iter()
        .map(|(name, oid)| ((*name).to_owned(), (*oid).to_owned()))
        .collect();
    expected.sort();
    assert_eq!(actual, expected, "ls-remote {url} did not match");
}

/// Assert `git rev-list --objects --all` succeeds in `dir`: every reachable
/// object resolves. When `promisor` is set (a partial clone), objects a
/// filter deliberately dropped are tolerated instead of counted as missing.
#[allow(dead_code)]
pub fn assert_reachability_closed(dir: &Path, promisor: bool) {
    let mut args = vec!["rev-list", "--objects", "--all"];
    if promisor {
        args.push("--missing=allow-promisor");
    }
    git_ok(dir, &args);
}

/// Assert every `(path, contents)` pair in `expected_files` matches the bytes
/// on disk under `clone_dir`, and that `git status --porcelain` reports a
/// clean working tree.
#[allow(dead_code)]
pub fn assert_worktree_matches(clone_dir: &Path, expected_files: &[(&str, &[u8])]) {
    for (path, contents) in expected_files {
        let actual =
            std::fs::read(clone_dir.join(path)).unwrap_or_else(|err| panic!("read {path}: {err}"));
        assert_eq!(&actual, contents, "{path} contents did not match");
    }
    let status = git_ok(clone_dir, &["status", "--porcelain"]);
    assert!(
        status.is_empty(),
        "git status --porcelain not clean: {}",
        String::from_utf8_lossy(&status)
    );
}

/// What a clone is expected to look like, for [`assert_clone_invariants`]:
/// the url it was cloned from (checked via `ls-remote`), the refs and tip it
/// should carry, its working-tree contents (`None` when the clone skipped
/// checkout), and whether it is a partial clone (relaxes the reachability
/// check to tolerate the objects a filter deliberately dropped).
#[allow(dead_code)]
pub struct CloneExpectation<'a> {
    pub url: &'a str,
    pub refs: &'a [(&'a str, &'a str)],
    pub tip: &'a str,
    pub files: Option<&'a [(&'a str, &'a [u8])]>,
    pub promisor: bool,
}

/// Run the full invariant battery against `clone_dir`: a strictly well-formed
/// object graph, the expected tip, a matching ref advertisement, a
/// reachability-closed object set, and — when `expect.files` is given — a
/// working tree that matches with nothing left dirty.
#[allow(dead_code)]
pub fn assert_clone_invariants(clone_dir: &Path, expect: &CloneExpectation) {
    assert_fsck_strict(clone_dir);
    assert_reproduces_tip(clone_dir, expect.tip);
    assert_ls_remote_matches(clone_dir, expect.url, expect.refs);
    assert_reachability_closed(clone_dir, expect.promisor);
    if let Some(files) = expect.files {
        assert_worktree_matches(clone_dir, files);
    }
}
