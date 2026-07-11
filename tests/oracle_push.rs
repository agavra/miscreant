//! Differential + invariant push matrix: push the same operation to a miscreant
//! server and a `git-http-backend` reference server and compare the
//! client-observable outcome (exit status and the resulting ref advertisement),
//! plus invariant-only checks that a pushed history clones back fsck-clean and
//! that racing pushes to one ref resolve to a single winner.
//!
//! Push always speaks receive-pack v0 (there is no v2 receive-pack), so unlike
//! the fetch matrix there is no protocol axis here.

mod common;

use std::path::Path;
use std::process::Output;
use std::sync::Arc;

use common::{
    CloneExpectation, FLUSH, OracleServer, Protocol, TestServer, assert_clone_invariants,
    assert_index_pack_strict, assert_same_exit, assert_same_ls_remote, clone_proto, commit_file,
    extract_pack, fetch_body, git, git_ok, init_repo, pack_revs, parse_pkts, pkt, post_upload_pack,
    test_config,
};

// Every test drives the blocking `git`/HTTP client against the in-process
// servers, so a multi-threaded runtime is required: the client blocks its
// thread while making calls the servers must answer concurrently.

/// Spawn a miscreant server and a reference server. The reference server needs
/// its bare repo created up front (and made push-accepting); miscreant
/// auto-creates on first push, so the create path is exercised there directly.
async fn spawn_pair(repo: &str) -> (TestServer, OracleServer, String, String) {
    let mc = TestServer::spawn(test_config()).await;
    let or = OracleServer::spawn().await;
    or.create_bare_repo(repo);
    let mc_url = format!("{}/{repo}.git", mc.base_url());
    let or_url = format!("{}/{repo}.git", or.base_url());
    (mc, or, mc_url, or_url)
}

/// Push `refspecs` from `src` to `url` with extra `git push` flags, returning
/// the process output.
fn push(src: &Path, url: &str, extra: &[&str], refspecs: &[&str]) -> Output {
    let mut args = vec!["push"];
    args.extend_from_slice(extra);
    args.push(url);
    args.extend_from_slice(refspecs);
    git(src, &args)
}

/// Push the same operation to both servers, returning `(miscreant, reference)`.
fn push_each(
    src: &Path,
    mc_url: &str,
    or_url: &str,
    extra: &[&str],
    refspecs: &[&str],
) -> (Output, Output) {
    (
        push(src, mc_url, extra, refspecs),
        push(src, or_url, extra, refspecs),
    )
}

/// Assert a push succeeded, surfacing stderr on failure.
fn assert_pushed(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The oid `refname` resolves to at `url`, via `git ls-remote` run from `dir`.
fn remote_oid(dir: &Path, url: &str, refname: &str) -> String {
    let out = git_ok(dir, &["ls-remote", url, refname]);
    String::from_utf8(out)
        .expect("utf-8 ls-remote")
        .split_whitespace()
        .next()
        .expect("ls-remote oid")
        .to_owned()
}

/// The full `git ls-remote` advertisement text for `url`, run from `dir`.
fn remote_refs(dir: &Path, url: &str) -> String {
    String::from_utf8(git_ok(dir, &["ls-remote", url])).expect("utf-8 ls-remote")
}

/// Deterministic pseudo-random bytes that resist zlib, so two near-identical
/// revisions of them are worth encoding as a delta rather than two full blobs.
fn delta_friendly_bytes() -> Vec<u8> {
    let mut state = 0x1234_5678u32;
    (0..64 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

/// Whether `needle` appears anywhere in `haystack`.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[tokio::test(flavor = "multi_thread")]
async fn should_create_a_new_repository_on_push_like_the_reference_server() {
    // given: a fresh history and a name neither server has seen (miscreant
    // auto-creates; the reference repo was created empty by spawn_pair)
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "a.txt", b"alpha\n", "add a");
    let tip = commit_file(&src, "b.txt", b"beta\n", "add b");

    // when: the same history is pushed to both servers
    let (mc_out, or_out) = push_each(&src, &mc_url, &or_url, &[], &["main:refs/heads/main"]);

    // then: both accept and advertise the same tip on refs/heads/main
    assert_same_exit(&mc_out, &or_out);
    assert_pushed(&mc_out, "miscreant create push");
    assert_pushed(&or_out, "reference create push");
    assert_same_ls_remote(&src, &mc_url, &or_url);
    assert_eq!(remote_oid(&src, &mc_url, "refs/heads/main"), tip);
    assert_eq!(remote_oid(&src, &or_url, "refs/heads/main"), tip);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_fast_forward_an_incremental_push_like_the_reference_server() {
    // given: a base commit already published on both servers
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "a.txt", b"alpha\n", "add a");
    let (base_mc, base_or) = push_each(&src, &mc_url, &or_url, &[], &["main:refs/heads/main"]);
    assert_pushed(&base_mc, "miscreant base push");
    assert_pushed(&base_or, "reference base push");

    // when: two more commits are pushed incrementally (git sends a thin pack
    // delta'd against the base each server already holds)
    commit_file(&src, "b.txt", b"beta\n", "add b");
    let tip = commit_file(&src, "c.txt", b"gamma\n", "add c");
    let (mc_out, or_out) = push_each(&src, &mc_url, &or_url, &[], &["main:refs/heads/main"]);

    // then: both fast-forward identically to the new tip
    assert_same_exit(&mc_out, &or_out);
    assert_pushed(&mc_out, "miscreant incremental push");
    assert_pushed(&or_out, "reference incremental push");
    assert_same_ls_remote(&src, &mc_url, &or_url);
    assert_eq!(remote_oid(&src, &mc_url, "refs/heads/main"), tip);

    // then: miscreant reproduces the thin-pack history fsck-clean on clone
    let clone = clone_proto(&mc, Protocol::V2, &mc_url, "clone");
    let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
    let files: [(&str, &[u8]); 3] = [
        ("a.txt", b"alpha\n"),
        ("b.txt", b"beta\n"),
        ("c.txt", b"gamma\n"),
    ];
    assert_clone_invariants(
        &clone,
        &CloneExpectation {
            url: &mc_url,
            refs: &refs,
            tip: &tip,
            files: Some(&files),
            promisor: false,
        },
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_resolve_a_delta_heavy_thin_push_like_the_reference_server() {
    // given: a large blob published on both servers as a delta base
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    let base = delta_friendly_bytes();
    commit_file(&src, "versioned.bin", &base, "base revision");
    let (base_mc, base_or) = push_each(&src, &mc_url, &or_url, &[], &["main:refs/heads/main"]);
    assert_pushed(&base_mc, "miscreant base push");
    assert_pushed(&base_or, "reference base push");

    // when: a substantially rewritten revision of that blob is pushed (git
    // sends a thin pack whose objects are REF_DELTAs against the server-held
    // base rather than whole objects)
    let mut target = base.clone();
    for byte in &mut target[24 * 1024..40 * 1024] {
        *byte = !*byte;
    }
    let tip = commit_file(&src, "versioned.bin", &target, "target revision");
    let (mc_out, or_out) = push_each(&src, &mc_url, &or_url, &[], &["main:refs/heads/main"]);

    // then: both servers resolve the thin pack and accept the push
    assert_same_exit(&mc_out, &or_out);
    assert_pushed(&mc_out, "miscreant delta push");
    assert_pushed(&or_out, "reference delta push");
    assert_same_ls_remote(&src, &mc_url, &or_url);

    // then: a pack miscreant serves back is strictly self-contained (every
    // delta resolves against a base it stored), and a clone is fsck-clean
    let pack = fetch_pack(&mc, "proj", &tip).await;
    assert_index_pack_strict(&mc, &pack, "delta-idx");
    let clone = clone_proto(&mc, Protocol::V2, &mc_url, "clone");
    let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
    let files: [(&str, &[u8]); 1] = [("versioned.bin", target.as_slice())];
    assert_clone_invariants(
        &clone,
        &CloneExpectation {
            url: &mc_url,
            refs: &refs,
            tip: &tip,
            files: Some(&files),
            promisor: false,
        },
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_a_non_fast_forward_push_then_force_like_the_reference_server() {
    // given: main published at c1 on both servers
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let a = mc.tempdir().join("a");
    init_repo(&a);
    let c1 = commit_file(&a, "f.txt", b"from a\n", "a");
    let (pa_mc, pa_or) = push_each(&a, &mc_url, &or_url, &[], &["main:refs/heads/main"]);
    assert_pushed(&pa_mc, "miscreant publish");
    assert_pushed(&pa_or, "reference publish");

    // given: a second, unrelated history diverging from what is published
    let b = mc.tempdir().join("b");
    init_repo(&b);
    let d1 = commit_file(&b, "f.txt", b"from b\n", "b");

    // when: b pushes without --force (the client refuses a non-fast-forward
    // update after reconciling against the advertised tip)
    let (mc_rej, or_rej) = push_each(&b, &mc_url, &or_url, &[], &["main:refs/heads/main"]);

    // then: both reject and leave the published ref untouched
    assert_same_exit(&mc_rej, &or_rej);
    assert!(!mc_rej.status.success(), "miscreant accepted a non-ff push");
    assert!(!or_rej.status.success(), "reference accepted a non-ff push");
    assert_same_ls_remote(&b, &mc_url, &or_url);
    assert_eq!(remote_oid(&b, &mc_url, "refs/heads/main"), c1);
    assert_eq!(remote_oid(&b, &or_url, "refs/heads/main"), c1);

    // when: b force-pushes (the CAS holds on the advertised tip)
    let (mc_force, or_force) = push_each(
        &b,
        &mc_url,
        &or_url,
        &["--force"],
        &["main:refs/heads/main"],
    );

    // then: both accept and advance identically to the divergent tip
    assert_same_exit(&mc_force, &or_force);
    assert_pushed(&mc_force, "miscreant force push");
    assert_pushed(&or_force, "reference force push");
    assert_same_ls_remote(&b, &mc_url, &or_url);
    assert_eq!(remote_oid(&b, &mc_url, "refs/heads/main"), d1);
    assert_eq!(remote_oid(&b, &or_url, "refs/heads/main"), d1);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_delete_a_branch_like_the_reference_server() {
    // given: main and a feature branch published on both servers
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "f.txt", b"one\n", "one");
    let (seed_mc, seed_or) = push_each(
        &src,
        &mc_url,
        &or_url,
        &[],
        &["main:refs/heads/main", "main:refs/heads/feature"],
    );
    assert_pushed(&seed_mc, "miscreant seed push");
    assert_pushed(&seed_or, "reference seed push");

    // when: the feature branch is deleted with an empty source refspec
    let (mc_out, or_out) = push_each(&src, &mc_url, &or_url, &[], &[":refs/heads/feature"]);

    // then: both delete feature, keep main, and agree on the advertisement
    assert_same_exit(&mc_out, &or_out);
    assert_pushed(&mc_out, "miscreant delete push");
    assert_pushed(&or_out, "reference delete push");
    assert_same_ls_remote(&src, &mc_url, &or_url);
    let refs = remote_refs(&src, &mc_url);
    assert!(
        refs.contains("refs/heads/main"),
        "main should remain: {refs}"
    );
    assert!(
        !refs.contains("refs/heads/feature"),
        "feature should be gone: {refs}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_let_exactly_one_of_two_racing_pushes_win() {
    // given: a base commit published on miscreant, and two distinct children of
    // that base, each with a self-contained pack
    let mc = TestServer::spawn(test_config()).await;
    let mc_url = format!("{}/proj.git", mc.base_url());
    let src = mc.tempdir().join("src");
    init_repo(&src);
    let base = commit_file(&src, "f.txt", b"base\n", "base");
    assert_pushed(
        &push(&src, &mc_url, &[], &["main:refs/heads/main"]),
        "seed push",
    );

    let child_a = commit_file(&src, "f.txt", b"child a\n", "child a");
    let pack_a = pack_revs(&src, &[], &[&child_a]);
    assert_pushed(&git(&src, &["reset", "--hard", &base]), "reset to base");
    let child_b = commit_file(&src, "f.txt", b"child b\n", "child b");
    let pack_b = pack_revs(&src, &[], &[&child_b]);
    assert_ne!(child_a, child_b);

    // given: two receive-pack bodies that BOTH claim the base as the old-oid
    // but advance main to different children
    let body_a = receive_pack_body(&base, &child_a, "refs/heads/main", &pack_a);
    let body_b = receive_pack_body(&base, &child_b, "refs/heads/main", &pack_b);

    // when: both POSTs are gated on a barrier so they hit the CAS concurrently
    let endpoint = format!("{mc_url}/git-receive-pack");
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let task_a = tokio::spawn(fire_receive_pack(endpoint.clone(), body_a, barrier.clone()));
    let task_b = tokio::spawn(fire_receive_pack(endpoint.clone(), body_b, barrier.clone()));
    let report_a = task_a.await.expect("join push a");
    let report_b = task_b.await.expect("join push b");

    // then: both packs unpack, but exactly one ref update is accepted and the
    // loser reports a clean non-fast-forward `ng`
    assert!(contains(&report_a, b"unpack ok"), "push a failed to unpack");
    assert!(contains(&report_b, b"unpack ok"), "push b failed to unpack");
    let a_won = contains(&report_a, b"ok refs/heads/main");
    let b_won = contains(&report_b, b"ok refs/heads/main");
    assert!(
        a_won ^ b_won,
        "expected exactly one winner, got a_won={a_won} b_won={b_won}\na={}\nb={}",
        String::from_utf8_lossy(&report_a),
        String::from_utf8_lossy(&report_b),
    );
    let (loser, winner_child) = if a_won {
        (&report_b, &child_a)
    } else {
        (&report_a, &child_b)
    };
    assert!(
        contains(loser, b"ng refs/heads/main non-fast-forward"),
        "loser should report a clean non-fast-forward ng, got {}",
        String::from_utf8_lossy(loser)
    );

    // then: the final ref settled on the winner's child
    assert_eq!(remote_oid(&src, &mc_url, "refs/heads/main"), *winner_child);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_back_a_pushed_multi_file_history() {
    // given: a multi-file, multi-commit history pushed to miscreant
    let mc = TestServer::spawn(test_config()).await;
    let mc_url = format!("{}/proj.git", mc.base_url());
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "a.txt", b"alpha\n", "add a");
    commit_file(&src, "dir/b.txt", b"beta\n", "add nested b");
    let tip = commit_file(&src, "c.txt", b"gamma\n", "add c");
    assert_pushed(
        &push(&src, &mc_url, &[], &["main:refs/heads/main"]),
        "round-trip push",
    );

    // when: the history is cloned back from miscreant
    let clone = clone_proto(&mc, Protocol::V2, &mc_url, "clone");

    // then: the clone is fsck-clean and reproduces the pushed tree exactly
    let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
    let files: [(&str, &[u8]); 3] = [
        ("a.txt", b"alpha\n"),
        ("dir/b.txt", b"beta\n"),
        ("c.txt", b"gamma\n"),
    ];
    assert_clone_invariants(
        &clone,
        &CloneExpectation {
            url: &mc_url,
            refs: &refs,
            tip: &tip,
            files: Some(&files),
            promisor: false,
        },
    );
}

/// Build a receive-pack request body: one `<old> <new> <ref>` command carrying
/// the `report-status` capability, a flush, then the pack bytes.
fn receive_pack_body(old: &str, new: &str, refname: &str, pack: &[u8]) -> Vec<u8> {
    let command = format!("{old} {new} {refname}\0report-status");
    let mut body = pkt(command.as_bytes());
    body.extend_from_slice(FLUSH);
    body.extend_from_slice(pack);
    body
}

/// Build a receive-pack POST, wait on `barrier`, then fire it — so the barrier
/// gates the actual network send and two callers race on the server's CAS.
/// Returns the report-status body.
async fn fire_receive_pack(
    url: String,
    body: Vec<u8>,
    barrier: Arc<tokio::sync::Barrier>,
) -> Vec<u8> {
    let request = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(body);
    barrier.wait().await;
    let response = request.send().await.expect("send receive-pack request");
    assert_eq!(response.status(), 200);
    response.bytes().await.expect("report body").to_vec()
}

/// Issue a raw protocol-v2 fetch of `tip` against `server`'s `repo`, accepting
/// `ofs-delta`, and return the extracted pack bytes.
async fn fetch_pack(server: &TestServer, repo: &str, tip: &str) -> Vec<u8> {
    let want = format!("want {tip}");
    let response = post_upload_pack(
        &server.base_url(),
        repo,
        fetch_body(&[want.as_str(), "ofs-delta", "done"]),
    )
    .await;
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("response body");
    extract_pack(&parse_pkts(&bytes))
}
