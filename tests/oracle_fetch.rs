//! Differential + invariant fetch matrix: every scenario seeds identical
//! content into a miscreant server and a `git-http-backend` reference server,
//! runs the client operation against each under both protocol versions, checks
//! the two servers agree on the client-observable outcome, and runs the clone
//! invariant battery on the miscreant clone.

mod common;

use std::path::{Path, PathBuf};

use common::{
    CloneExpectation, OracleServer, Protocol, TestServer, assert_clone_invariants,
    assert_fsck_strict, assert_index_pack_strict, assert_same_exit, assert_same_ls_remote,
    assert_same_object_set, assert_same_refs, clone_proto, commit_file, extract_pack, fetch_body,
    git_ok, git_ok_proto, git_proto, init_repo, parse_pkts, post_upload_pack, rev_parse,
    test_config, unpack_oids,
};

/// Commit/tagger identity flags for the hermetic `git` helper.
const IDENTITY: [&str; 4] = [
    "-c",
    "user.name=miscreant",
    "-c",
    "user.email=miscreant@example.com",
];

/// A short, stable directory-name suffix for a protocol, so clones taken under
/// both protocols into the same scratch tempdir do not collide.
fn proto_tag(proto: Protocol) -> &'static str {
    match proto {
        Protocol::V0 => "v0",
        Protocol::V2 => "v2",
    }
}

/// Spawn a miscreant server and a reference server, each holding an empty repo
/// `<repo>` ready to be pushed to. The reference repo has partial-clone filters
/// enabled so it honours `--filter` exactly as miscreant does over v2.
async fn spawn_pair(repo: &str) -> (TestServer, OracleServer, String, String) {
    let mc = TestServer::spawn(test_config()).await;
    let or = OracleServer::spawn().await;
    let oracle_repo = or.create_bare_repo(repo);
    git_ok(&oracle_repo, &["config", "uploadpack.allowFilter", "true"]);
    let mc_url = format!("{}/{repo}.git", mc.base_url());
    let or_url = format!("{}/{repo}.git", or.base_url());
    (mc, or, mc_url, or_url)
}

/// Push `refspecs` from `src` to both servers, asserting both succeed.
fn push_both(src: &Path, mc_url: &str, or_url: &str, refspecs: &[&str]) {
    let mut mc_args = vec!["push", mc_url];
    mc_args.extend_from_slice(refspecs);
    git_ok(src, &mc_args);
    let mut or_args = vec!["push", or_url];
    or_args.extend_from_slice(refspecs);
    git_ok(src, &or_args);
}

/// Clone `url` under `protocol` with extra `git clone` flags (e.g.
/// `--filter=blob:none`) into `<server tempdir>/<name>`, asserting success.
fn clone_with(
    server: &TestServer,
    protocol: Protocol,
    url: &str,
    name: &str,
    extra: &[&str],
) -> PathBuf {
    let dest = server.tempdir().join(name);
    let dest_str = dest.to_str().expect("utf-8 clone path").to_owned();
    let mut args = vec!["clone"];
    args.extend_from_slice(extra);
    args.push(url);
    args.push(&dest_str);
    let out = git_proto(server.tempdir(), protocol, &args);
    assert!(
        out.status.success(),
        "clone {extra:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    dest
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

/// The sorted object-id set of `clone`, each id prefixed with `?` when the
/// object is a (promised) missing one. Uses `--missing=print`, which reports
/// missing objects without fetching them — unlike a plain `rev-list --objects`,
/// which faults every missing object in from the promisor remote and so would
/// erase the very filtering this compares.
fn partial_object_markers(clone: &Path) -> Vec<String> {
    let output = git_ok(
        clone,
        &["rev-list", "--objects", "--missing=print", "--all"],
    );
    let mut markers: Vec<String> = String::from_utf8(output)
        .expect("utf-8 rev-list output")
        .lines()
        .map(|line| match line.strip_prefix('?') {
            Some(oid) => format!("?{}", oid.split_whitespace().next().unwrap_or(oid)),
            None => line.split_whitespace().next().unwrap_or(line).to_owned(),
        })
        .collect();
    markers.sort();
    markers
}

/// Assert two partial clones reference the same objects and agree on exactly
/// which are present versus (promise-)missing, without faulting anything in.
fn assert_same_partial_objects(a: &Path, b: &Path) {
    assert_eq!(
        partial_object_markers(a),
        partial_object_markers(b),
        "partial clones disagree on present/missing objects"
    );
}

/// Assert `present_oid` is a present object in `clone` and `missing_oid` is a
/// (promised) missing one. Uses `--missing=print` so the check itself never
/// faults the missing object in.
fn assert_blob_presence(clone: &Path, present_oid: &str, missing_oid: &str) {
    let output = git_ok(
        clone,
        &["rev-list", "--objects", "--missing=print", "--all"],
    );
    let text = String::from_utf8(output).expect("utf-8 rev-list output");
    let mut present = false;
    let mut missing = false;
    for line in text.lines() {
        if let Some(oid) = line.strip_prefix('?') {
            missing |= oid == missing_oid;
        } else if line.split_whitespace().next() == Some(present_oid) {
            present = true;
        }
    }
    assert!(present, "expected {present_oid} present in {clone:?}");
    assert!(missing, "expected {missing_oid} missing in {clone:?}");
}

// The blocking `git` subprocess drives HTTP that the in-process servers must
// answer concurrently, so every CLI-driven test uses a multi-threaded runtime.

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_a_single_branch_repository_like_the_reference_server() {
    // given: identical single-branch history on miscreant and the reference
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "a.txt", b"alpha\n", "add a");
    let tip = commit_file(&src, "b.txt", b"beta\n", "add b");
    push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);

    for proto in [Protocol::V0, Protocol::V2] {
        let tag = proto_tag(proto);
        // when: each server is cloned under the protocol
        let from_mc = clone_proto(&mc, proto, &mc_url, &format!("mc-{tag}"));
        let from_or = clone_proto(&mc, proto, &or_url, &format!("or-{tag}"));

        // then: the servers agree and the miscreant clone is well formed
        assert_same_ls_remote(&src, &mc_url, &or_url);
        assert_same_object_set(&from_mc, &from_or);
        assert_same_refs(&from_mc, &from_or);

        let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
        let files: [(&str, &[u8]); 2] = [("a.txt", b"alpha\n"), ("b.txt", b"beta\n")];
        assert_clone_invariants(
            &from_mc,
            &CloneExpectation {
                url: &mc_url,
                refs: &refs,
                tip: &tip,
                files: Some(&files),
                promisor: false,
            },
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_many_refs_and_tags_like_the_reference_server() {
    // given: main, a feature branch, a lightweight tag, and an annotated tag
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "a.txt", b"alpha\n", "add a");
    let main_tip = commit_file(&src, "b.txt", b"beta\n", "add b");
    git_ok(&src, &["checkout", "-q", "-b", "feature"]);
    let feature_tip = commit_file(&src, "f.txt", b"feature\n", "add f");
    git_ok(&src, &["checkout", "-q", "main"]);
    git_ok(&src, &["tag", "light"]);
    let mut tag_args = IDENTITY.to_vec();
    tag_args.extend_from_slice(&["tag", "-a", "v1", "-m", "release"]);
    git_ok(&src, &tag_args);
    push_both(
        &src,
        &mc_url,
        &or_url,
        &[
            "main:refs/heads/main",
            "feature:refs/heads/feature",
            "light:refs/tags/light",
            "v1:refs/tags/v1",
        ],
    );
    let annotated = rev_parse(&src, "refs/tags/v1");

    for proto in [Protocol::V0, Protocol::V2] {
        let tag = proto_tag(proto);
        // when: each server is cloned under the protocol
        let from_mc = clone_proto(&mc, proto, &mc_url, &format!("mc-{tag}"));
        let from_or = clone_proto(&mc, proto, &or_url, &format!("or-{tag}"));

        // then: tag refs and their peeled `^{}` lines agree, and the clones
        // carry the same refs and objects
        assert_same_ls_remote(&src, &mc_url, &or_url);
        assert_same_object_set(&from_mc, &from_or);
        assert_same_refs(&from_mc, &from_or);

        let refs = [
            ("HEAD", main_tip.as_str()),
            ("refs/heads/main", main_tip.as_str()),
            ("refs/heads/feature", feature_tip.as_str()),
            ("refs/tags/light", main_tip.as_str()),
            ("refs/tags/v1", annotated.as_str()),
            ("refs/tags/v1^{}", main_tip.as_str()),
        ];
        let files: [(&str, &[u8]); 2] = [("a.txt", b"alpha\n"), ("b.txt", b"beta\n")];
        assert_clone_invariants(
            &from_mc,
            &CloneExpectation {
                url: &mc_url,
                refs: &refs,
                tip: &main_tip,
                files: Some(&files),
                promisor: false,
            },
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_fast_forward_an_incremental_fetch_like_the_reference_server() {
    for proto in [Protocol::V0, Protocol::V2] {
        let tag = proto_tag(proto);
        // given: a clone of an initial commit from each server
        let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
        let src = mc.tempdir().join("src");
        init_repo(&src);
        commit_file(&src, "a.txt", b"alpha\n", "add a");
        push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);
        let mc_clone = clone_proto(&mc, proto, &mc_url, &format!("mc-{tag}"));
        let or_clone = clone_proto(&mc, proto, &or_url, &format!("or-{tag}"));

        // when: two more commits land and each clone fetches then fast-forwards
        commit_file(&src, "b.txt", b"beta\n", "add b");
        let tip = commit_file(&src, "c.txt", b"gamma\n", "add c");
        push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);
        git_ok_proto(&mc_clone, proto, &["fetch", "origin"]);
        git_ok_proto(&or_clone, proto, &["fetch", "origin"]);
        git_ok_proto(&mc_clone, proto, &["merge", "--ff-only", "origin/main"]);
        git_ok_proto(&or_clone, proto, &["merge", "--ff-only", "origin/main"]);

        // then: both clones sit on the new tip, stay fsck-clean, and agree
        assert_eq!(rev_parse(&mc_clone, "HEAD"), tip);
        assert_eq!(rev_parse(&or_clone, "HEAD"), tip);
        assert_fsck_strict(&mc_clone);
        assert_fsck_strict(&or_clone);
        assert_same_object_set(&mc_clone, &or_clone);
        assert_same_refs(&mc_clone, &or_clone);

        let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
        let files: [(&str, &[u8]); 3] = [
            ("a.txt", b"alpha\n"),
            ("b.txt", b"beta\n"),
            ("c.txt", b"gamma\n"),
        ];
        assert_clone_invariants(
            &mc_clone,
            &CloneExpectation {
                url: &mc_url,
                refs: &refs,
                tip: &tip,
                files: Some(&files),
                promisor: false,
            },
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_observe_a_forced_non_fast_forward_update_like_the_reference_server() {
    for proto in [Protocol::V0, Protocol::V2] {
        let tag = proto_tag(proto);
        // given: a clone taken before the server branch is force-updated onto a
        // divergent history
        let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
        let src = mc.tempdir().join("src");
        init_repo(&src);
        let base = commit_file(&src, "a.txt", b"alpha\n", "add a");
        let original = commit_file(&src, "b.txt", b"beta\n", "add b");
        push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);
        let mc_clone = clone_proto(&mc, proto, &mc_url, &format!("mc-{tag}"));
        let or_clone = clone_proto(&mc, proto, &or_url, &format!("or-{tag}"));

        // when: main is reset onto the base, rebuilt divergently, and force-pushed
        git_ok(&src, &["checkout", "-q", "-B", "main", &base]);
        let divergent = commit_file(&src, "d.txt", b"delta\n", "add d");
        assert_ne!(divergent, original);
        push_both(&src, &mc_url, &or_url, &["+main:refs/heads/main"]);
        git_ok_proto(&mc_clone, proto, &["fetch", "origin"]);
        git_ok_proto(&or_clone, proto, &["fetch", "origin"]);

        // then: both clones observe the same divergent remote tip and agree
        assert_eq!(rev_parse(&mc_clone, "origin/main"), divergent);
        assert_eq!(rev_parse(&or_clone, "origin/main"), divergent);
        assert_same_ls_remote(&src, &mc_url, &or_url);
        assert_same_object_set(&mc_clone, &or_clone);
        assert_same_refs(&mc_clone, &or_clone);

        // the fetch leaves the local branch untouched, so HEAD stays on the
        // original tip while the server now advertises the divergent one
        let refs = [
            ("HEAD", divergent.as_str()),
            ("refs/heads/main", divergent.as_str()),
        ];
        let files: [(&str, &[u8]); 2] = [("a.txt", b"alpha\n"), ("b.txt", b"beta\n")];
        assert_clone_invariants(
            &mc_clone,
            &CloneExpectation {
                url: &mc_url,
                refs: &refs,
                tip: &original,
                files: Some(&files),
                promisor: false,
            },
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_an_empty_repository_like_the_reference_server() {
    // given: an empty repository (unborn HEAD, no refs) on each server
    let mc = TestServer::spawn(test_config()).await;
    let or = OracleServer::spawn().await;
    mc.store().create_repo("empty").await.expect("create repo");
    or.create_bare_repo("empty");
    let mc_url = format!("{}/empty.git", mc.base_url());
    let or_url = format!("{}/empty.git", or.base_url());

    for proto in [Protocol::V0, Protocol::V2] {
        let tag = proto_tag(proto);
        // when: each empty server is cloned
        let mc_clone = mc.tempdir().join(format!("mc-{tag}"));
        let or_clone = mc.tempdir().join(format!("or-{tag}"));
        let mc_out = git_proto(
            mc.tempdir(),
            proto,
            &["clone", &mc_url, mc_clone.to_str().expect("utf-8 path")],
        );
        let or_out = git_proto(
            mc.tempdir(),
            proto,
            &["clone", &or_url, or_clone.to_str().expect("utf-8 path")],
        );

        // then: both clients exit identically (a warn-but-succeed clone of an
        // empty repo), carry no refs, and advertise nothing
        assert_same_exit(&mc_out, &or_out);
        assert!(
            mc_out.status.success(),
            "miscreant empty clone failed: {}",
            String::from_utf8_lossy(&mc_out.stderr)
        );
        assert_same_ls_remote(mc.tempdir(), &mc_url, &or_url);
        assert_same_refs(&mc_clone, &or_clone);
        let mc_refs = git_ok(&mc_clone, &["for-each-ref"]);
        assert!(
            mc_refs.is_empty(),
            "empty clone carried refs: {}",
            String::from_utf8_lossy(&mc_refs)
        );
    }
}

// miscreant advertises the `filter` argument only on the v2 `fetch` command,
// so partial clones are exercised under v2 only. Over the classic protocol a
// real client sees no advertised filter capability and silently drops the
// filter, cloning in full — which would diverge from the reference server,
// which honours `--filter` under both protocols.
#[tokio::test(flavor = "multi_thread")]
async fn should_partially_clone_with_blob_none_and_fault_in_like_the_reference_server() {
    // given: history whose tip rewrites a file, leaving one historical blob
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "a.txt", b"alpha\n", "add a");
    commit_file(&src, "a.txt", b"alpha v2\n", "modify a");
    let tip = commit_file(&src, "b.txt", b"beta\n", "add b");
    let tip_blob = rev_parse(&src, "HEAD:a.txt");
    let historical_blob = rev_parse(&src, "HEAD~2:a.txt");
    push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);

    // when: a blob:none partial clone WITH checkout faults the tip blobs in
    let from_mc = clone_with(&mc, Protocol::V2, &mc_url, "mc", &["--filter=blob:none"]);
    let from_or = clone_with(&mc, Protocol::V2, &or_url, "or", &["--filter=blob:none"]);

    // then: both honour the filter and agree, and the miscreant clone is a
    // valid promisor clone with the tip checked out (tip blobs faulted in, the
    // rewritten file's historical blob still absent)
    assert_same_ls_remote(&src, &mc_url, &or_url);
    assert_same_partial_objects(&from_mc, &from_or);
    assert_same_refs(&from_mc, &from_or);
    assert_blob_presence(&from_mc, &tip_blob, &historical_blob);

    let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
    let files: [(&str, &[u8]); 2] = [("a.txt", b"alpha v2\n"), ("b.txt", b"beta\n")];
    assert_clone_invariants(
        &from_mc,
        &CloneExpectation {
            url: &mc_url,
            refs: &refs,
            tip: &tip,
            files: Some(&files),
            promisor: true,
        },
    );
}

// Partial clones are exercised under v2 only; see the blob:none test above.
#[tokio::test(flavor = "multi_thread")]
async fn should_partially_clone_with_a_blob_size_limit_like_the_reference_server() {
    // given: a small blob and a blob over the 1KiB limit
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    commit_file(&src, "small.txt", b"alpha\n", "add small");
    let big: Vec<u8> = (0..2000u32).map(|i| (i % 251) as u8).collect();
    let tip = commit_file(&src, "big.bin", &big, "add big");
    let small_oid = rev_parse(&src, "HEAD:small.txt");
    let big_oid = rev_parse(&src, "HEAD:big.bin");
    push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);

    // when: a blob:limit partial clone WITHOUT checkout keeps the large blob out
    let extra = ["--filter=blob:limit=1k", "--no-checkout"];
    let from_mc = clone_with(&mc, Protocol::V2, &mc_url, "mc", &extra);
    let from_or = clone_with(&mc, Protocol::V2, &or_url, "or", &extra);

    // then: the small blob transfers and the large one is filtered out on both
    assert_same_ls_remote(&src, &mc_url, &or_url);
    assert_same_partial_objects(&from_mc, &from_or);
    assert_same_refs(&from_mc, &from_or);
    assert_blob_presence(&from_mc, &small_oid, &big_oid);

    let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
    assert_clone_invariants(
        &from_mc,
        &CloneExpectation {
            url: &mc_url,
            refs: &refs,
            tip: &tip,
            files: None,
            promisor: true,
        },
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_a_delta_friendly_history_like_the_reference_server() {
    // given: two large, near-identical revisions of one path on both servers
    let (mc, _or, mc_url, or_url) = spawn_pair("proj").await;
    let src = mc.tempdir().join("src");
    init_repo(&src);
    let base = delta_friendly_bytes();
    commit_file(&src, "versioned.bin", &base, "base revision");
    let mut target = base.clone();
    target[32 * 1024] ^= 0xff;
    let tip = commit_file(&src, "versioned.bin", &target, "target revision");
    push_both(&src, &mc_url, &or_url, &["main:refs/heads/main"]);

    for proto in [Protocol::V0, Protocol::V2] {
        let tag = proto_tag(proto);
        // when: each server is cloned (the delta form is not client-observable)
        let from_mc = clone_proto(&mc, proto, &mc_url, &format!("mc-{tag}"));
        let from_or = clone_proto(&mc, proto, &or_url, &format!("or-{tag}"));

        // then: both clones carry identical objects regardless of delta encoding
        assert_same_ls_remote(&src, &mc_url, &or_url);
        assert_same_object_set(&from_mc, &from_or);
        assert_same_refs(&from_mc, &from_or);

        let refs = [("HEAD", tip.as_str()), ("refs/heads/main", tip.as_str())];
        let files: [(&str, &[u8]); 1] = [("versioned.bin", target.as_slice())];
        assert_clone_invariants(
            &from_mc,
            &CloneExpectation {
                url: &mc_url,
                refs: &refs,
                tip: &tip,
                files: Some(&files),
                promisor: false,
            },
        );
    }
}

// The delta form is not client-observable, so this half of the ofs-delta
// scenario is invariant-only against miscreant: it drives the send-side delta
// planner both with and without the `ofs-delta` capability and checks both
// packs are strictly valid and carry the same objects.
#[tokio::test(flavor = "multi_thread")]
async fn should_serve_delta_friendly_history_with_and_without_ofs_delta() {
    // given: two large, near-identical revisions of one path on miscreant
    let mc = TestServer::spawn(test_config()).await;
    mc.store().create_repo("proj").await.expect("create repo");
    let src = mc.tempdir().join("src");
    init_repo(&src);
    let base = delta_friendly_bytes();
    commit_file(&src, "versioned.bin", &base, "base revision");
    let mut target = base.clone();
    target[32 * 1024] ^= 0xff;
    let tip = commit_file(&src, "versioned.bin", &target, "target revision");
    let mc_url = format!("{}/proj.git", mc.base_url());
    git_ok(&src, &["push", &mc_url, "main:refs/heads/main"]);

    // when: a raw v2 fetch of the tip WITH ofs-delta accepted, and one WITHOUT
    let with = fetch_pack(&mc, &tip, true).await;
    let without = fetch_pack(&mc, &tip, false).await;

    // then: both packs are strictly well formed and unpack to the same objects
    assert_index_pack_strict(&mc, &with, "idx-with-ofs");
    assert_index_pack_strict(&mc, &without, "idx-without-ofs");
    let with_oids = unpack_oids(&mc, &with, "unpack-with-ofs");
    let without_oids = unpack_oids(&mc, &without, "unpack-without-ofs");
    assert_eq!(with_oids, without_oids, "delta form changed the object set");
    assert!(with_oids.contains(&rev_parse(&src, "HEAD:versioned.bin")));
    assert!(with_oids.contains(&rev_parse(&src, "HEAD~1:versioned.bin")));
}

/// Issue a raw protocol-v2 fetch of `tip` against `server`'s `proj` repo,
/// optionally accepting `ofs-delta`, and return the extracted pack bytes.
async fn fetch_pack(server: &TestServer, tip: &str, ofs_delta: bool) -> Vec<u8> {
    let want = format!("want {tip}");
    let args = if ofs_delta {
        vec![want.as_str(), "ofs-delta", "done"]
    } else {
        vec![want.as_str(), "done"]
    };
    let response = post_upload_pack(&server.base_url(), "proj", fetch_body(&args)).await;
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("response body");
    extract_pack(&parse_pkts(&bytes))
}
