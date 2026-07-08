mod common;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use common::{
    TestServer, commit_file, git, git_ok, init_repo, rev_objects, rev_parse, test_config,
};
use gix_object::Kind;

/// Commit identity flags for the hermetic `git` helper.
const IDENTITY: [&str; 4] = [
    "-c",
    "user.name=miscreant",
    "-c",
    "user.email=miscreant@example.com",
];

const DELIM: &[u8] = b"0001";
const FLUSH: &[u8] = b"0000";

/// Build a data pkt-line: a 4-hex length prefix (covering itself) then the
/// payload verbatim.
fn pkt(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data);
    out
}

/// Frame a `fetch` command request: the command, the `object-format`
/// capability, a delimiter, then one pkt-line per argument, then a flush.
fn fetch_body(args: &[&str]) -> Vec<u8> {
    let mut body = pkt(b"command=fetch\n");
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

/// One decoded pkt-line of a response body.
#[derive(Debug, PartialEq, Eq)]
enum Pkt {
    Flush,
    Delim,
    Data(Vec<u8>),
}

/// Split a raw pkt-line stream into its packets.
fn parse_pkts(mut bytes: &[u8]) -> Vec<Pkt> {
    let mut pkts = Vec::new();
    while !bytes.is_empty() {
        let len_hex = std::str::from_utf8(&bytes[..4]).expect("pkt length prefix");
        let len = usize::from_str_radix(len_hex, 16).expect("hex pkt length");
        match len {
            0 => {
                pkts.push(Pkt::Flush);
                bytes = &bytes[4..];
            }
            1 => {
                pkts.push(Pkt::Delim);
                bytes = &bytes[4..];
            }
            _ => {
                pkts.push(Pkt::Data(bytes[4..len].to_vec()));
                bytes = &bytes[len..];
            }
        }
    }
    pkts
}

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

/// Clone `url` into `<server tempdir>/<name>`, asserting success.
fn clone_repo(server: &TestServer, url: &str, name: &str) -> PathBuf {
    let clone_dir = server.tempdir().join(name);
    let clone = git(
        server.tempdir(),
        &["clone", url, clone_dir.to_str().expect("utf-8 clone path")],
    );
    assert!(
        clone.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    clone_dir
}

/// Assert `git fsck --full` finds nothing wrong in `dir`.
fn assert_fsck_clean(dir: &Path) {
    git_ok(dir, &["fsck", "--full"]);
}

/// Like [`clone_repo`], but with extra `git clone` flags (e.g.
/// `--filter=blob:none --no-checkout`) inserted before the URL.
fn clone_repo_with(server: &TestServer, url: &str, name: &str, extra: &[&str]) -> PathBuf {
    let clone_dir = server.tempdir().join(name);
    let mut args = vec!["clone"];
    args.extend_from_slice(extra);
    let dir_str = clone_dir.to_str().expect("utf-8 clone path").to_owned();
    args.push(url);
    args.push(&dir_str);
    let clone = git(server.tempdir(), &args);
    assert!(
        clone.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    clone_dir
}

/// Split `git rev-list --objects --missing=print` output into the oids
/// present locally (`<oid> <path>`) and the oids reported missing
/// (`?<oid>`).
fn parse_missing(output: &[u8]) -> (HashSet<String>, HashSet<String>) {
    let mut present = HashSet::new();
    let mut missing = HashSet::new();
    for line in String::from_utf8(output.to_vec())
        .expect("utf-8 rev-list output")
        .lines()
    {
        if let Some(oid) = line.strip_prefix('?') {
            missing.insert(oid.to_owned());
        } else {
            let oid = line.split_whitespace().next().unwrap_or(line);
            present.insert(oid.to_owned());
        }
    }
    (present, missing)
}

// The `git` subprocess blocks its thread on HTTP the in-process server must
// answer concurrently, so the CLI-driven tests use a multi-threaded runtime.

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_pushed_history_with_a_clean_fsck() {
    // given: two pushed commits
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a fresh clone
    let clone_dir = clone_repo(&server, &url, "clone");

    // then: the clone is object-complete and sits on the pushed tip
    assert_fsck_clean(&clone_dir);
    assert_eq!(rev_parse(&clone_dir, "HEAD"), tip);
    assert_eq!(
        std::fs::read(clone_dir.join("a.txt")).expect("read a.txt"),
        b"alpha\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_round_trip_a_blob_larger_than_the_inline_threshold_through_clone() {
    // given: a pushed file bigger than the 64KiB inline threshold, so its
    // blob is offloaded to the blob store on push and read back on fetch
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let big: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    commit_file(&local, "big.bin", &big, "add big blob");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when
    let clone_dir = clone_repo(&server, &url, "clone");

    // then: the offloaded blob's bytes round-tripped exactly
    assert_fsck_clean(&clone_dir);
    assert_eq!(
        std::fs::read(clone_dir.join("big.bin")).expect("read big.bin"),
        big
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_fetch_and_fast_forward_new_commits_into_an_existing_clone() {
    // given: a clone taken before two further commits were pushed
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let clone_dir = clone_repo(&server, &url, "clone");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    let tip = commit_file(&local, "c.txt", b"gamma\n", "add c");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: the existing clone fetches, then fast-forwards
    git_ok(&clone_dir, &["fetch", "origin"]);
    assert_eq!(rev_parse(&clone_dir, "origin/main"), tip);
    let mut pull = IDENTITY.to_vec();
    pull.extend_from_slice(&["pull", "--ff-only"]);
    git_ok(&clone_dir, &pull);

    // then: the clone sits on the new tip with the new files present
    assert_eq!(rev_parse(&clone_dir, "HEAD"), tip);
    assert_eq!(
        std::fs::read(clone_dir.join("c.txt")).expect("read c.txt"),
        b"gamma\n"
    );
    assert_fsck_clean(&clone_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_the_annotated_tag_object() {
    // given: an annotated tag pushed alongside main
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let mut tag_args = IDENTITY.to_vec();
    tag_args.extend_from_slice(&["tag", "-a", "v1", "-m", "release", "HEAD"]);
    git_ok(&local, &tag_args);
    let tag_oid = rev_parse(&local, "refs/tags/v1");
    git_ok(&local, &["push", &url, "v1:refs/tags/v1"]);

    // when
    let clone_dir = clone_repo(&server, &url, "clone");

    // then: the tag object itself (not just the peeled commit) came across
    let kind = git_ok(&clone_dir, &["cat-file", "-t", &tag_oid]);
    assert_eq!(String::from_utf8(kind).expect("utf-8 type").trim(), "tag");
    assert_eq!(rev_parse(&clone_dir, "refs/tags/v1"), tag_oid);
    assert_fsck_clean(&clone_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reclone_cleanly_after_several_pushes() {
    // given: three pushes — the fixture, two more commits on main, and a
    // side branch forked from the first commit
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    let main_tip = commit_file(&local, "c.txt", b"gamma\n", "add c");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    git_ok(&local, &["checkout", "-q", "-b", "feature", "main~2"]);
    let feature_tip = commit_file(&local, "f.txt", b"feature\n", "add f");
    git_ok(&local, &["push", &url, "feature:refs/heads/feature"]);

    // when: a fresh clone after all of it
    let clone_dir = clone_repo(&server, &url, "clone");

    // then: every branch tip is present and the object store is intact
    assert_fsck_clean(&clone_dir);
    assert_eq!(rev_parse(&clone_dir, "origin/main"), main_tip);
    assert_eq!(rev_parse(&clone_dir, "origin/feature"), feature_tip);
    assert_eq!(rev_parse(&clone_dir, "HEAD"), main_tip);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_acknowledge_common_haves_and_stream_the_pack_when_not_done() {
    // given: two pushed commits, so the client can claim the parent
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let parent = rev_parse(&local, "HEAD");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a raw fetch that wants the tip, has the parent, and is not done
    let body = fetch_body(&[&format!("want {tip}"), &format!("have {parent}")]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: single-round negotiation — the acknowledgments section ACKs the
    // common have and signals ready, then the packfile section follows in
    // the same response
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("body");
    let pkts = parse_pkts(&bytes);
    assert_eq!(pkts[0], Pkt::Data(b"acknowledgments\n".to_vec()));
    assert_eq!(pkts[1], Pkt::Data(format!("ACK {parent}\n").into_bytes()));
    assert_eq!(pkts[2], Pkt::Data(b"ready\n".to_vec()));
    assert_eq!(pkts[3], Pkt::Delim);
    assert_eq!(pkts[4], Pkt::Data(b"packfile\n".to_vec()));
    assert_eq!(pkts.last(), Some(&Pkt::Flush));

    // The pack rides in side-band channel-1 packets; without `no-progress`
    // at least one channel-2 progress line is present too.
    let mut pack = Vec::new();
    let mut saw_progress = false;
    for p in &pkts[5..] {
        if let Pkt::Data(data) = p {
            match data.first() {
                Some(1) => pack.extend_from_slice(&data[1..]),
                Some(2) => saw_progress = true,
                other => panic!("unexpected side-band channel: {other:?}"),
            }
        }
    }
    assert!(pack.starts_with(b"PACK"), "pack bytes: {:?}", &pack[..4]);
    assert!(saw_progress, "no channel-2 progress packet seen");
}

#[tokio::test]
async fn should_err_not_our_ref_for_an_unknown_want() {
    // given: a repository that does not contain the wanted object
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");
    let bogus = "f".repeat(40);

    // when
    let body = fetch_body(&[&format!("want {bogus}"), "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: an in-band ERR naming the missing ref
    assert_eq!(response.status(), 200);
    let text =
        String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8 body");
    assert!(
        text.contains("ERR not our ref") && text.contains(&bogus),
        "body: {text}"
    );
}

#[tokio::test]
async fn should_err_for_an_unsupported_filter_argument() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");
    let bogus = "f".repeat(40);

    // when: a fetch requesting a filter kind the server does not implement
    let body = fetch_body(&[&format!("want {bogus}"), "filter tree:0", "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then
    let text =
        String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8 body");
    assert!(
        text.contains("ERR unsupported filter: tree:0"),
        "body: {text}"
    );
}

#[tokio::test]
async fn should_err_when_fetch_names_no_wants() {
    // given
    let server = TestServer::spawn(test_config()).await;
    server.store().create_repo("proj").await.expect("create");

    // when: a fetch with no want lines at all
    let body = fetch_body(&["done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then
    let text =
        String::from_utf8(response.bytes().await.expect("body").to_vec()).expect("utf-8 body");
    assert!(
        text.contains("ERR fetch requires at least one want"),
        "body: {text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_with_blob_none_filter_and_omit_every_blob() {
    // given: two commits, so the history has two distinct blobs
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    let blob_oids: Vec<String> = rev_objects(&local, &["HEAD"])
        .into_iter()
        .filter(|(_, kind, _)| *kind == Kind::Blob)
        .map(|(oid, _, _)| oid.to_string())
        .collect();
    assert_eq!(blob_oids.len(), 2, "expected two distinct blobs");

    // when: a partial clone that omits all blob content, without checkout
    // (checkout would need to fault in blobs by arbitrary oid, which this
    // server does not yet serve)
    let clone_dir = clone_repo_with(
        &server,
        &url,
        "clone",
        &["--filter=blob:none", "--no-checkout"],
    );

    // then: every blob the clone would otherwise have is reported missing —
    // git only reports this when the server actually honored the filter,
    // since a server that silently ignored an unadvertised filter would have
    // sent (and the client would have stored) every blob.
    let (present, missing) = parse_missing(&git_ok(
        &clone_dir,
        &["rev-list", "--objects", "--missing=print", "HEAD"],
    ));
    for oid in &blob_oids {
        assert!(missing.contains(oid), "expected {oid} missing: {missing:?}");
        assert!(!present.contains(oid), "expected {oid} absent: {present:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_with_blob_limit_filter_and_include_only_small_blobs() {
    // given: a small blob (the fixture's `a.txt`) and a blob over 1KiB
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let small_oid = rev_parse(&local, "HEAD:a.txt");
    let big: Vec<u8> = (0..2000u32).map(|i| (i % 251) as u8).collect();
    commit_file(&local, "big.bin", &big, "add big");
    let big_oid = rev_parse(&local, "HEAD:big.bin");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when
    let clone_dir = clone_repo_with(
        &server,
        &url,
        "clone",
        &["--filter=blob:limit=1k", "--no-checkout"],
    );

    // then: the small blob transferred, the large one was filtered out
    let (present, missing) = parse_missing(&git_ok(
        &clone_dir,
        &["rev-list", "--objects", "--missing=print", "HEAD"],
    ));
    assert!(
        present.contains(&small_oid),
        "small blob present: {present:?}"
    );
    assert!(!missing.contains(&small_oid), "small blob not missing");
    assert!(missing.contains(&big_oid), "big blob missing: {missing:?}");
    assert!(!present.contains(&big_oid), "big blob not present");
}
