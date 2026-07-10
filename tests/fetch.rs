mod common;

use std::collections::HashSet;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use common::{
    TestServer, commit_file, git, git_ok, init_repo, rev_objects, rev_parse, test_config,
};
use gix_object::Kind;
use gix_pack::data::entry::Header;
use gix_pack::data::input::{BytesToEntriesIter, EntryDataMode, Mode};

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

/// POST a classic (v0) fetch request to a repo's upload-pack endpoint: no
/// `Git-Protocol` header, so the server serves the classic protocol.
async fn post_upload_pack_v0(base_url: &str, repo: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base_url}/{repo}/git-upload-pack"))
        .body(body)
        .send()
        .await
        .expect("send request")
}

/// Build a classic (v0) fetch request body: one `want` pkt-line per oid (the
/// first carrying `caps` after the oid when non-empty), a flush, then `done`.
fn v0_fetch_body(wants: &[&str], caps: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (index, want) in wants.iter().enumerate() {
        let line = if index == 0 && !caps.is_empty() {
            format!("want {want} {caps}\n")
        } else {
            format!("want {want}\n")
        };
        body.extend(pkt(line.as_bytes()));
    }
    body.extend_from_slice(FLUSH);
    body.extend(pkt(b"done\n"));
    body
}

/// Clone `url` with the classic (v0) protocol forced on, into
/// `<server tempdir>/<name>`, asserting success. The later `-c` overrides the
/// hermetic harness's `protocol.version=2`.
fn clone_repo_v0(server: &TestServer, url: &str, name: &str) -> PathBuf {
    let clone_dir = server.tempdir().join(name);
    let clone = git(
        server.tempdir(),
        &[
            "-c",
            "protocol.version=0",
            "clone",
            url,
            clone_dir.to_str().expect("utf-8 clone path"),
        ],
    );
    assert!(
        clone.status.success(),
        "v0 clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    clone_dir
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

/// Pull the raw pack bytes out of a fetch response: the side-band channel-1
/// payload of every packet after the `packfile` section header (channels 2
/// and 3 carry progress and errors and are dropped).
fn extract_pack(pkts: &[Pkt]) -> Vec<u8> {
    let mut pack = Vec::new();
    let mut in_packfile = false;
    for p in pkts {
        match p {
            Pkt::Data(data) if data.as_slice() == b"packfile\n" => in_packfile = true,
            Pkt::Data(data) if in_packfile && data.first() == Some(&1) => {
                pack.extend_from_slice(&data[1..]);
            }
            _ => {}
        }
    }
    pack
}

/// Pull the raw pack bytes out of a classic (v0) fetch response: the side-band
/// channel-1 payload of every packet. The leading `ACK`/`NAK` pkt-lines and
/// the channel-2/3 (progress/error) packets are skipped — a v0 response has no
/// `packfile` section header to key off, unlike the v2 form.
fn extract_sideband_pack(pkts: &[Pkt]) -> Vec<u8> {
    let mut pack = Vec::new();
    for p in pkts {
        if let Pkt::Data(data) = p
            && data.first() == Some(&1)
        {
            pack.extend_from_slice(&data[1..]);
        }
    }
    pack
}

/// The object count a pack's v2 header declares (bytes 8..12, big-endian).
fn pack_object_count(pack: &[u8]) -> u32 {
    assert!(pack.starts_with(b"PACK"), "not a pack: {:?}", pack.get(..4));
    u32::from_be_bytes(pack[8..12].try_into().expect("pack count field"))
}

/// Unpack `pack` into a fresh repository under the server's tempdir and return
/// the hex ids of the objects it contained. `unpack-objects` performs no
/// connectivity check, so a pack whose objects reference absent ones (a bare
/// tree want, or a filtered-out sibling blob) unpacks fine.
fn unpack_oids(server: &TestServer, pack: &[u8], name: &str) -> HashSet<String> {
    let dir = server.tempdir().join(name);
    init_repo(&dir);
    common::git_ok_with_input(&dir, &["unpack-objects", "-q"], pack);
    let listing = git_ok(
        &dir,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
    );
    String::from_utf8(listing)
        .expect("utf-8 object listing")
        .lines()
        .map(|line| line.trim().to_owned())
        .filter(|line| !line.is_empty())
        .collect()
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
async fn should_send_an_offset_delta_for_two_revisions_of_the_same_path() {
    // given: a history containing two large, nearly identical revisions of
    // one path. The deterministic bytes resist ordinary zlib compression, so
    // the delta is materially smaller than a second full entry.
    let server = TestServer::spawn(test_config()).await;
    let local = server.tempdir().join("local-ofs");
    init_repo(&local);
    let mut state = 0x1234_5678u32;
    let base: Vec<u8> = (0..64 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    commit_file(&local, "versioned.bin", &base, "base revision");
    let mut target = base.clone();
    target[32 * 1024] ^= 0xff;
    let tip = commit_file(&local, "versioned.bin", &target, "target revision");
    let url = format!("{}/proj.git", server.base_url());
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a fresh protocol-v2 fetch explicitly accepts OFS_DELTA.
    let body = fetch_body(&[&format!("want {tip}"), "ofs-delta", "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;
    assert_eq!(response.status(), 200);
    let pkts = parse_pkts(&response.bytes().await.expect("response body"));
    let pack = extract_pack(&pkts);

    // then: the response is a checksum-valid pack with at least one offset
    // delta, and Git itself can unpack the complete, self-contained result.
    let entries: Vec<_> = BytesToEntriesIter::new_from_header(
        BufReader::new(pack.as_slice()),
        Mode::Verify,
        EntryDataMode::Keep,
        gix_hash::Kind::Sha1,
    )
    .expect("read pack header")
    .collect::<Result<_, _>>()
    .expect("verify pack");
    assert!(
        entries
            .iter()
            .any(|entry| matches!(entry.header, Header::OfsDelta { .. })),
        "expected an OFS_DELTA entry"
    );
    let unpacked = unpack_oids(&server, &pack, "unpack-ofs-delta");
    assert!(unpacked.contains(&rev_parse(&local, "HEAD:versioned.bin")));
    assert!(unpacked.contains(&rev_parse(&local, "HEAD~1:versioned.bin")));
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

#[tokio::test(flavor = "multi_thread")]
async fn should_pack_exactly_the_two_blob_wants_by_arbitrary_oid() {
    // given: two commits, so history holds two distinct blobs
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    let blob_a = rev_parse(&local, "HEAD:a.txt");
    let blob_b = rev_parse(&local, "HEAD:b.txt");

    // when: a fetch that wants the two blobs by oid and is done — no commit
    // wants, so commit discovery and collection are skipped and the pack is
    // exactly the wanted objects (git's promisor backfill shape)
    let body = fetch_body(&[&format!("want {blob_a}"), &format!("want {blob_b}"), "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: the pack holds precisely those two blobs
    assert_eq!(response.status(), 200);
    let pkts = parse_pkts(&response.bytes().await.expect("body"));
    let pack = extract_pack(&pkts);
    assert_eq!(pack_object_count(&pack), 2);
    assert_eq!(
        unpack_oids(&server, &pack, "unpack-blobs"),
        HashSet::from([blob_a, blob_b])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_serve_a_bare_tree_want_by_arbitrary_oid() {
    // given: a pushed commit with a root tree
    let server = TestServer::spawn(test_config()).await;
    let (local, _url) = push_fixture(&server, "proj");
    let root_tree = rev_parse(&local, "HEAD^{tree}");

    // when: a fetch that wants only that tree by oid
    let body = fetch_body(&[&format!("want {root_tree}"), "done"]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: the pack holds precisely the tree object, unpackable on its own
    // even though its blob referent is absent
    assert_eq!(response.status(), 200);
    let pkts = parse_pkts(&response.bytes().await.expect("body"));
    let pack = extract_pack(&pkts);
    assert_eq!(pack_object_count(&pack), 1);
    let dir = server.tempdir().join("unpack-tree");
    assert_eq!(
        unpack_oids(&server, &pack, "unpack-tree"),
        HashSet::from([root_tree.clone()])
    );
    let kind = git_ok(&dir, &["cat-file", "-t", &root_tree]);
    assert_eq!(String::from_utf8(kind).expect("utf-8 type").trim(), "tree");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_pack_an_explicitly_wanted_blob_even_under_a_blob_none_filter() {
    // given: a single commit whose root tree holds two blobs
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    let commit = rev_parse(&local, "HEAD");
    let root_tree = rev_parse(&local, "HEAD^{tree}");
    let wanted_blob = rev_parse(&local, "HEAD:a.txt");
    let filtered_blob = rev_parse(&local, "HEAD:b.txt");

    // when: git's checkout-triggered backfill shape — a commit want, an
    // explicit blob want, and `filter blob:none` in the same request (real
    // git sends exactly this, per GIT_TRACE_PACKET of a partial-clone
    // checkout)
    let body = fetch_body(&[
        &format!("want {commit}"),
        &format!("want {wanted_blob}"),
        "filter blob:none",
        "done",
    ]);
    let response = post_upload_pack(&server.base_url(), "proj", body).await;

    // then: the filter drops the traversal-discovered blob but never the
    // explicitly requested one; the commit and its tree ride along
    assert_eq!(response.status(), 200);
    let pkts = parse_pkts(&response.bytes().await.expect("body"));
    let oids = unpack_oids(&server, &extract_pack(&pkts), "unpack-mixed");
    assert!(oids.contains(&commit), "commit packed: {oids:?}");
    assert!(oids.contains(&root_tree), "root tree packed: {oids:?}");
    assert!(
        oids.contains(&wanted_blob),
        "explicit blob packed despite the filter: {oids:?}"
    );
    assert!(
        !oids.contains(&filtered_blob),
        "traversal-discovered blob filtered out: {oids:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_with_blob_none_filter_and_check_out_the_working_tree() {
    // given: history that changes a file across commits
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "a.txt", b"alpha v2\n", "modify a");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a partial clone WITH checkout — checkout triggers a promisor
    // backfill of the tip's blobs by arbitrary oid
    let clone_dir = clone_repo_with(&server, &url, "clone", &["--filter=blob:none"]);

    // then: the working tree is materialized with the tip's contents and the
    // partial clone is self-consistent
    assert_fsck_clean(&clone_dir);
    assert_eq!(
        std::fs::read(clone_dir.join("a.txt")).expect("read a.txt"),
        b"alpha v2\n"
    );
    assert_eq!(
        std::fs::read(clone_dir.join("b.txt")).expect("read b.txt"),
        b"beta\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_fault_in_historical_blobs_when_diffing_with_log_p() {
    // given: a partial clone of a file that changed across two commits
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    commit_file(&local, "a.txt", b"alpha v2\n", "modify a");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    let clone_dir = clone_repo_with(&server, &url, "clone", &["--filter=blob:none"]);

    // when: `log -p` diffs each commit against its parent, faulting in the
    // older blob version on demand (git_ok asserts the exit status is 0)
    let text = String::from_utf8(git_ok(&clone_dir, &["log", "-p"])).expect("utf-8 log output");

    // then: both the tip and the historical file contents appear in the diffs
    assert!(text.contains("+alpha v2"), "tip content in log: {text}");
    assert!(
        text.contains("-alpha"),
        "historical content faulted in: {text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_materialize_an_earlier_commit_on_checkout() {
    // given: a partial clone whose root commit predates two later changes
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let first = rev_parse(&local, "HEAD");
    commit_file(&local, "a.txt", b"alpha v2\n", "modify a");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);
    let clone_dir = clone_repo_with(&server, &url, "clone", &["--filter=blob:none"]);

    // when: checking out the root commit faults in that commit's blob by oid
    git_ok(&clone_dir, &["checkout", "-q", &first]);

    // then: the earlier contents are restored byte-for-byte and the
    // later-added file is gone
    assert_eq!(
        std::fs::read(clone_dir.join("a.txt")).expect("read a.txt"),
        b"alpha\n"
    );
    assert!(
        !clone_dir.join("b.txt").exists(),
        "b.txt absent at the root commit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_clone_pushed_history_via_protocol_v0() {
    // given: two pushed commits
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a fresh clone forced onto the classic (v0) protocol
    let clone_dir = clone_repo_v0(&server, &url, "clone-v0");

    // then: the clone is object-complete, sits on the pushed tip, and both
    // files check out with the right contents
    assert_fsck_clean(&clone_dir);
    assert_eq!(rev_parse(&clone_dir, "HEAD"), tip);
    assert_eq!(
        std::fs::read(clone_dir.join("a.txt")).expect("read a.txt"),
        b"alpha\n"
    );
    assert_eq!(
        std::fs::read(clone_dir.join("b.txt")).expect("read b.txt"),
        b"beta\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_still_clone_pushed_history_via_protocol_v2() {
    // given: two pushed commits
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a fresh clone forced onto protocol v2 (still the default path)
    let clone_dir = server.tempdir().join("clone-v2");
    let clone = git(
        server.tempdir(),
        &[
            "-c",
            "protocol.version=2",
            "clone",
            &url,
            clone_dir.to_str().expect("utf-8 clone path"),
        ],
    );
    assert!(
        clone.status.success(),
        "v2 clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );

    // then: the v2 clone is unaffected by the classic path
    assert_fsck_clean(&clone_dir);
    assert_eq!(rev_parse(&clone_dir, "HEAD"), tip);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_stream_a_raw_pack_via_v0_when_side_band_is_not_negotiated() {
    // given: a pushed commit
    let server = TestServer::spawn(test_config()).await;
    let (local, _url) = push_fixture(&server, "proj");
    let tip = rev_parse(&local, "HEAD");

    // when: a classic fetch that wants the tip, negotiates no capabilities, and
    // is done
    let body = v0_fetch_body(&[&tip], "");
    let response = post_upload_pack_v0(&server.base_url(), "proj", body).await;

    // then: a NAK pkt-line, then the raw pack bytes directly (no side-band
    // framing) — a real, unpackable pack of the commit's closure
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("body");
    assert!(
        bytes.starts_with(b"0008NAK\n"),
        "expected NAK pkt: {:?}",
        &bytes[..8.min(bytes.len())]
    );
    let pack = &bytes[8..];
    assert!(
        pack.starts_with(b"PACK"),
        "raw pack follows NAK: {:?}",
        &pack[..4.min(pack.len())]
    );
    assert_eq!(pack_object_count(pack), 3);
    // commit + root tree + the one blob
    assert_eq!(unpack_oids(&server, pack, "unpack-raw-v0").len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_acknowledge_a_common_have_and_stream_an_incremental_pack_via_v0() {
    // given: two pushed commits, so the client can claim the parent
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let parent = rev_parse(&local, "HEAD");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a classic fetch wanting the tip, offering the parent, and done, with
    // multi_ack_detailed + side-band-64k negotiated
    let caps = "multi_ack_detailed side-band-64k";
    let mut body = pkt(format!("want {tip} {caps}\n").as_bytes());
    body.extend_from_slice(FLUSH);
    body.extend(pkt(format!("have {parent}\n").as_bytes()));
    body.extend(pkt(b"done\n"));
    let response = post_upload_pack_v0(&server.base_url(), "proj", body).await;

    // then: the done round opens with a plain ACK of the common, then the
    // side-band pack — which holds only the incremental objects (the new
    // commit, its root tree, and the new blob), not a full re-clone
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("body");
    let pkts = parse_pkts(&bytes);
    assert_eq!(pkts[0], Pkt::Data(format!("ACK {parent}\n").into_bytes()));
    let pack = extract_sideband_pack(&pkts);
    assert_eq!(pack_object_count(&pack), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_acknowledge_common_and_signal_ready_in_a_v0_negotiation_round() {
    // given: two pushed commits
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let parent = rev_parse(&local, "HEAD");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a negotiation round (no done) that wants the tip and offers the
    // parent as a have
    let caps = "multi_ack_detailed side-band-64k";
    let mut body = pkt(format!("want {tip} {caps}\n").as_bytes());
    body.extend_from_slice(FLUSH);
    body.extend(pkt(format!("have {parent}\n").as_bytes()));
    body.extend_from_slice(FLUSH);
    let response = post_upload_pack_v0(&server.base_url(), "proj", body).await;

    // then: the parent is acknowledged common, ready is signalled (it bounds
    // the wanted tip), the round ends with NAK, and no pack is sent
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("body");
    let pkts = parse_pkts(&bytes);
    assert_eq!(
        pkts,
        vec![
            Pkt::Data(format!("ACK {parent} common\n").into_bytes()),
            Pkt::Data(format!("ACK {parent} ready\n").into_bytes()),
            Pkt::Data(b"NAK\n".to_vec()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_nak_a_v0_negotiation_round_with_no_common_have() {
    // given: a pushed commit and an unrelated have the server has never seen
    let server = TestServer::spawn(test_config()).await;
    let (local, _url) = push_fixture(&server, "proj");
    let tip = rev_parse(&local, "HEAD");
    let stranger = "f".repeat(40);

    // when: a negotiation round offering only the unknown have
    let caps = "multi_ack_detailed side-band-64k";
    let mut body = pkt(format!("want {tip} {caps}\n").as_bytes());
    body.extend_from_slice(FLUSH);
    body.extend(pkt(format!("have {stranger}\n").as_bytes()));
    body.extend_from_slice(FLUSH);
    let response = post_upload_pack_v0(&server.base_url(), "proj", body).await;

    // then: the unknown have is silently dropped, so nothing is common and the
    // round is a lone NAK
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.expect("body");
    assert_eq!(parse_pkts(&bytes), vec![Pkt::Data(b"NAK\n".to_vec())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_fetch_new_commits_incrementally_via_protocol_v0() {
    // given: a v0 clone taken before two further commits were pushed
    let server = TestServer::spawn(test_config()).await;
    let (local, url) = push_fixture(&server, "proj");
    let clone_dir = clone_repo_v0(&server, &url, "clone-v0");
    commit_file(&local, "b.txt", b"beta\n", "add b");
    let tip = commit_file(&local, "c.txt", b"gamma\n", "add c");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: the existing clone fetches over the classic protocol (a real
    // negotiation: git sends a no-done round, sees ready, then a done round),
    // then fast-forwards
    git_ok(&clone_dir, &["-c", "protocol.version=0", "fetch", "origin"]);
    assert_eq!(rev_parse(&clone_dir, "origin/main"), tip);
    let mut pull = IDENTITY.to_vec();
    pull.extend_from_slice(&["-c", "protocol.version=0", "pull", "--ff-only"]);
    git_ok(&clone_dir, &pull);

    // then: the clone sits on the new tip with the new file present, intact
    assert_eq!(rev_parse(&clone_dir, "HEAD"), tip);
    assert_eq!(
        std::fs::read(clone_dir.join("c.txt")).expect("read c.txt"),
        b"gamma\n"
    );
    assert_fsck_clean(&clone_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_serve_a_clonable_repo_at_compression_level_zero() {
    // given: a server that deflates objects at level 0 (stored blocks, no
    // compression), with both an inline file and an offloaded blob pushed
    let mut config = test_config();
    config.object_compression_level = 0;
    let server = TestServer::spawn(config).await;
    let (local, url) = push_fixture(&server, "proj");
    let big: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    commit_file(&local, "big.bin", &big, "add big blob");
    let tip = commit_file(&local, "b.txt", b"beta\n", "add b");
    git_ok(&local, &["push", &url, "main:refs/heads/main"]);

    // when: a fresh clone reads back the level-0 streams verbatim
    let clone_dir = clone_repo(&server, &url, "clone");

    // then: the clone is object-complete, on the pushed tip, with both the
    // inline and the offloaded blob intact — proving the level is plumbed
    // end to end and inflate is level-agnostic
    assert_fsck_clean(&clone_dir);
    assert_eq!(rev_parse(&clone_dir, "HEAD"), tip);
    assert_eq!(
        std::fs::read(clone_dir.join("a.txt")).expect("read a.txt"),
        b"alpha\n"
    );
    assert_eq!(
        std::fs::read(clone_dir.join("big.bin")).expect("read big.bin"),
        big
    );
}
