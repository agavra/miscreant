mod common;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use common::{commit_file, git_ok_with_input, init_repo, pack_revs, rev_objects};
use gix_hash::ObjectId;
use gix_object::Kind;
use gix_pack::bundle::write::Error as PackWriteError;
use gix_pack::data::input::Error as PackInputError;
use gix_pack::index::write::Error as IndexWriteError;
use miscreant::git::{IngestError, StagedPack, ingest_pack};
use miscreant::storage::{BlobStore, ObjectDb, RepoMeta, Store};
use slatedb::object_store::{self, ObjectStore};
use tempfile::TempDir;

/// A fresh in-memory ObjectDb plus its registered repository.
async fn memory_objectdb(repo_name: &str) -> (ObjectDb, RepoMeta) {
    let store = Store::open("memory://").await.expect("open store");
    let meta = store.create_repo(repo_name).await.expect("create repo");
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    (ObjectDb::new(store, BlobStore::new(backing), 65536), meta)
}

/// A scratch area holding a fixture repository and a staging root.
struct Fixture {
    _scratch: TempDir,
    repo_dir: PathBuf,
    staging: PathBuf,
}

fn fixture() -> Fixture {
    let scratch = TempDir::new().expect("create scratch dir");
    let repo_dir = scratch.path().join("repo");
    let staging = scratch.path().join("staging");
    init_repo(&repo_dir);
    Fixture {
        _scratch: scratch,
        repo_dir,
        staging,
    }
}

async fn ingest(
    pack: Vec<u8>,
    db: &ObjectDb,
    meta: &RepoMeta,
    staging: &Path,
) -> Result<StagedPack, IngestError> {
    ingest_pack(Cursor::new(pack), db, meta, staging).await
}

/// The entries left under the staging root (tempdirs of live StagedPacks).
fn staging_entries(staging: &Path) -> usize {
    match std::fs::read_dir(staging) {
        Ok(entries) => entries.count(),
        Err(_) => 0,
    }
}

/// A large, compressible body whose one-line edits delta well.
fn repetitive_body(lines: usize) -> Vec<u8> {
    let mut body = Vec::new();
    for i in 0..lines {
        body.extend_from_slice(format!("line {i:04} of some repetitive content\n").as_bytes());
    }
    body
}

fn absent_oid() -> ObjectId {
    ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").expect("valid hex")
}

#[tokio::test(flavor = "multi_thread")]
async fn should_stage_all_objects_of_a_full_pack() {
    // given: a repository with two commits and a full pack of HEAD's closure
    let fx = fixture();
    commit_file(&fx.repo_dir, "a.txt", b"alpha\n", "add a");
    let head = commit_file(&fx.repo_dir, "b/nested.txt", b"beta\n", "add b");
    let pack = pack_revs(&fx.repo_dir, &[], &[&head]);
    let expected = rev_objects(&fx.repo_dir, &[&head]);
    let (db, meta) = memory_objectdb("ingest/full").await;

    // when
    let staged = ingest(pack, &db, &meta, &fx.staging)
        .await
        .expect("ingest full pack");

    // then: every object is present with the right kind and body
    assert_eq!(staged.object_count() as usize, expected.len());
    for (oid, kind, body) in &expected {
        assert!(staged.contains(oid), "missing {oid}");
        assert_eq!(staged.info(oid).expect("info"), Some(*kind));
        let (read_kind, read_body) = staged.read(oid).expect("read").expect("object present");
        assert_eq!(read_kind, *kind);
        assert_eq!(read_body, Bytes::from(body.clone()));
    }

    // then: iteration lists exactly the expected (oid, kind) pairs
    let listed: BTreeMap<ObjectId, Kind> = staged
        .iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("iterate staged pack")
        .into_iter()
        .collect();
    let wanted: BTreeMap<ObjectId, Kind> = expected
        .iter()
        .map(|(oid, kind, _)| (*oid, *kind))
        .collect();
    assert_eq!(listed, wanted);

    // then: an unknown oid is reported absent everywhere
    let absent = absent_oid();
    assert!(!staged.contains(&absent));
    assert_eq!(staged.info(&absent).expect("info"), None);
    assert!(staged.read(&absent).expect("read").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn should_resolve_thin_pack_bases_from_committed_objects() {
    // given: a child commit whose blob deltas against the excluded base
    // blob, packed thin, with the base closure already in the ObjectDb
    let fx = fixture();
    let base_body = repetitive_body(200);
    let base = commit_file(&fx.repo_dir, "data.txt", &base_body, "base");
    let mut child_body = base_body.clone();
    child_body.extend_from_slice(b"one more line\n");
    let child = commit_file(&fx.repo_dir, "data.txt", &child_body, "child");
    let exclude_base = format!("^{base}");
    let pack = pack_revs(&fx.repo_dir, &["--thin"], &[&child, &exclude_base]);

    let (db, meta) = memory_objectdb("ingest/thin").await;
    for (oid, kind, body) in rev_objects(&fx.repo_dir, &[&base]) {
        db.put(meta.id, &oid, kind, Bytes::from(body))
            .await
            .expect("seed base object");
    }

    // when
    let staged = ingest(pack, &db, &meta, &fx.staging)
        .await
        .expect("ingest thin pack");

    // then: the new objects are staged and their bodies decode through the
    // resolved base
    for (oid, kind, body) in rev_objects(&fx.repo_dir, &[&child, &exclude_base]) {
        assert!(staged.contains(&oid), "missing {oid}");
        assert_eq!(staged.info(&oid).expect("info"), Some(kind));
        let (read_kind, read_body) = staged.read(&oid).expect("read").expect("object present");
        assert_eq!(read_kind, kind);
        assert_eq!(read_body, Bytes::from(body));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_thin_pack_when_base_objects_are_unknown() {
    // given: the same thin pack, but nothing seeded into the ObjectDb
    let fx = fixture();
    let base_body = repetitive_body(200);
    let base = commit_file(&fx.repo_dir, "data.txt", &base_body, "base");
    let mut child_body = base_body.clone();
    child_body.extend_from_slice(b"one more line\n");
    let child = commit_file(&fx.repo_dir, "data.txt", &child_body, "child");
    let exclude_base = format!("^{base}");
    let pack = pack_revs(&fx.repo_dir, &["--thin"], &[&child, &exclude_base]);
    let (db, meta) = memory_objectdb("ingest/thin-missing").await;

    // when
    let result = ingest(pack, &db, &meta, &fx.staging).await;

    // then: the unresolved base fails the ingest (proving the pack really
    // was thin) and no staging directory is left behind
    assert!(matches!(
        result,
        Err(IngestError::Pack(
            PackWriteError::PackIter(PackInputError::NotFound { .. })
                | PackWriteError::IndexWrite(IndexWriteError::PackEntryDecode(
                    PackInputError::NotFound { .. }
                ))
        ))
    ));
    assert_eq!(staging_entries(&fx.staging), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_pack_with_corrupted_byte_and_clean_up_staging() {
    // given: a valid pack with one byte flipped between header and trailer
    let fx = fixture();
    let head = commit_file(&fx.repo_dir, "a.txt", b"alpha\n", "add a");
    let mut pack = pack_revs(&fx.repo_dir, &[], &[&head]);
    assert!(pack.len() > 32, "pack too small to corrupt mid-stream");
    let middle = pack.len() / 2;
    pack[middle] ^= 0xff;
    let (db, meta) = memory_objectdb("ingest/corrupt").await;

    // when
    let result = ingest(pack, &db, &meta, &fx.staging).await;

    // then: the pack is rejected and the staging tempdir is gone
    assert!(result.is_err());
    assert_eq!(staging_entries(&fx.staging), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_pack_with_mismatched_trailer_sha() {
    // given: a valid pack whose trailing checksum is tampered with
    let fx = fixture();
    let head = commit_file(&fx.repo_dir, "a.txt", b"alpha\n", "add a");
    let mut pack = pack_revs(&fx.repo_dir, &[], &[&head]);
    let last = pack.len() - 1;
    pack[last] ^= 0xff;
    let (db, meta) = memory_objectdb("ingest/sha-mismatch").await;

    // when
    let result = ingest(pack, &db, &meta, &fx.staging).await;

    // then
    assert!(matches!(result, Err(IngestError::Pack(_))));
    assert_eq!(staging_entries(&fx.staging), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_accept_a_pack_with_zero_objects() {
    // given: a genuine empty pack, as pack-objects emits for no input
    let fx = fixture();
    let pack = git_ok_with_input(&fx.repo_dir, &["pack-objects", "-q", "--stdout"], b"");
    let (db, meta) = memory_objectdb("ingest/empty-pack").await;

    // when
    let staged = ingest(pack, &db, &meta, &fx.staging)
        .await
        .expect("ingest empty pack");

    // then
    assert_eq!(staged.object_count(), 0);
    assert!(staged.iter().next().is_none());
    assert!(!staged.contains(&absent_oid()));
}

#[tokio::test(flavor = "multi_thread")]
async fn should_accept_absent_pack_bytes() {
    // given: no pack bytes at all, as sent by a delete-only push
    let fx = fixture();
    let (db, meta) = memory_objectdb("ingest/absent-pack").await;

    // when
    let staged = ingest(Vec::new(), &db, &meta, &fx.staging)
        .await
        .expect("ingest absent pack");

    // then
    assert_eq!(staged.object_count(), 0);
    assert!(staged.read(&absent_oid()).expect("read").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn should_remove_staging_directory_when_staged_pack_is_dropped() {
    // given: a successfully staged pack occupying the staging root
    let fx = fixture();
    let head = commit_file(&fx.repo_dir, "a.txt", b"alpha\n", "add a");
    let pack = pack_revs(&fx.repo_dir, &[], &[&head]);
    let (db, meta) = memory_objectdb("ingest/drop").await;
    let staged = ingest(pack, &db, &meta, &fx.staging)
        .await
        .expect("ingest full pack");
    assert_eq!(staging_entries(&fx.staging), 1);

    // when
    drop(staged);

    // then
    assert_eq!(staging_entries(&fx.staging), 0);
}
