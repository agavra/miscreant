mod common;

use std::collections::HashSet;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use common::{commit_file, git_ok, init_repo, pack_objects, pack_revs, rev_objects, rev_parse};
use gix_hash::ObjectId;
use gix_object::Kind;
use miscreant::git::{PromoteError, ingest_pack, validate_and_promote};
use miscreant::storage::values::ObjectRecord;
use miscreant::storage::{BlobStore, ObjectDb, RepoMeta, Store};
use slatedb::object_store::{self, ObjectStore};
use tempfile::TempDir;

/// A fresh in-memory store, its object database, and one registered
/// repository. The store handle is kept so tests can assert on persisted
/// records directly.
async fn memory_backend(repo_name: &str) -> (Store, ObjectDb, RepoMeta) {
    let store = Store::open("memory://").await.expect("open store");
    let meta = store.create_repo(repo_name).await.expect("create repo");
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let objectdb = ObjectDb::new(store.clone(), BlobStore::new(backing), 65536);
    (store, objectdb, meta)
}

/// An `ObjectDb` façade over an already-open `Store`, backed by its own
/// fresh in-memory blob store (no fixture in these tests offloads a blob,
/// so blob-store identity across a reopen is irrelevant).
fn object_db_over(store: &Store) -> ObjectDb {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    ObjectDb::new(store.clone(), BlobStore::new(backing), 65536)
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

/// Configure a fixture repo's committer identity for operations that mint
/// commits outside [`commit_file`] (merges, annotated tags).
fn git_authored(dir: &Path, args: &[&str]) -> Vec<u8> {
    let mut full = vec![
        "-c",
        "user.name=miscreant",
        "-c",
        "user.email=miscreant@example.com",
    ];
    full.extend_from_slice(args);
    git_ok(dir, &full)
}

fn oid_of(hex: &str) -> ObjectId {
    ObjectId::from_hex(hex.as_bytes()).expect("valid hex")
}

fn as_set(ids: &[ObjectId]) -> HashSet<ObjectId> {
    ids.iter().copied().collect()
}

fn closure_set(objects: &[(ObjectId, Kind, Vec<u8>)]) -> HashSet<ObjectId> {
    objects.iter().map(|(oid, _, _)| *oid).collect()
}

/// The stored generation number of a commit; panics if it has no record.
async fn generation(store: &Store, meta: &RepoMeta, hex: &str) -> u32 {
    store
        .get_commit_graph(meta.id, &oid_of(hex))
        .await
        .expect("read commit graph")
        .expect("commit graph record present")
        .generation
}

#[tokio::test(flavor = "multi_thread")]
async fn should_promote_the_full_closure_then_nothing_on_a_repush() {
    // given: a repository with two commits and a full pack of HEAD's closure
    let fx = fixture();
    commit_file(&fx.repo_dir, "a.txt", b"alpha\n", "add a");
    let head = commit_file(&fx.repo_dir, "b/nested.txt", b"beta\n", "add b");
    let pack = pack_revs(&fx.repo_dir, &[], &[&head]);
    let expected = rev_objects(&fx.repo_dir, &[&head]);
    let (store, db, meta) = memory_backend("promote/full").await;
    let tips = vec![oid_of(&head)];

    // when
    let staged = ingest_pack(Cursor::new(pack.clone()), &db, &meta, &fx.staging)
        .await
        .expect("ingest pack");
    let promotion = validate_and_promote(&staged, &tips, &db, &store, meta.id)
        .await
        .expect("promote pack");

    // then: exactly the closure is promoted and readable through the db
    assert_eq!(as_set(&promotion.promoted), closure_set(&expected));
    assert_eq!(promotion.promoted.len(), expected.len());
    for (oid, kind, body) in &expected {
        let (read_kind, read_body) = db.get(meta.id, oid).await.expect("get").expect("present");
        assert_eq!(read_kind, *kind);
        assert_eq!(read_body, Bytes::from(body.clone()));
    }

    // when: the identical pack is ingested and promoted a second time
    let staged_again = ingest_pack(Cursor::new(pack), &db, &meta, &fx.staging)
        .await
        .expect("ingest pack again");
    let repromotion = validate_and_promote(&staged_again, &tips, &db, &store, meta.id)
        .await
        .expect("promote pack again");

    // then: the whole closure is already committed, so nothing is promoted
    assert!(repromotion.promoted.is_empty());
    assert!(repromotion.commits.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn should_reject_a_pack_missing_a_referenced_blob_and_write_nothing() {
    // given: a pack of just the commit and its root tree, deliberately
    // omitting the single blob the tree references
    let fx = fixture();
    let head = commit_file(&fx.repo_dir, "f.txt", b"hello\n", "add f");
    let tree = rev_parse(&fx.repo_dir, &format!("{head}^{{tree}}"));
    let pack = pack_objects(&fx.repo_dir, &[&head, &tree]);
    let (store, db, meta) = memory_backend("promote/missing-blob").await;

    // when
    let staged = ingest_pack(Cursor::new(pack), &db, &meta, &fx.staging)
        .await
        .expect("ingest partial pack");
    let result = validate_and_promote(&staged, &[oid_of(&head)], &db, &store, meta.id).await;

    // then: rejected with a connectivity error naming the missing blob and
    // the tree that referenced it
    let missing = match result {
        Err(PromoteError::Connectivity {
            missing,
            referenced_by,
        }) => {
            assert_eq!(referenced_by, Some(oid_of(&tree)));
            missing
        }
        other => panic!("expected a connectivity error, got {other:?}"),
    };

    // then: nothing at all reached committed storage
    assert!(!db.exists(meta.id, &oid_of(&head)).await.expect("exists"));
    assert!(!db.exists(meta.id, &oid_of(&tree)).await.expect("exists"));
    assert!(!db.exists(meta.id, &missing).await.expect("exists"));
    assert_eq!(
        store
            .get_commit_graph(meta.id, &oid_of(&head))
            .await
            .expect("graph"),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_prune_at_the_shared_boundary_on_an_incremental_push() {
    // given: an initial commit already promoted
    let fx = fixture();
    let c1 = commit_file(&fx.repo_dir, "f.txt", b"one\n", "one");
    let (store, db, meta) = memory_backend("promote/incremental").await;
    let staged_c1 = ingest_pack(
        Cursor::new(pack_revs(&fx.repo_dir, &[], &[&c1])),
        &db,
        &meta,
        &fx.staging,
    )
    .await
    .expect("ingest c1");
    validate_and_promote(&staged_c1, &[oid_of(&c1)], &db, &store, meta.id)
        .await
        .expect("promote c1");

    // given: a child commit that rewrites the only file, packed without c1's
    // closure
    let c2 = commit_file(&fx.repo_dir, "f.txt", b"two\n", "two");
    let exclude_c1 = format!("^{c1}");
    let pack_c2 = pack_revs(&fx.repo_dir, &[], &[&c2, &exclude_c1]);
    let new_objects = rev_objects(&fx.repo_dir, &[&c2, &exclude_c1]);

    // when
    let staged_c2 = ingest_pack(Cursor::new(pack_c2), &db, &meta, &fx.staging)
        .await
        .expect("ingest c2");
    let promotion = validate_and_promote(&staged_c2, &[oid_of(&c2)], &db, &store, meta.id)
        .await
        .expect("promote c2");

    // then: only the objects new to c2 are promoted; c1's shared closure is
    // pruned at the boundary
    assert_eq!(as_set(&promotion.promoted), closure_set(&new_objects));
    assert_eq!(promotion.commits, vec![oid_of(&c2)]);
    // then: c2's generation follows the already-recorded c1
    assert_eq!(generation(&store, &meta, &c2).await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_number_generations_along_a_linear_chain() {
    // given: a three-commit linear history
    let fx = fixture();
    let c1 = commit_file(&fx.repo_dir, "f.txt", b"1\n", "c1");
    let c2 = commit_file(&fx.repo_dir, "f.txt", b"2\n", "c2");
    let c3 = commit_file(&fx.repo_dir, "f.txt", b"3\n", "c3");
    let (store, db, meta) = memory_backend("promote/linear").await;

    // when
    let staged = ingest_pack(
        Cursor::new(pack_revs(&fx.repo_dir, &[], &[&c3])),
        &db,
        &meta,
        &fx.staging,
    )
    .await
    .expect("ingest chain");
    validate_and_promote(&staged, &[oid_of(&c3)], &db, &store, meta.id)
        .await
        .expect("promote chain");

    // then: generations increase 1, 2, 3 from root to tip
    assert_eq!(generation(&store, &meta, &c1).await, 1);
    assert_eq!(generation(&store, &meta, &c2).await, 2);
    assert_eq!(generation(&store, &meta, &c3).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_set_a_merge_generation_to_one_more_than_its_max_parent() {
    // given: two branches that diverged from a shared root and were merged
    let fx = fixture();
    let c1 = commit_file(&fx.repo_dir, "base.txt", b"base\n", "c1");
    git_ok(&fx.repo_dir, &["checkout", "-q", "-b", "side"]);
    let s1 = commit_file(&fx.repo_dir, "side.txt", b"side\n", "s1");
    git_ok(&fx.repo_dir, &["checkout", "-q", "main"]);
    let m1 = commit_file(&fx.repo_dir, "main.txt", b"main\n", "m1");
    git_authored(
        &fx.repo_dir,
        &["merge", "-q", "--no-ff", "-m", "merge side", "side"],
    );
    let merge = rev_parse(&fx.repo_dir, "HEAD");
    let (store, db, meta) = memory_backend("promote/merge").await;

    // when: packed with ofs-delta (as real push clients do — we advertise
    // it) so in-pack deltas resolve during ingest
    let staged = ingest_pack(
        Cursor::new(pack_revs(&fx.repo_dir, &["--delta-base-offset"], &[&merge])),
        &db,
        &meta,
        &fx.staging,
    )
    .await
    .expect("ingest merge");
    validate_and_promote(&staged, &[oid_of(&merge)], &db, &store, meta.id)
        .await
        .expect("promote merge");

    // then: root=1, each branch tip=2, and the merge is 1 + max(2, 2) = 3
    assert_eq!(generation(&store, &meta, &c1).await, 1);
    assert_eq!(generation(&store, &meta, &s1).await, 2);
    assert_eq!(generation(&store, &meta, &m1).await, 2);
    assert_eq!(generation(&store, &meta, &merge).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_promote_an_annotated_tag_after_its_new_target_commit() {
    // given: a commit and an annotated tag pointing at it, both new, packed
    // as exactly {tag, commit, tree, blob}
    let fx = fixture();
    let head = commit_file(&fx.repo_dir, "f.txt", b"tagged\n", "commit");
    git_authored(
        &fx.repo_dir,
        &["tag", "-a", "v1", "-m", "release v1", &head],
    );
    let tag = rev_parse(&fx.repo_dir, "v1");
    let objects = rev_objects(&fx.repo_dir, &[&head]);
    let commit_id = objects
        .iter()
        .find(|(_, kind, _)| *kind == Kind::Commit)
        .expect("one commit")
        .0;
    let member = |kind: Kind| {
        objects
            .iter()
            .find(|(_, k, _)| *k == kind)
            .expect("member present")
            .0
            .to_string()
    };
    let pack = pack_objects(
        &fx.repo_dir,
        &[
            &tag,
            &member(Kind::Commit),
            &member(Kind::Tree),
            &member(Kind::Blob),
        ],
    );
    let (store, db, meta) = memory_backend("promote/tag").await;

    // when
    let staged = ingest_pack(Cursor::new(pack), &db, &meta, &fx.staging)
        .await
        .expect("ingest tag pack");
    let promotion = validate_and_promote(&staged, &[oid_of(&tag)], &db, &store, meta.id)
        .await
        .expect("promote tag pack");

    // then: the tag is promoted only after its target commit, keeping the
    // closure invariant intact should promotion be interrupted
    let tag_oid = oid_of(&tag);
    let position = |target: &ObjectId| {
        promotion
            .promoted
            .iter()
            .position(|oid| oid == target)
            .expect("object was promoted")
    };
    assert!(position(&commit_id) < position(&tag_oid));
    assert!(db.exists(meta.id, &tag_oid).await.expect("exists"));
    // then: the target commit still got its generation-1 record
    assert_eq!(generation(&store, &meta, &member(Kind::Commit)).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_offload_a_blob_larger_than_the_inline_threshold() {
    // given: a commit whose file exceeds the 64KiB inline threshold
    let fx = fixture();
    let big = vec![b'x'; 70_000];
    let head = commit_file(&fx.repo_dir, "big.bin", &big, "add big");
    let objects = rev_objects(&fx.repo_dir, &[&head]);
    let blob_oid = objects
        .iter()
        .find(|(_, kind, _)| *kind == Kind::Blob)
        .expect("one blob")
        .0;
    let (store, db, meta) = memory_backend("promote/large-blob").await;

    // when
    let staged = ingest_pack(
        Cursor::new(pack_revs(&fx.repo_dir, &[], &[&head])),
        &db,
        &meta,
        &fx.staging,
    )
    .await
    .expect("ingest large blob");
    validate_and_promote(&staged, &[oid_of(&head)], &db, &store, meta.id)
        .await
        .expect("promote large blob");

    // then: the stored record is a size-only pointer (content offloaded to
    // the blob store), never an inline copy
    assert_eq!(
        store
            .get_object(meta.id, &blob_oid)
            .await
            .expect("object record"),
        Some(ObjectRecord::BlobPointer { size: 70_000 })
    );
    // then: the content still round-trips transparently through the db
    let (kind, body) = db
        .get(meta.id, &blob_oid)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(kind, Kind::Blob);
    assert_eq!(body, Bytes::from(big));
}

#[tokio::test(flavor = "multi_thread")]
async fn should_read_promoted_objects_and_graph_records_after_reopening_the_store() {
    // given: a fixture commit and its pack, promoted into a fresh
    // file://-backed store (relaxed writes only become durable through
    // promotion's flush barrier, so this exercises that barrier for real —
    // memory:// has no persistence to lose in the first place)
    let fx = fixture();
    let head = commit_file(&fx.repo_dir, "f.txt", b"durable\n", "add f");
    let pack = pack_revs(&fx.repo_dir, &[], &[&head]);
    let expected = rev_objects(&fx.repo_dir, &[&head]);

    let store_dir = TempDir::new().expect("create store dir");
    let store_url = format!("file://{}", store_dir.path().display());
    let store = Store::open(&store_url).await.expect("open store");
    let meta = store.create_repo("promote/reopen").await.expect("create");
    let db = object_db_over(&store);

    // when: the pack is promoted and the store is cleanly closed
    let staged = ingest_pack(Cursor::new(pack), &db, &meta, &fx.staging)
        .await
        .expect("ingest pack");
    let promotion = validate_and_promote(&staged, &[oid_of(&head)], &db, &store, meta.id)
        .await
        .expect("promote pack");
    store.close().await.expect("close store");

    // when: the store is reopened from the same location with fresh handles
    let reopened = Store::open(&store_url).await.expect("reopen store");
    let reopened_db = object_db_over(&reopened);

    // then: every promoted object is readable through the reopened handles
    for (oid, kind, body) in &expected {
        let (read_kind, read_body) = reopened_db
            .get(meta.id, oid)
            .await
            .expect("get")
            .expect("present after reopen");
        assert_eq!(read_kind, *kind);
        assert_eq!(read_body, Bytes::from(body.clone()));
    }

    // then: the commit-graph record promotion wrote also survived the reopen
    assert_eq!(promotion.commits, vec![oid_of(&head)]);
    assert_eq!(
        reopened
            .get_commit_graph(meta.id, &oid_of(&head))
            .await
            .expect("get graph")
            .expect("present after reopen")
            .generation,
        1
    );

    // A backfilled commit-graph record is derived data written with no
    // flush barrier (see `backfill_commit_info`): recomputing it after a
    // wipe, rather than surviving a reopen, is what makes it safe to lose.
    // `should_backfill_a_missing_middle_graph_record` in walk.rs already
    // covers that recomputation; there is nothing durability-specific left
    // to assert about backfilled records here.
}
