//! SlateDB-backed persistence: the repository registry, typed per-segment
//! operations, and compare-and-swap ref updates.
//!
//! See `docs/0001-init.md` §Storage (intro), §Key Preamble (per-request
//! resolution order), and §Metadata Storage. All keys are built through the
//! `keys` codec and all values through the `values` codec; this module only
//! layers SlateDB access and the repo lifecycle on top of them.

use std::sync::Arc;

use gix_hash::{Kind, ObjectId, oid};
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::object_store::path::Path;
use slatedb::object_store::prefix::PrefixStore;
use slatedb::object_store::{self, ObjectStore};
use slatedb::{Db, ErrorKind, IsolationLevel, PrefixExtractor, PrefixTarget};
use url::Url;

use crate::storage::keys::{self, KeyError, RepoId, Segment};
use crate::storage::values::{CommitGraphRecord, MetaValue, ObjectRecord, RefTarget, ValueError};

/// Child of the root object store that holds the SlateDB instance. Offloaded
/// blobs (added in a later change) live under a sibling prefix of the same
/// root store, so a single bucket hosts both.
const SLATEDB_PREFIX: &str = "slatedb";

/// Routes every key to one of the four fixed segments by its leading segment
/// byte (see `docs/0001-init.md` §Key Preamble), enabling SlateDB's segmented
/// compaction with exactly four segments shared by all repositories.
///
/// The extractor is fixed for the life of a database: SlateDB persists
/// `name()` in the manifest and refuses to open when it changes, so any
/// future change to segmentation requires a new store and a migration.
struct SegmentExtractor;

impl PrefixExtractor for SegmentExtractor {
    fn name(&self) -> &str {
        "miscreant-segment-v1"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        // Fixed one-byte extraction is safe for point keys and for every
        // scan prefix of at least one byte, which all `keys` builders
        // guarantee.
        let bytes = match target {
            PrefixTarget::Point(bytes) | PrefixTarget::Prefix(bytes) => bytes,
        };
        (!bytes.is_empty()).then_some(1)
    }
}

/// Prefix of a server-global `repo:<name>` mapping key.
const META_REPO_PREFIX: &str = "repo:";
/// Server-global allocation counter for repository ids.
const META_NEXT_REPO_ID: &str = "next-repo-id";
/// Per-repo SHA width (`sha1`/`sha256`).
const META_OBJECT_FORMAT: &str = "object-format";
/// Per-repo symref target advertised for `HEAD`.
const META_DEFAULT_BRANCH: &str = "default-branch";
/// Per-repo KV layout version.
const META_SCHEMA_VERSION: &str = "schema-version";

/// The only object format the server creates (see the design's SHA-1
/// end-to-end decision).
const OBJECT_FORMAT_SHA1: &str = "sha1";
/// Default branch written into every freshly created repository.
const DEFAULT_BRANCH: &str = "refs/heads/main";
/// The symbolic ref name every repository is created with.
const HEAD_REF: &str = "HEAD";
/// KV layout version stamped on new repositories.
const SCHEMA_VERSION: u32 = 1;
/// First repository id handed out; `0` is reserved for global metadata.
const FIRST_REPO_ID: u64 = 1;

/// Attempts (initial try + retries) for the repo-creation transaction. A
/// loser of a create race resolves on its next attempt by reading the
/// winner's committed mapping, so contention is bounded by the number of
/// concurrent creators; the cap only guards against pathological livelock.
const CREATE_ATTEMPTS: usize = 32;
/// Attempts for the ref-CAS transaction: one try plus a single retry, after
/// which every command is failed as `ng` (design: non-atomic ref updates).
const UPDATE_REFS_ATTEMPTS: usize = 2;

/// Rejection reason when a ref's current value does not match `expected_old`.
const REASON_STALE: &str = "stale info";
/// Rejection reason when a command targets a symbolic ref (e.g. `HEAD`).
const REASON_SYMBOLIC: &str = "cannot update symbolic ref";
/// Rejection reason when the CAS transaction could not commit.
const REASON_CONFLICT: &str = "transaction conflict";

/// Errors surfaced by the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The configured storage URL could not be parsed.
    #[error("invalid storage url: {0}")]
    Url(#[from] url::ParseError),
    /// An I/O error while resolving the storage location (e.g. reading the
    /// current directory to make a relative `file` URL absolute).
    #[error("io error resolving storage location: {0}")]
    Io(#[from] std::io::Error),
    /// The object store could not be resolved from the URL, or an object
    /// store operation failed.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// An error propagated from SlateDB.
    #[error("slatedb error: {0}")]
    Db(#[from] slatedb::Error),
    /// A stored key could not be decoded (indicates corruption).
    #[error("malformed key in store: {0}")]
    Key(#[from] KeyError),
    /// A stored value could not be decoded (indicates corruption).
    #[error("malformed value in store: {0}")]
    Value(#[from] ValueError),
    /// A metadata entry existed but held an unexpected encoding.
    #[error("metadata {key:?} for repo {repo:?} has an unexpected encoding")]
    UnexpectedMeta {
        /// Repository the metadata belongs to.
        repo: RepoId,
        /// Metadata key name.
        key: String,
    },
    /// A repository resolved to an id but a required metadata key was absent.
    #[error("repo {repo:?} is missing required metadata {key:?}")]
    MissingMeta {
        /// Repository the metadata belongs to.
        repo: RepoId,
        /// Metadata key name.
        key: String,
    },
    /// The repository's `object-format` names an unsupported SHA width.
    #[error("unsupported object format: {0:?}")]
    UnsupportedObjectFormat(String),
    /// The repo-creation transaction could not commit within its retry
    /// budget.
    #[error("repository creation exceeded its retry budget")]
    CreateContention,
}

/// Resolved metadata for a single repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoMeta {
    /// The repository's opaque id.
    pub id: RepoId,
    /// SHA width used by this repository's object/ref/commit-graph segments.
    pub object_format: Kind,
    /// The symref target advertised for `HEAD` (e.g. `refs/heads/main`).
    pub default_branch: String,
}

/// One requested ref mutation for [`Store::update_refs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdate {
    /// Full ref name (e.g. `refs/heads/main`).
    pub name: String,
    /// Value the ref must currently hold, or `None` if it must not yet exist.
    pub expected_old: Option<ObjectId>,
    /// New value to set, or `None` to delete the ref.
    pub new: Option<ObjectId>,
}

/// Per-command outcome from [`Store::update_refs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdateResult {
    /// The ref this result is for.
    pub name: String,
    /// Whether the update was applied or rejected.
    pub outcome: RefOutcome,
}

/// Whether a single ref update succeeded or was rejected (and why).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefOutcome {
    /// The ref was updated (or deleted) as requested.
    Updated,
    /// The ref was left unchanged; the string is a git-style `ng` reason.
    Rejected(String),
}

/// Durability requested for a single SlateDB write. `Durable` blocks the
/// write until SlateDB's WAL flush task has persisted it (the default for
/// every write except promotion and commit-graph backfill); `Relaxed`
/// returns as soon as the write lands in the in-memory WAL buffer, leaving
/// durability to a later [`Store::flush`] call or another durable write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Durability {
    Durable,
    Relaxed,
}

impl Durability {
    fn write_options(self) -> WriteOptions {
        WriteOptions {
            await_durable: matches!(self, Durability::Durable),
            ..WriteOptions::default()
        }
    }
}

/// Typed persistence over SlateDB plus the repository registry. Cheaply
/// cloneable (shared handles behind `Arc`).
#[derive(Clone)]
pub struct Store {
    db: Arc<Db>,
    root_store: Arc<dyn ObjectStore>,
}

impl Store {
    /// Open (or create) the store backing `storage_url`. SlateDB is placed
    /// under the `slatedb/` prefix of the resolved object store; the root
    /// store handle is retained for later blob offload.
    pub async fn open(storage_url: &str) -> Result<Self, StoreError> {
        let root_store = resolve_root_store(storage_url)?;
        let db = Db::builder(Path::from(SLATEDB_PREFIX), root_store.clone())
            .with_segment_extractor(Arc::new(SegmentExtractor))
            .build()
            .await?;
        Ok(Self {
            db: Arc::new(db),
            root_store,
        })
    }

    /// The root object store (already offset by the URL's path component).
    /// Blob offload builds its `blobs/` prefix on top of this.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    /// Flush and cleanly shut down the underlying SlateDB instance.
    pub async fn close(&self) -> Result<(), StoreError> {
        self.db.close().await?;
        Ok(())
    }

    /// Block until every write submitted so far — durable or relaxed — is
    /// durably persisted, by flushing SlateDB's WAL. The barrier a caller
    /// needs after a batch of `_relaxed` writes.
    pub async fn flush(&self) -> Result<(), StoreError> {
        self.db.flush().await?;
        Ok(())
    }

    // --- repository registry ---

    /// Look up a repository by name, returning `None` if it does not exist.
    pub async fn lookup_repo(&self, name: &str) -> Result<Option<RepoMeta>, StoreError> {
        match self.read_repo_id(name).await? {
            Some(id) => Ok(Some(self.read_repo_meta(id).await?)),
            None => Ok(None),
        }
    }

    /// Return the repository for `name`, creating it if absent. Safe under
    /// concurrent creation: the loser of a race observes and returns the
    /// winner's repository rather than allocating a second id.
    pub async fn get_or_create_repo(&self, name: &str) -> Result<RepoMeta, StoreError> {
        if let Some(meta) = self.lookup_repo(name).await? {
            return Ok(meta);
        }
        self.create_repo(name).await
    }

    /// Create the repository for `name`, or return the existing one if it was
    /// created concurrently. Allocation of the id, the `repo:<name>` mapping,
    /// the per-repo defaults, and the initial `HEAD` symref all happen inside
    /// one serializable-snapshot transaction.
    pub async fn create_repo(&self, name: &str) -> Result<RepoMeta, StoreError> {
        let repo_key = format!("{META_REPO_PREFIX}{name}");
        let global_repo_key = keys::meta_key(RepoId::GLOBAL, &repo_key);
        let next_id_key = keys::meta_key(RepoId::GLOBAL, META_NEXT_REPO_ID);

        for _ in 0..CREATE_ATTEMPTS {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            // Someone already created it (possibly a race winner): return it.
            if let Some(bytes) = txn.get(&global_repo_key).await? {
                let id = decode_repo_id(&repo_key, &MetaValue::decode(&bytes)?)?;
                txn.rollback();
                return self.read_repo_meta(id).await;
            }

            let next_id = match txn.get(&next_id_key).await? {
                Some(bytes) => decode_counter(&MetaValue::decode(&bytes)?)?,
                None => FIRST_REPO_ID,
            };
            let id = RepoId(next_id);

            txn.put(
                &next_id_key,
                MetaValue::Raw(next_id.saturating_add(1).to_be_bytes().to_vec()).encode(),
            )?;
            txn.put(
                &global_repo_key,
                MetaValue::Raw(next_id.to_be_bytes().to_vec()).encode(),
            )?;
            txn.put(
                keys::meta_key(id, META_OBJECT_FORMAT),
                MetaValue::Utf8(OBJECT_FORMAT_SHA1.to_owned()).encode(),
            )?;
            txn.put(
                keys::meta_key(id, META_DEFAULT_BRANCH),
                MetaValue::Utf8(DEFAULT_BRANCH.to_owned()).encode(),
            )?;
            txn.put(
                keys::meta_key(id, META_SCHEMA_VERSION),
                MetaValue::U32(SCHEMA_VERSION).encode(),
            )?;
            txn.put(
                keys::ref_key(id, HEAD_REF),
                RefTarget::Reference(DEFAULT_BRANCH.to_owned()).encode(),
            )?;

            match txn.commit().await {
                Ok(_) => {
                    return Ok(RepoMeta {
                        id,
                        object_format: Kind::Sha1,
                        default_branch: DEFAULT_BRANCH.to_owned(),
                    });
                }
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(StoreError::CreateContention)
    }

    async fn read_repo_id(&self, name: &str) -> Result<Option<RepoId>, StoreError> {
        let repo_key = format!("{META_REPO_PREFIX}{name}");
        let key = keys::meta_key(RepoId::GLOBAL, &repo_key);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(decode_repo_id(
                &repo_key,
                &MetaValue::decode(&bytes)?,
            )?)),
            None => Ok(None),
        }
    }

    async fn read_repo_meta(&self, id: RepoId) -> Result<RepoMeta, StoreError> {
        let object_format = match self.get_meta(id, META_OBJECT_FORMAT).await? {
            Some(MetaValue::Utf8(s)) => parse_object_format(&s)?,
            Some(_) => {
                return Err(StoreError::UnexpectedMeta {
                    repo: id,
                    key: META_OBJECT_FORMAT.to_owned(),
                });
            }
            None => {
                return Err(StoreError::MissingMeta {
                    repo: id,
                    key: META_OBJECT_FORMAT.to_owned(),
                });
            }
        };
        let default_branch = match self.get_meta(id, META_DEFAULT_BRANCH).await? {
            Some(MetaValue::Utf8(s)) => s,
            Some(_) => {
                return Err(StoreError::UnexpectedMeta {
                    repo: id,
                    key: META_DEFAULT_BRANCH.to_owned(),
                });
            }
            None => DEFAULT_BRANCH.to_owned(),
        };
        Ok(RepoMeta {
            id,
            object_format,
            default_branch,
        })
    }

    // --- typed object segment ops ---

    /// Read an object record, or `None` if the repo has no such object.
    pub async fn get_object(
        &self,
        repo: RepoId,
        oid: &oid,
    ) -> Result<Option<ObjectRecord>, StoreError> {
        match self.db.get(keys::object_key(repo, oid)).await? {
            Some(bytes) => Ok(Some(ObjectRecord::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Write an object record, waiting for it to become durable before
    /// returning.
    pub async fn put_object(
        &self,
        repo: RepoId,
        oid: &oid,
        record: &ObjectRecord,
    ) -> Result<(), StoreError> {
        self.put_object_with(repo, oid, record, Durability::Durable)
            .await
    }

    /// Write an object record without waiting for durability; see
    /// [`Durability::Relaxed`].
    pub async fn put_object_relaxed(
        &self,
        repo: RepoId,
        oid: &oid,
        record: &ObjectRecord,
    ) -> Result<(), StoreError> {
        self.put_object_with(repo, oid, record, Durability::Relaxed)
            .await
    }

    async fn put_object_with(
        &self,
        repo: RepoId,
        oid: &oid,
        record: &ObjectRecord,
        durability: Durability,
    ) -> Result<(), StoreError> {
        self.db
            .put_with_options(
                keys::object_key(repo, oid),
                record.encode(),
                &PutOptions::default(),
                &durability.write_options(),
            )
            .await?;
        Ok(())
    }

    /// Whether the repo holds an object record for `oid`.
    pub async fn object_exists(&self, repo: RepoId, oid: &oid) -> Result<bool, StoreError> {
        Ok(self.db.get(keys::object_key(repo, oid)).await?.is_some())
    }

    // --- typed ref segment ops ---

    /// Read a ref target, or `None` if the ref does not exist.
    pub async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefTarget>, StoreError> {
        match self.db.get(keys::ref_key(repo, name)).await? {
            Some(bytes) => Ok(Some(RefTarget::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Write a ref target unconditionally (no CAS; use [`Store::update_refs`]
    /// for compare-and-swap semantics).
    pub async fn put_ref(
        &self,
        repo: RepoId,
        name: &str,
        target: &RefTarget,
    ) -> Result<(), StoreError> {
        self.db
            .put(keys::ref_key(repo, name), target.encode())
            .await?;
        Ok(())
    }

    /// Delete a ref unconditionally.
    pub async fn delete_ref(&self, repo: RepoId, name: &str) -> Result<(), StoreError> {
        self.db.delete(keys::ref_key(repo, name)).await?;
        Ok(())
    }

    /// List refs in name order. With `prefix`, only refs whose name starts
    /// with it are returned (e.g. `refs/heads/`); with `None`, all refs.
    pub async fn list_refs(
        &self,
        repo: RepoId,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, RefTarget)>, StoreError> {
        let scan_prefix = match prefix {
            Some(p) => keys::ref_prefix(repo, p),
            None => keys::segment_prefix(repo, Segment::Ref),
        };
        let mut iter = self.db.scan_prefix(scan_prefix, ..).await?;
        let mut refs = Vec::new();
        while let Some(kv) = iter.next().await? {
            let (_, name) = keys::decode_ref_key(&kv.key)?;
            let target = RefTarget::decode(&kv.value)?;
            refs.push((name, target));
        }
        Ok(refs)
    }

    /// Apply a batch of ref updates with per-ref compare-and-swap inside a
    /// single serializable-snapshot transaction. A CAS mismatch or a symbolic
    /// target rejects only that command; the rest still commit (git's
    /// non-atomic default). A commit conflict is retried once, after which all
    /// commands are reported as `ng`.
    pub async fn update_refs(
        &self,
        repo: RepoId,
        updates: &[RefUpdate],
    ) -> Result<Vec<RefUpdateResult>, StoreError> {
        let mut attempt = 0;
        loop {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut results = Vec::with_capacity(updates.len());

            for update in updates {
                let key = keys::ref_key(repo, &update.name);
                let current = match txn.get(&key).await? {
                    None => None,
                    Some(bytes) => match RefTarget::decode(&bytes)? {
                        RefTarget::Direct(oid) => Some(oid),
                        RefTarget::Reference(_) => {
                            results.push(RefUpdateResult {
                                name: update.name.clone(),
                                outcome: RefOutcome::Rejected(REASON_SYMBOLIC.to_owned()),
                            });
                            continue;
                        }
                    },
                };

                if current != update.expected_old {
                    results.push(RefUpdateResult {
                        name: update.name.clone(),
                        outcome: RefOutcome::Rejected(REASON_STALE.to_owned()),
                    });
                    continue;
                }

                match update.new {
                    Some(new_oid) => txn.put(&key, RefTarget::Direct(new_oid).encode())?,
                    None => txn.delete(&key)?,
                }
                results.push(RefUpdateResult {
                    name: update.name.clone(),
                    outcome: RefOutcome::Updated,
                });
            }

            match txn.commit().await {
                Ok(_) => return Ok(results),
                Err(e) if is_conflict(&e) => {
                    attempt += 1;
                    if attempt >= UPDATE_REFS_ATTEMPTS {
                        return Ok(updates
                            .iter()
                            .map(|u| RefUpdateResult {
                                name: u.name.clone(),
                                outcome: RefOutcome::Rejected(REASON_CONFLICT.to_owned()),
                            })
                            .collect());
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    // --- typed commit-graph segment ops ---

    /// Read the commit-graph record for a commit, or `None` if unrecorded.
    pub async fn get_commit_graph(
        &self,
        repo: RepoId,
        oid: &oid,
    ) -> Result<Option<CommitGraphRecord>, StoreError> {
        match self.db.get(keys::commit_key(repo, oid)).await? {
            Some(bytes) => Ok(Some(CommitGraphRecord::decode(&bytes, oid.kind())?)),
            None => Ok(None),
        }
    }

    /// Write a commit-graph record, waiting for it to become durable before
    /// returning.
    pub async fn put_commit_graph(
        &self,
        repo: RepoId,
        oid: &oid,
        record: &CommitGraphRecord,
    ) -> Result<(), StoreError> {
        self.put_commit_graph_with(repo, oid, record, Durability::Durable)
            .await
    }

    /// Write a commit-graph record without waiting for durability; see
    /// [`Durability::Relaxed`].
    pub async fn put_commit_graph_relaxed(
        &self,
        repo: RepoId,
        oid: &oid,
        record: &CommitGraphRecord,
    ) -> Result<(), StoreError> {
        self.put_commit_graph_with(repo, oid, record, Durability::Relaxed)
            .await
    }

    async fn put_commit_graph_with(
        &self,
        repo: RepoId,
        oid: &oid,
        record: &CommitGraphRecord,
        durability: Durability,
    ) -> Result<(), StoreError> {
        self.db
            .put_with_options(
                keys::commit_key(repo, oid),
                record.encode(),
                &PutOptions::default(),
                &durability.write_options(),
            )
            .await?;
        Ok(())
    }

    // --- typed metadata segment ops ---

    /// Read a metadata value, or `None` if unset.
    pub async fn get_meta(
        &self,
        repo: RepoId,
        name: &str,
    ) -> Result<Option<MetaValue>, StoreError> {
        match self.db.get(keys::meta_key(repo, name)).await? {
            Some(bytes) => Ok(Some(MetaValue::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Write a metadata value.
    pub async fn put_meta(
        &self,
        repo: RepoId,
        name: &str,
        value: &MetaValue,
    ) -> Result<(), StoreError> {
        self.db
            .put(keys::meta_key(repo, name), value.encode())
            .await?;
        Ok(())
    }
}

/// Resolve the storage URL into a root object store already offset by the
/// URL's path component. SlateDB and blob offload build their prefixes on top.
/// Supports `file://`, `memory://`, and `s3://` (see `object_store::parse_url`).
fn resolve_root_store(storage_url: &str) -> Result<Arc<dyn ObjectStore>, StoreError> {
    let url = Url::parse(storage_url)?;
    let (store, path) = object_store::parse_url(&url)?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    if path.as_ref().is_empty() {
        Ok(store)
    } else {
        Ok(Arc::new(PrefixStore::new(store, path)))
    }
}

fn is_conflict(err: &slatedb::Error) -> bool {
    err.kind() == ErrorKind::Transaction
}

fn decode_repo_id(repo_key: &str, value: &MetaValue) -> Result<RepoId, StoreError> {
    raw_u64(value)
        .map(RepoId)
        .ok_or_else(|| StoreError::UnexpectedMeta {
            repo: RepoId::GLOBAL,
            key: repo_key.to_owned(),
        })
}

fn decode_counter(value: &MetaValue) -> Result<u64, StoreError> {
    raw_u64(value).ok_or_else(|| StoreError::UnexpectedMeta {
        repo: RepoId::GLOBAL,
        key: META_NEXT_REPO_ID.to_owned(),
    })
}

fn raw_u64(value: &MetaValue) -> Option<u64> {
    match value {
        MetaValue::Raw(bytes) => {
            let arr: [u8; 8] = bytes.as_slice().try_into().ok()?;
            Some(u64::from_be_bytes(arr))
        }
        _ => None,
    }
}

fn parse_object_format(value: &str) -> Result<Kind, StoreError> {
    match value {
        "sha1" => Ok(Kind::Sha1),
        "sha256" => Ok(Kind::Sha256),
        other => Err(StoreError::UnsupportedObjectFormat(other.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashSet;

    async fn memory_store() -> Store {
        Store::open("memory://").await.expect("open memory store")
    }

    fn oid(hex_byte: u8) -> ObjectId {
        // A deterministic distinct SHA-1 oid: 40 hex chars, all the same nibble
        // pair derived from `hex_byte` (which must itself be a hex character).
        let s = vec![hex_byte; 40];
        ObjectId::from_hex(&s).expect("valid sha1 hex")
    }

    #[tokio::test]
    async fn should_create_repo_with_registry_defaults_and_round_trip_lookup() {
        // given
        let store = memory_store().await;

        // when
        let meta = store.create_repo("acme/widgets").await.expect("create");

        // then
        assert_eq!(meta.id, RepoId(FIRST_REPO_ID));
        assert_eq!(meta.object_format, Kind::Sha1);
        assert_eq!(meta.default_branch, DEFAULT_BRANCH);

        // lookup returns the same metadata.
        let looked_up = store.lookup_repo("acme/widgets").await.expect("lookup");
        assert_eq!(looked_up, Some(meta.clone()));

        // Unknown repo resolves to None.
        assert_eq!(store.lookup_repo("nope").await.expect("lookup"), None);

        // Creation wrote the documented per-repo defaults.
        assert_eq!(
            store
                .get_meta(meta.id, META_OBJECT_FORMAT)
                .await
                .expect("meta"),
            Some(MetaValue::Utf8(OBJECT_FORMAT_SHA1.to_owned()))
        );
        assert_eq!(
            store
                .get_meta(meta.id, META_SCHEMA_VERSION)
                .await
                .expect("meta"),
            Some(MetaValue::U32(SCHEMA_VERSION))
        );
        assert_eq!(
            store
                .get_meta(meta.id, META_DEFAULT_BRANCH)
                .await
                .expect("meta"),
            Some(MetaValue::Utf8(DEFAULT_BRANCH.to_owned()))
        );

        // HEAD is a symref to the default branch.
        assert_eq!(
            store.get_ref(meta.id, HEAD_REF).await.expect("head"),
            Some(RefTarget::Reference(DEFAULT_BRANCH.to_owned()))
        );

        // The global allocation counter advanced past the first id.
        assert_eq!(
            store
                .get_meta(RepoId::GLOBAL, META_NEXT_REPO_ID)
                .await
                .expect("counter"),
            Some(MetaValue::Raw((FIRST_REPO_ID + 1).to_be_bytes().to_vec()))
        );
    }

    #[tokio::test]
    async fn should_increase_repo_ids_monotonically() {
        // given
        let store = memory_store().await;

        // when
        let a = store.create_repo("a").await.expect("create a");
        let b = store.create_repo("b").await.expect("create b");
        let c = store.create_repo("org/c").await.expect("create c");

        // then
        assert_eq!(a.id, RepoId(1));
        assert_eq!(b.id, RepoId(2));
        assert_eq!(c.id, RepoId(3));
    }

    #[tokio::test]
    async fn should_return_existing_repo_on_duplicate_create() {
        // given
        let store = memory_store().await;
        let first = store.create_repo("dup").await.expect("create");

        // when
        // A second create for the same name returns the existing repo (this
        // exercises the in-transaction "already exists" branch), not a new id.
        let second = store.create_repo("dup").await.expect("recreate");

        // then
        assert_eq!(first, second);

        // when
        let via_goc = store.get_or_create_repo("dup").await.expect("goc");

        // then
        assert_eq!(first, via_goc);
        // No extra id was allocated.
        assert_eq!(
            store
                .get_meta(RepoId::GLOBAL, META_NEXT_REPO_ID)
                .await
                .expect("counter"),
            Some(MetaValue::Raw(2u64.to_be_bytes().to_vec()))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_yield_one_repo_id_under_concurrent_create_race() {
        // given
        let store = memory_store().await;
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let store = store.clone();
            set.spawn(async move { store.get_or_create_repo("contended").await });
        }

        // when
        let mut ids = HashSet::new();
        while let Some(joined) = set.join_next().await {
            let meta = joined.expect("task joined").expect("get_or_create");
            ids.insert(meta.id);
        }

        // then
        // Every racer agreed on exactly one repo id...
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.into_iter().next(), Some(RepoId(FIRST_REPO_ID)));
        // ...and only one id was ever allocated.
        assert_eq!(
            store
                .get_meta(RepoId::GLOBAL, META_NEXT_REPO_ID)
                .await
                .expect("counter"),
            Some(MetaValue::Raw((FIRST_REPO_ID + 1).to_be_bytes().to_vec()))
        );
    }

    #[tokio::test]
    async fn should_support_ref_crud_and_ordered_listing() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("refs-repo").await.expect("create").id;

        // when
        store
            .put_ref(repo, "refs/heads/main", &RefTarget::Direct(oid(b'a')))
            .await
            .expect("put main");
        store
            .put_ref(repo, "refs/heads/dev", &RefTarget::Direct(oid(b'b')))
            .await
            .expect("put dev");
        store
            .put_ref(repo, "refs/tags/v1", &RefTarget::Direct(oid(b'c')))
            .await
            .expect("put tag");

        // then
        assert_eq!(
            store.get_ref(repo, "refs/heads/main").await.expect("get"),
            Some(RefTarget::Direct(oid(b'a')))
        );

        // when
        // Prefix scan is ordered and excludes non-matching prefixes.
        let heads = store
            .list_refs(repo, Some("refs/heads/"))
            .await
            .expect("list heads");

        // then
        assert_eq!(
            heads,
            vec![
                ("refs/heads/dev".to_owned(), RefTarget::Direct(oid(b'b'))),
                ("refs/heads/main".to_owned(), RefTarget::Direct(oid(b'a'))),
            ]
        );

        // when
        // Full listing is name-ordered; HEAD (from create) sorts before refs/*.
        let all = store.list_refs(repo, None).await.expect("list all");
        let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();

        // then
        assert_eq!(
            names,
            vec!["HEAD", "refs/heads/dev", "refs/heads/main", "refs/tags/v1"]
        );

        // when
        store
            .delete_ref(repo, "refs/heads/dev")
            .await
            .expect("delete");

        // then
        assert_eq!(
            store.get_ref(repo, "refs/heads/dev").await.expect("get"),
            None
        );
    }

    #[tokio::test]
    async fn should_apply_cas_create_update_and_delete() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("cas").await.expect("create").id;

        // when
        // Create: expected_old = None (must not exist).
        let created = store
            .update_refs(
                repo,
                &[RefUpdate {
                    name: "refs/heads/main".to_owned(),
                    expected_old: None,
                    new: Some(oid(b'1')),
                }],
            )
            .await
            .expect("create ref");

        // then
        assert_eq!(created[0].outcome, RefOutcome::Updated);
        assert_eq!(
            store.get_ref(repo, "refs/heads/main").await.expect("get"),
            Some(RefTarget::Direct(oid(b'1')))
        );

        // when
        // Fast-forward update: expected_old matches.
        let updated = store
            .update_refs(
                repo,
                &[RefUpdate {
                    name: "refs/heads/main".to_owned(),
                    expected_old: Some(oid(b'1')),
                    new: Some(oid(b'2')),
                }],
            )
            .await
            .expect("update ref");

        // then
        assert_eq!(updated[0].outcome, RefOutcome::Updated);

        // when
        // Delete: new = None.
        let deleted = store
            .update_refs(
                repo,
                &[RefUpdate {
                    name: "refs/heads/main".to_owned(),
                    expected_old: Some(oid(b'2')),
                    new: None,
                }],
            )
            .await
            .expect("delete ref");

        // then
        assert_eq!(deleted[0].outcome, RefOutcome::Updated);
        assert_eq!(
            store.get_ref(repo, "refs/heads/main").await.expect("get"),
            None
        );
    }

    #[tokio::test]
    async fn should_reject_cas_update_with_stale_old_oid() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("cas-stale").await.expect("create").id;
        store
            .put_ref(repo, "refs/heads/main", &RefTarget::Direct(oid(b'a')))
            .await
            .expect("seed");

        // when
        let results = store
            .update_refs(
                repo,
                &[RefUpdate {
                    name: "refs/heads/main".to_owned(),
                    expected_old: Some(oid(b'f')), // wrong current value
                    new: Some(oid(b'b')),
                }],
            )
            .await
            .expect("cas");

        // then
        assert_eq!(
            results[0].outcome,
            RefOutcome::Rejected(REASON_STALE.to_owned())
        );
        // Ref is unchanged.
        assert_eq!(
            store.get_ref(repo, "refs/heads/main").await.expect("get"),
            Some(RefTarget::Direct(oid(b'a')))
        );
    }

    #[tokio::test]
    async fn should_reject_cas_create_when_ref_already_exists() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("cas-exists").await.expect("create").id;
        store
            .put_ref(repo, "refs/heads/main", &RefTarget::Direct(oid(b'a')))
            .await
            .expect("seed");

        // when
        let results = store
            .update_refs(
                repo,
                &[RefUpdate {
                    name: "refs/heads/main".to_owned(),
                    expected_old: None, // "must not exist", but it does
                    new: Some(oid(b'b')),
                }],
            )
            .await
            .expect("cas");

        // then
        assert_eq!(
            results[0].outcome,
            RefOutcome::Rejected(REASON_STALE.to_owned())
        );
    }

    #[tokio::test]
    async fn should_commit_non_conflicting_commands_in_partial_cas_batch() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("cas-batch").await.expect("create").id;
        store
            .put_ref(repo, "refs/heads/keep", &RefTarget::Direct(oid(b'a')))
            .await
            .expect("seed keep");

        // when
        let results = store
            .update_refs(
                repo,
                &[
                    // Valid create of a fresh ref.
                    RefUpdate {
                        name: "refs/heads/fresh".to_owned(),
                        expected_old: None,
                        new: Some(oid(b'1')),
                    },
                    // Stale CAS on an existing ref.
                    RefUpdate {
                        name: "refs/heads/keep".to_owned(),
                        expected_old: Some(oid(b'9')),
                        new: Some(oid(b'2')),
                    },
                ],
            )
            .await
            .expect("cas batch");

        // then
        assert_eq!(results[0].outcome, RefOutcome::Updated);
        assert_eq!(
            results[1].outcome,
            RefOutcome::Rejected(REASON_STALE.to_owned())
        );
        // The valid command committed even though a sibling was rejected.
        assert_eq!(
            store.get_ref(repo, "refs/heads/fresh").await.expect("get"),
            Some(RefTarget::Direct(oid(b'1')))
        );
        // The rejected ref is untouched.
        assert_eq!(
            store.get_ref(repo, "refs/heads/keep").await.expect("get"),
            Some(RefTarget::Direct(oid(b'a')))
        );
    }

    #[tokio::test]
    async fn should_reject_cas_update_of_symbolic_ref() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("cas-symref").await.expect("create").id;

        // when
        // HEAD is symbolic and must not be directly CAS-updated.
        let results = store
            .update_refs(
                repo,
                &[RefUpdate {
                    name: HEAD_REF.to_owned(),
                    expected_old: None,
                    new: Some(oid(b'1')),
                }],
            )
            .await
            .expect("cas");

        // then
        assert_eq!(
            results[0].outcome,
            RefOutcome::Rejected(REASON_SYMBOLIC.to_owned())
        );
        // HEAD still points at the default branch.
        assert_eq!(
            store.get_ref(repo, HEAD_REF).await.expect("get"),
            Some(RefTarget::Reference(DEFAULT_BRANCH.to_owned()))
        );
    }

    #[tokio::test]
    async fn should_round_trip_meta_values() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("meta").await.expect("create").id;

        // when/then: each setting is its own put -> get -> compare cycle.
        for (name, value) in [
            ("string-setting", MetaValue::Utf8("hello".to_owned())),
            ("byte-setting", MetaValue::U8(7)),
            ("u32-setting", MetaValue::U32(4242)),
            ("raw-setting", MetaValue::Raw(vec![9, 8, 7])),
        ] {
            store.put_meta(repo, name, &value).await.expect("put meta");
            assert_eq!(
                store.get_meta(repo, name).await.expect("get meta"),
                Some(value)
            );
        }

        // then
        assert_eq!(
            store.get_meta(repo, "absent").await.expect("get meta"),
            None
        );
    }

    #[tokio::test]
    async fn should_round_trip_object_and_commit_graph_records() {
        // given
        let store = memory_store().await;
        let repo = store.create_repo("objects").await.expect("create").id;
        let blob = oid(b'a');

        // then
        assert!(!store.object_exists(repo, &blob).await.expect("exists"));

        // when
        let record = ObjectRecord::BlobInline(Bytes::from_static(b"blob 5\0hello"));
        store
            .put_object(repo, &blob, &record)
            .await
            .expect("put object");

        // then
        assert!(store.object_exists(repo, &blob).await.expect("exists"));
        assert_eq!(
            store.get_object(repo, &blob).await.expect("get object"),
            Some(record)
        );

        // given
        let commit = oid(b'c');
        let graph = CommitGraphRecord {
            generation: 2,
            root_tree: oid(b'd'),
            parents: vec![oid(b'e')],
        };

        // when
        store
            .put_commit_graph(repo, &commit, &graph)
            .await
            .expect("put graph");

        // then
        assert_eq!(
            store
                .get_commit_graph(repo, &commit)
                .await
                .expect("get graph"),
            Some(graph)
        );
        // Missing commit-graph record resolves to None.
        assert_eq!(
            store
                .get_commit_graph(repo, &oid(b'f'))
                .await
                .expect("get graph"),
            None
        );
    }

    #[tokio::test]
    async fn should_reopen_store_with_persisted_segment_extractor() {
        // given: a store on durable (file-backed) storage with one repo,
        // cleanly closed so the manifest (which records the segment
        // extractor's name) is persisted.
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("file://{}", dir.path().display());
        let store = Store::open(&url).await.expect("first open");
        let created = store.create_repo("acme/widgets").await.expect("create");
        store.close().await.expect("close");

        // when: reopening the same database.
        let reopened = Store::open(&url).await.expect("reopen");
        let found = reopened.lookup_repo("acme/widgets").await.expect("lookup");

        // then: the open succeeded against the persisted extractor name and
        // the data is intact.
        assert_eq!(found.map(|meta| meta.id), Some(created.id));
    }
}
