//! Pack ingestion into a verified, indexed local staging area.
//!
//! `docs/0001-init.md` §Packfiles: received packs are exploded on the
//! receiving end. §Receive API: the pack is unpacked into a per-request
//! temporary directory — building an index, resolving all deltas, and
//! verifying the pack checksum — so connectivity can be validated before
//! anything reaches committed storage. The temporary directory is deleted
//! when the request completes, success or failure.

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use gix_features::progress::Discard;
use gix_features::zlib;
use gix_hash::{ObjectId, oid};
use gix_object::Kind;
use gix_pack::Bundle;
use tempfile::TempDir;
use tokio::runtime::Handle;

use crate::storage::keys::RepoId;
use crate::storage::store::RepoMeta;
use crate::storage::{ObjectDb, ObjectDbError};

/// Errors returned by [`ingest_pack`] and [`StagedPack`] accessors.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Creating or writing the staging directory failed.
    #[error("staging directory i/o failed")]
    Io(#[from] std::io::Error),
    /// The pack stream was malformed, truncated, failed its checksum, or
    /// referenced a thin-pack base that exists nowhere.
    #[error("pack could not be indexed")]
    Pack(#[from] gix_pack::bundle::write::Error),
    /// A thin-pack base lookup against committed storage failed.
    #[error("thin-pack base lookup failed")]
    BaseLookup(#[from] ObjectDbError),
    /// The staged `.pack`/`.idx` pair could not be opened after writing.
    #[error("staged pack could not be opened")]
    Bundle(#[from] gix_pack::bundle::init::Error),
    /// A staged object could not be decoded even though the pack verified.
    /// Indicates the staged files changed underneath us.
    #[error("staged object {oid} could not be decoded")]
    Decode {
        /// The object id whose staged entry failed to decode.
        oid: ObjectId,
        /// The underlying pack decode failure.
        #[source]
        source: gix_pack::data::decode::Error,
    },
    /// The blocking ingestion task was cancelled or panicked.
    #[error("pack ingestion task aborted")]
    TaskAborted(#[from] tokio::task::JoinError),
}

/// A received pack, exploded into an indexed `.pack`/`.idx` pair inside its
/// own temporary directory. Dropping it deletes the directory, so staging
/// cleans up on both the success and the failure path.
pub struct StagedPack {
    /// Owns the on-disk staging area; removed on drop.
    _tempdir: TempDir,
    /// `None` for an empty pack (zero objects), which produces no files.
    bundle: Option<Bundle>,
}

impl StagedPack {
    /// Whether the pack contains an entry for `id`.
    pub fn contains(&self, id: &oid) -> bool {
        self.bundle
            .as_ref()
            .is_some_and(|bundle| bundle.index.lookup(id).is_some())
    }

    /// The kind of the packed object `id`, without decoding its body
    /// (delta-chain headers are followed instead). `None` if absent.
    pub fn info(&self, id: &oid) -> Result<Option<Kind>, IngestError> {
        let Some(bundle) = &self.bundle else {
            return Ok(None);
        };
        let Some(index) = bundle.index.lookup(id) else {
            return Ok(None);
        };
        kind_at(bundle, bundle.index.pack_offset_at_index(index), id).map(Some)
    }

    /// Decode the packed object `id` into its kind and body (content bytes,
    /// no git header). `None` if absent.
    pub fn read(&self, id: &oid) -> Result<Option<(Kind, Bytes)>, IngestError> {
        let Some(bundle) = &self.bundle else {
            return Ok(None);
        };
        let mut buf = Vec::new();
        let found = bundle
            .find(
                id,
                &mut buf,
                &mut zlib::Inflate::default(),
                &mut gix_pack::cache::Never,
            )
            .map_err(|source| IngestError::Decode {
                oid: id.to_owned(),
                source,
            })?;
        let kind = match found {
            Some((data, _location)) => data.kind,
            None => return Ok(None),
        };
        Ok(Some((kind, Bytes::from(buf))))
    }

    /// Iterate over every packed object as `(oid, kind)`, in index (oid)
    /// order.
    pub fn iter(&self) -> impl Iterator<Item = Result<(ObjectId, Kind), IngestError>> + '_ {
        self.bundle.iter().flat_map(|bundle| {
            bundle.index.iter().map(move |entry| {
                let kind = kind_at(bundle, entry.pack_offset, &entry.oid)?;
                Ok((entry.oid, kind))
            })
        })
    }

    /// The number of objects staged by this pack.
    pub fn object_count(&self) -> u32 {
        self.bundle
            .as_ref()
            .map_or(0, |bundle| bundle.index.num_objects())
    }
}

/// Resolve the kind of the entry at `offset` by following delta headers.
fn kind_at(bundle: &Bundle, offset: u64, id: &oid) -> Result<Kind, IngestError> {
    let decode_error = |source: gix_pack::data::decode::Error| IngestError::Decode {
        oid: id.to_owned(),
        source,
    };
    let entry = bundle
        .pack
        .entry(offset)
        .map_err(gix_pack::data::decode::Error::from)
        .map_err(decode_error)?;
    // Staged packs are self-contained: ingestion rewrites every ref-delta
    // into an ofs-delta over an in-pack base, so out-of-pack resolution
    // never happens.
    let outcome = bundle
        .pack
        .decode_header(entry, &mut zlib::Inflate::default(), &|_| None)
        .map_err(decode_error)?;
    Ok(outcome.kind)
}

/// Stream a received pack into a fresh temporary directory under
/// `staging_root`, resolving deltas and verifying the pack checksum as it is
/// indexed. Thin-pack bases (git clients delta against objects they know the
/// server has) are fetched from `objectdb` under `repo` and inlined into the
/// staged pack. Zero input bytes and a zero-object pack are both valid and
/// yield an empty [`StagedPack`]: delete-only pushes send no objects.
pub async fn ingest_pack(
    input: impl Read + Send + 'static,
    objectdb: &ObjectDb,
    repo: &RepoMeta,
    staging_root: &Path,
) -> Result<StagedPack, IngestError> {
    let lookup_error = Arc::new(Mutex::new(None));
    let lookup = ObjectDbLookup {
        db: objectdb.clone(),
        repo: repo.id,
        handle: Handle::current(),
        error: Arc::clone(&lookup_error),
    };
    let object_hash = repo.object_format;
    let staging_root = staging_root.to_path_buf();

    let staged = tokio::task::spawn_blocking(move || {
        ingest_blocking(input, lookup, object_hash, &staging_root)
    })
    .await?;

    // A failed (as opposed to unsuccessful) base lookup is reported to the
    // pack iterator as "not found" — see `ObjectDbLookup` — so the recorded
    // storage error supersedes whatever the pack machinery derived from it.
    if let Some(db_error) = lookup_error.lock().ok().and_then(|mut slot| slot.take()) {
        return Err(IngestError::BaseLookup(db_error));
    }
    staged
}

/// The synchronous body of [`ingest_pack`], run on the blocking pool.
fn ingest_blocking(
    input: impl Read,
    lookup: ObjectDbLookup,
    object_hash: gix_hash::Kind,
    staging_root: &Path,
) -> Result<StagedPack, IngestError> {
    std::fs::create_dir_all(staging_root)?;
    let tempdir = TempDir::new_in(staging_root)?;

    // A delete-only push may send no pack at all: zero input bytes are an
    // empty pack.
    let mut reader = BufReader::new(input);
    if reader.fill_buf()?.is_empty() {
        return Ok(StagedPack {
            _tempdir: tempdir,
            bundle: None,
        });
    }

    let should_interrupt = AtomicBool::new(false);
    let outcome = Bundle::write_to_directory(
        &mut reader,
        Some(tempdir.path()),
        &mut Discard,
        &should_interrupt,
        Some(lookup),
        gix_pack::bundle::write::Options {
            thread_limit: None,
            iteration_mode: gix_pack::data::input::Mode::Verify,
            index_version: gix_pack::index::Version::default(),
            object_hash,
        },
    )?;

    // A zero-object pack produces no `.pack`/`.idx` files at all.
    let bundle = outcome.to_bundle().transpose()?;
    Ok(StagedPack {
        _tempdir: tempdir,
        bundle,
    })
}

/// Thin-pack base lookup: fetches canonical object bytes from committed
/// storage so ref-delta entries can be resolved against them.
struct ObjectDbLookup {
    db: ObjectDb,
    repo: RepoId,
    handle: Handle,
    error: Arc<Mutex<Option<ObjectDbError>>>,
}

impl gix_object::Find for ObjectDbLookup {
    fn try_find<'a>(
        &self,
        id: &oid,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Option<gix_object::Data<'a>>, gix_object::find::Error> {
        match self.handle.block_on(self.db.get(self.repo, id)) {
            Ok(Some((kind, body))) => {
                buffer.clear();
                buffer.extend_from_slice(&body);
                Ok(Some(gix_object::Data {
                    kind,
                    object_hash: id.kind(),
                    data: buffer.as_slice(),
                }))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                // The pack iterator swallows lookup `Err`s by ending
                // iteration early, which would stage a silently truncated
                // pack. Record the failure and report "not found" instead:
                // that surfaces as a hard error, which `ingest_pack` then
                // replaces with the recorded cause.
                if let Ok(mut slot) = self.error.lock() {
                    slot.get_or_insert(err);
                }
                Ok(None)
            }
        }
    }
}
