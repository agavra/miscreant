//! Pack ingestion into a verified, indexed local staging area.
//!
//! `docs/0001-init.md` §Packfiles: received packs are exploded on the
//! receiving end. §Receive API: the pack is unpacked into a per-request
//! temporary directory — building an index, resolving all deltas, and
//! verifying the pack checksum — so connectivity can be validated before
//! anything reaches committed storage. The temporary directory is deleted
//! when the request completes, success or failure.

use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bytes::Bytes;
use gix_features::progress::Discard;
use gix_features::zlib;
use gix_hash::{ObjectId, oid};
use gix_object::Kind;
use gix_pack::Bundle;
use gix_pack::data::entry::Header;
use gix_pack::data::input::{BytesToEntriesIter, EntryDataMode, Mode};
use tempfile::TempDir;

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
/// server has) are prefetched from `objectdb` under `repo` before indexing,
/// then served from an in-memory map while the pack is written. Zero input
/// bytes and a zero-object pack are both valid and yield an empty
/// [`StagedPack`]: delete-only pushes send no objects.
pub async fn ingest_pack(
    input: impl Read + Send + 'static,
    objectdb: &ObjectDb,
    repo: &RepoMeta,
    staging_root: &Path,
) -> Result<StagedPack, IngestError> {
    let object_hash = repo.object_format;
    let staging_root = staging_root.to_path_buf();

    // Spool the incoming stream to disk and scan its entry headers for
    // ref-delta base ids. Both steps are synchronous (`Read`/`BufRead`), so
    // they run on the blocking pool; nothing here touches `objectdb` yet.
    let spooled =
        tokio::task::spawn_blocking(move || spool_and_scan(input, object_hash, &staging_root))
            .await??;

    let Some(pack_path) = spooled.pack_path else {
        return Ok(StagedPack {
            _tempdir: spooled.tempdir,
            bundle: None,
        });
    };

    // Prefetch every distinct ref-delta base from committed storage. A
    // store failure surfaces immediately with its real cause; a base that
    // simply isn't there is omitted — gix decides later whether that makes
    // the pack invalid.
    let ref_delta_bases = spooled.ref_delta_bases.len();
    let mut bases = HashMap::with_capacity(ref_delta_bases);
    for base_id in spooled.ref_delta_bases {
        if let Some(object) = objectdb.get(repo.id, &base_id).await? {
            bases.insert(base_id, object);
        }
    }
    let bases_prefetched = bases.len();

    let tempdir = spooled.tempdir;
    let (tempdir, bundle) = tokio::task::spawn_blocking(move || {
        let bundle = write_bundle(&pack_path, tempdir.path(), object_hash, bases);
        (tempdir, bundle)
    })
    .await?;

    let staged = StagedPack {
        _tempdir: tempdir,
        bundle: bundle?,
    };
    tracing::debug!(
        objects = staged.object_count(),
        ref_delta_bases,
        bases_prefetched,
        "pack ingested"
    );
    Ok(staged)
}

/// The spooled pack file plus the ref-delta base ids found while scanning
/// its entry headers, still inside the staging tempdir that owns them.
struct Spooled {
    tempdir: TempDir,
    /// `None` when zero bytes were spooled (a delete-only push).
    pack_path: Option<PathBuf>,
    ref_delta_bases: HashSet<ObjectId>,
}

/// Copy `input` into a fresh file inside a new tempdir under `staging_root`,
/// then scan the spooled pack's entry headers to collect every `RefDelta`
/// base id. Scanning never resolves deltas or hashes the pack: it uses
/// [`Mode::AsIs`] and discards entry bytes, so a corrupt or truncated pack
/// simply yields whatever bases were found before the scan gave up —
/// [`write_bundle`] performs the real, verifying pass and reports the
/// authoritative error.
fn spool_and_scan(
    mut input: impl Read,
    object_hash: gix_hash::Kind,
    staging_root: &Path,
) -> Result<Spooled, IngestError> {
    std::fs::create_dir_all(staging_root)?;
    let tempdir = TempDir::new_in(staging_root)?;
    let pack_path = tempdir.path().join("received.pack");

    let bytes_spooled = std::io::copy(&mut input, &mut std::fs::File::create(&pack_path)?)?;
    if bytes_spooled == 0 {
        return Ok(Spooled {
            tempdir,
            pack_path: None,
            ref_delta_bases: HashSet::new(),
        });
    }

    let ref_delta_bases = scan_ref_delta_bases(&pack_path, object_hash)?;
    Ok(Spooled {
        tempdir,
        pack_path: Some(pack_path),
        ref_delta_bases,
    })
}

/// Scan every entry header in the pack at `pack_path`, collecting the base
/// ids of `RefDelta` entries. A malformed header or a decode failure part
/// way through just truncates what is collected here; it does not surface
/// as an error, since [`write_bundle`]'s verifying pass will hit the same
/// problem and report it properly.
fn scan_ref_delta_bases(
    pack_path: &Path,
    object_hash: gix_hash::Kind,
) -> Result<HashSet<ObjectId>, IngestError> {
    let reader = BufReader::new(std::fs::File::open(pack_path)?);
    let entries = match BytesToEntriesIter::new_from_header(
        reader,
        Mode::AsIs,
        EntryDataMode::Ignore,
        object_hash,
    ) {
        Ok(entries) => entries,
        Err(_) => return Ok(HashSet::new()),
    };
    Ok(entries
        .filter_map(Result::ok)
        .filter_map(|entry| match entry.header {
            Header::RefDelta { base_id } => Some(base_id),
            _ => None,
        })
        .collect())
}

/// The synchronous body of [`ingest_pack`]'s indexing step, run on the
/// blocking pool. `bases` is the already-prefetched, infallible lookup for
/// any ref-delta bases the pack references.
fn write_bundle(
    pack_path: &Path,
    directory: &Path,
    object_hash: gix_hash::Kind,
    bases: HashMap<ObjectId, (Kind, Bytes)>,
) -> Result<Option<Bundle>, IngestError> {
    let mut reader = BufReader::new(std::fs::File::open(pack_path)?);
    let should_interrupt = AtomicBool::new(false);
    let outcome = Bundle::write_to_directory(
        &mut reader,
        Some(directory),
        &mut Discard,
        &should_interrupt,
        Some(PrefetchedBases(bases)),
        gix_pack::bundle::write::Options {
            thread_limit: None,
            iteration_mode: Mode::Verify,
            index_version: gix_pack::index::Version::default(),
            object_hash,
        },
    )?;

    // A zero-object pack produces no `.pack`/`.idx` files at all.
    Ok(outcome.to_bundle().transpose()?)
}

/// Thin-pack base lookup backed by objects fetched from committed storage
/// ahead of time. Infallible by construction: a miss just means the base
/// wasn't prefetched (either it doesn't exist, or the pack never referenced
/// it), which `gix_pack` reports as the usual "base not found" pack error.
struct PrefetchedBases(HashMap<ObjectId, (Kind, Bytes)>);

impl gix_object::Find for PrefetchedBases {
    fn try_find<'a>(
        &self,
        id: &oid,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Option<gix_object::Data<'a>>, gix_object::find::Error> {
        let Some((kind, body)) = self.0.get(id) else {
            return Ok(None);
        };
        buffer.clear();
        buffer.extend_from_slice(body);
        Ok(Some(gix_object::Data {
            kind: *kind,
            object_hash: id.kind(),
            data: buffer.as_slice(),
        }))
    }
}
