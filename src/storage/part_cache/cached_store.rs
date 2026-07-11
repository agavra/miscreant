// Adapted from SlateDB `src/cached_object_store/object_store.rs` (Apache-2.0).
// See the module comment in `mod.rs` for provenance.
//
// Adaptations:
//   * The per-call `ObjectStoreCallTag`/policy routing is removed. SlateDB
//     0.14.1 (the published crate miscreant links) does not stamp those tags,
//     and miscreant additionally caches untagged offloaded-blob reads, so every
//     GET/HEAD is served read-through and every PUT is gated only by the
//     `cache_puts` flag (miscreant sets it false: read-miss admission only).
//   * `SlateDBError` -> the local `PartCacheError`; metrics route through the
//     `metrics` facade (see `stats.rs`); import paths point at this module.
//   * `from_config` and `load_files_to_cache` are dropped (unused by miscreant).

// Vendored code keeps upstream style; allow the lint that would otherwise force
// it to diverge from the upstream source.
#![allow(clippy::useless_conversion, clippy::collapsible_if)]

use super::single_flight::SingleFlight;
use super::stats::CachedObjectStoreStats;
use super::storage::{LocalCacheEntry, LocalCacheStorage, PartID};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, future::BoxFuture, stream, stream::BoxStream};
use log::warn;
use object_store::{
    Attributes, CopyOptions, Extensions, GetRange, GetResultPayload, PutMultipartOptions,
    PutResult, RenameOptions,
};
use object_store::{GetOptions, GetResult, ObjectMeta, ObjectStore, path::Path};
use object_store::{ListResult, MultipartUpload, PutOptions, PutPayload};
use std::{ops::Range, sync::Arc};

/// Configuration error building a [`CachedObjectStore`]. Replaces SlateDB's
/// `SlateDBError::InvalidCachePartSize`.
#[derive(Debug, thiserror::Error)]
pub enum PartCacheError {
    /// The part size must be a positive multiple of 1024 bytes.
    #[error("part size must be a positive multiple of 1024 bytes, got {0}")]
    InvalidPartSize(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct CachedObjectStore {
    object_store: Arc<dyn ObjectStore>,
    part_size_bytes: usize, // expected to be aligned with mb or kb
    pub(crate) cache_storage: Arc<dyn LocalCacheStorage>,
    // Whether write-source PUTs are cached. Miscreant leaves this false: the
    // cache admits on read-miss only, so SlateDB WAL churn cannot pollute it.
    cache_puts: bool,
    stats: Arc<CachedObjectStoreStats>,
    // Deduplicates concurrent HEAD requests for the same path after a cache miss.
    head_flights: SingleFlight<Path, (ObjectMeta, Attributes, Extensions)>,
    // Deduplicates concurrent prefetch/GET requests for the same path after a cache miss.
    prefetch_flights:
        SingleFlight<(Path, Option<GetRangeKey>), (ObjectMeta, Attributes, Extensions)>,
    // Deduplicates concurrent fetches of the same part after a cache miss.
    // Keyed on (path, part_id) so multiple readers needing the same part share one fetch.
    part_flights: SingleFlight<(Path, PartID), Bytes>,
}

impl CachedObjectStore {
    pub(crate) fn new(
        object_store: Arc<dyn ObjectStore>,
        cache_storage: Arc<dyn LocalCacheStorage>,
        part_size_bytes: usize,
        cache_puts: bool,
        stats: Arc<CachedObjectStoreStats>,
    ) -> Result<Arc<Self>, PartCacheError> {
        if part_size_bytes == 0 || !part_size_bytes.is_multiple_of(1024) {
            return Err(PartCacheError::InvalidPartSize(part_size_bytes));
        }

        Ok(Arc::new(Self {
            object_store,
            part_size_bytes,
            cache_storage,
            cache_puts,
            stats,
            head_flights: SingleFlight::new(),
            prefetch_flights: SingleFlight::new(),
            part_flights: SingleFlight::new(),
        }))
    }

    pub(crate) async fn start_evictor(&self) {
        self.cache_storage.start_evictor().await;
    }

    pub(crate) async fn cached_head(
        &self,
        location: &Path,
        admit_on_miss: bool,
    ) -> object_store::Result<GetResult> {
        let entry = self.cache_storage.entry(location, self.part_size_bytes);
        if let Ok(Some((meta, attributes))) = entry.read_head().await {
            return Ok(head_only_get_result(meta, attributes, Extensions::new()));
        }

        // Cache miss — deduplicate concurrent HEAD requests for the same path.
        let (meta, attributes, extensions) = self
            .head_flights
            .call(location.clone(), || async {
                let result = self
                    .object_store
                    .get_opts(
                        location,
                        GetOptions {
                            range: None,
                            head: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                let meta = result.meta.clone();
                let attributes = result.attributes.clone();
                let extensions = result.extensions.clone();

                if admit_on_miss {
                    self.save_get_result(location, result).await.ok();
                }
                Ok::<_, object_store::Error>((meta, attributes, extensions))
            })
            .await?;
        Ok(head_only_get_result(meta, attributes, extensions))
    }

    pub(crate) async fn cached_get_opts(
        &self,
        location: &Path,
        opts: GetOptions,
        force_refresh: bool,
    ) -> object_store::Result<GetResult> {
        let PrefetchedHead {
            meta,
            attributes,
            extensions,
            head_source,
        } = self.maybe_prefetch_range(location, opts.clone()).await?;

        let get_range = opts.range.clone();
        let range = self.canonicalize_range(get_range, meta.size)?;
        let parts = self.split_range_into_parts(range.clone());

        // Read parts and concatenate them into a single stream. Some parts may not
        // be cached; read_part falls back to the object store for the missing ones.
        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| {
                let this = self.clone();
                let location = location.clone();
                async move {
                    let (bytes, part_source) = this
                        .read_part(&location, part_id, range_in_part, force_refresh)
                        .await?;
                    let hit = head_source == ReadResultSource::Disk
                        && part_source == ReadResultSource::Disk;
                    this.stats.record_part_access(hit, bytes.len());
                    Ok::<Bytes, object_store::Error>(bytes)
                }
            })
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();

        Ok(GetResult {
            meta,
            range,
            attributes,
            payload: GetResultPayload::Stream(result_stream),
            extensions,
        })
    }

    async fn cached_put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        if !self.cache_puts {
            // Write directly to upstream without caching the payload.
            return self.object_store.put_opts(location, payload, opts).await;
        }

        // Capture the size and attributes before payload/opts are consumed: they
        // go into the head we write below.
        let payload_size = payload.content_length() as u64;
        let attributes = opts.attributes.clone();

        // First, write to the upstream object store.
        let result = self
            .object_store
            .put_opts(location, payload.clone(), opts)
            .await?;

        // Convert PutPayload to stream and save parts to cache.
        let entry = self.cache_storage.entry(location, self.part_size_bytes);
        let stream = stream::iter(payload.into_iter()).map(Ok::<Bytes, object_store::Error>);
        // Save parts, ignoring errors (cache failures must not fail the PUT).
        self.save_parts_stream(entry.as_ref(), stream, 0).await.ok();

        // Make the write visible to reads by writing the head. This is not
        // the actual HEAD response from the upstream store, but a synthesized
        // head with the known size and attributes.
        let meta = build_head(location, payload_size, &result);
        entry.save_head((&meta, &attributes)).await.ok();

        Ok(result)
    }

    // if an object is not cached before, maybe_prefetch_range will try to prefetch the object from the
    // object store and save the parts into the local disk cache. the prefetching is helpful to reduce the
    // number of GET requests to the object store, it'll try to aggregate the parts among the range into a
    // single GET request, and save the related parts into local disks together.
    // when it sends GET requests to the object store, the range is expected to be ALIGNED with the part
    // size.
    async fn maybe_prefetch_range(
        &self,
        location: &Path,
        mut opts: GetOptions,
    ) -> object_store::Result<PrefetchedHead> {
        let entry = self.cache_storage.entry(location, self.part_size_bytes);
        match entry.read_head().await {
            Ok(Some((meta, attrs))) => {
                return Ok(PrefetchedHead {
                    meta,
                    attributes: attrs,
                    extensions: Extensions::new(),
                    head_source: ReadResultSource::Disk,
                });
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "failed to read head from disk cache, will fallback to object store [location={}, error={:?}]",
                    location, e,
                );
            }
        }

        if let Some(range) = &opts.range {
            opts.range = Some(self.align_get_range(range));
        }

        // Cache miss — deduplicate concurrent prefetch requests for the same path.
        // Only one caller performs the fetch+save; others share the metadata result.
        // Parts not covered by the winning caller's range are handled by read_part's
        // own object-store fallback, so correctness is maintained.
        self.prefetch_flights
            .call(
                (location.clone(), opts.range.clone().map(Into::into)),
                || async {
                    let get_result = self.object_store.get_opts(location, opts).await?;
                    let result_meta = get_result.meta.clone();
                    let result_attrs = get_result.attributes.clone();
                    let result_extensions = get_result.extensions.clone();
                    // swallow the error on saving to disk here (the disk might be already full), just fallback
                    // to the object store.
                    // TODO: add a warning log here
                    self.save_get_result(location, get_result).await.ok();
                    Ok((result_meta, result_attrs, result_extensions))
                },
            )
            .await
            .map(|(meta, attributes, extensions)| PrefetchedHead {
                meta,
                attributes,
                extensions,
                head_source: ReadResultSource::Upstream,
            })
    }

    /// save the GetResult to the disk cache, a GetResult may be transformed into multiple part
    /// files and a meta file. please note that the `range` in the GetResult is expected to be
    /// aligned with the part size.
    async fn save_get_result(
        &self,
        cache_location: &Path,
        result: GetResult,
    ) -> object_store::Result<u64> {
        let part_size_bytes_u64 = self.part_size_bytes as u64;
        assert!(result.range.start.is_multiple_of(part_size_bytes_u64));
        assert!(
            result.range.end.is_multiple_of(part_size_bytes_u64)
                || result.range.end == result.meta.size
        );

        let entry = self
            .cache_storage
            .entry(cache_location, self.part_size_bytes);
        let object_size = result.meta.size;

        // Reaching here means the read policy already chose to fill the cache
        // so always save.
        entry.save_head((&result.meta, &result.attributes)).await?;

        let start_part_number = usize::try_from(result.range.start / part_size_bytes_u64)
            .expect("Part number exceeds u32 on a 32-bit system. Try increasing part size.");

        let stream = result.into_stream();

        self.save_parts_stream(entry.as_ref(), stream, start_part_number)
            .await?;

        Ok(object_size)
    }

    /// Save a stream of bytes to cache as parts, starting from the specified part number.
    /// Returns the number of bytes saved.
    /// This method only saves the data parts - the head should be saved separately.
    async fn save_parts_stream<S>(
        &self,
        entry: &dyn LocalCacheEntry,
        mut stream: S,
        start_part_number: usize,
    ) -> object_store::Result<usize>
    where
        S: stream::Stream<Item = Result<Bytes, object_store::Error>> + Unpin,
    {
        let mut buffer = BytesMut::new();
        let mut part_number = start_part_number;
        let mut total_bytes: usize = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            total_bytes += chunk.len();
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= self.part_size_bytes {
                let to_write = buffer.split_to(self.part_size_bytes);
                entry.save_part(part_number, to_write.into()).await?;
                part_number += 1;
            }
        }

        // Save any remaining bytes as the last part
        if !buffer.is_empty() {
            entry.save_part(part_number, buffer.into()).await?;
        }

        Ok(total_bytes)
    }

    // split the range into parts, and return the part id and the range inside the part.
    fn split_range_into_parts(&self, range: Range<u64>) -> Vec<(PartID, Range<usize>)> {
        let part_size_bytes_u64 = self.part_size_bytes as u64;
        let range_aligned = self.align_range(&range, self.part_size_bytes);
        let start_part = range_aligned.start / part_size_bytes_u64;
        let end_part = range_aligned.end / part_size_bytes_u64;
        let mut parts: Vec<_> = (start_part..end_part)
            .map(|part_id| {
                (
                    usize::try_from(part_id).expect("Number of parts exceeds usize"),
                    Range {
                        start: 0,
                        end: self.part_size_bytes,
                    },
                )
            })
            .collect();
        if parts.is_empty() {
            return vec![];
        }
        if let Some(first_part) = parts.first_mut() {
            first_part.1.start = usize::try_from(range.start % part_size_bytes_u64)
                .expect("Part size is too large to fit in a usize");
        }
        if let Some(last_part) = parts.last_mut() {
            if !range.end.is_multiple_of(part_size_bytes_u64) {
                last_part.1.end = usize::try_from(range.end % part_size_bytes_u64)
                    .expect("Part size is too large to fit in a usize");
            }
        }
        parts
    }

    /// Get a part from disk if cached, otherwise start a new GET request.
    ///
    /// IO errors reading the disk cache are ignored and fall back to the object
    /// store.
    ///
    /// Returns the bytes plus where they were served from, so the caller can
    /// classify the read as a hit or a miss.
    fn read_part(
        &self,
        location: &Path,
        part_id: PartID,
        range_in_part: Range<usize>,
        force_refresh: bool,
    ) -> BoxFuture<'static, object_store::Result<(Bytes, ReadResultSource)>> {
        let this = self.clone();
        let location = location.clone();
        Box::pin(async move {
            let entry = this.cache_storage.entry(&location, this.part_size_bytes);
            if !force_refresh {
                if let Ok(Some(bytes)) = entry.read_part(part_id, range_in_part.clone()).await {
                    return Ok((bytes, ReadResultSource::Disk));
                }
            }

            // Cache miss, so we need to fetch from the object store.
            // Read Part — deduplicate concurrent fetches of the same part.
            // The SingleFlight fetches the full part and saves it to cache; each
            // caller then slices out their own range_in_part.
            let bytes = this
                .part_flights
                .call((location.clone(), part_id), || async {
                    let part_range = Range {
                        start: (part_id * this.part_size_bytes) as u64,
                        end: ((part_id + 1) * this.part_size_bytes) as u64,
                    };
                    let get_result = this
                        .object_store
                        .get_opts(
                            &location,
                            GetOptions {
                                range: Some(GetRange::Bounded(part_range)),
                                ..Default::default()
                            },
                        )
                        .await?;

                    // Save the head and the part to cache for future accesses.
                    let entry = this.cache_storage.entry(&location, this.part_size_bytes);
                    let meta = get_result.meta.clone();
                    let attrs = get_result.attributes.clone();
                    let bytes = get_result.bytes().await?;
                    entry.save_head((&meta, &attrs)).await.ok();
                    entry.save_part(part_id, bytes.clone()).await.ok();

                    Ok::<_, object_store::Error>(bytes)
                })
                .await?;

            Ok((bytes.slice(range_in_part), ReadResultSource::Upstream))
        })
    }

    // given the range and object size, return the canonicalized `Range<usize>` with concrete start and
    // end.
    fn canonicalize_range(
        &self,
        range: Option<GetRange>,
        object_size: u64,
    ) -> object_store::Result<Range<u64>> {
        let (start_offset, end_offset) = match range {
            None => (0, object_size),
            Some(range) => match range {
                GetRange::Bounded(range) => {
                    if range.start >= object_size {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::StartTooLarge {
                                requested: range.start,
                                length: object_size,
                            }),
                        });
                    }
                    if range.start >= range.end {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::Inconsistent {
                                start: range.start,
                                end: range.end,
                            }),
                        });
                    }
                    (range.start, range.end.min(object_size))
                }
                GetRange::Offset(offset) => {
                    if offset >= object_size {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::StartTooLarge {
                                requested: offset,
                                length: object_size,
                            }),
                        });
                    }
                    (offset, object_size)
                }
                GetRange::Suffix(suffix) => (object_size.saturating_sub(suffix), object_size),
            },
        };
        Ok(Range {
            start: start_offset,
            end: end_offset,
        })
    }

    fn align_get_range(&self, range: &GetRange) -> GetRange {
        match range {
            GetRange::Bounded(bounded) => {
                let aligned = self.align_range(bounded, self.part_size_bytes);
                GetRange::Bounded(aligned)
            }
            GetRange::Suffix(suffix) => {
                let suffix_aligned = self.align_range(&(0..*suffix), self.part_size_bytes).end;
                GetRange::Suffix(suffix_aligned)
            }
            GetRange::Offset(offset) => {
                let offset_aligned = *offset - *offset % self.part_size_bytes as u64;
                GetRange::Offset(offset_aligned)
            }
        }
    }

    fn align_range(&self, range: &Range<u64>, alignment: usize) -> Range<u64> {
        let alignment = alignment as u64;
        let start_aligned = range.start - range.start % alignment;
        let end_aligned = range.end.div_ceil(alignment) * alignment;
        Range {
            start: start_aligned,
            end: end_aligned,
        }
    }
}

fn head_only_get_result(
    meta: ObjectMeta,
    attributes: Attributes,
    extensions: Extensions,
) -> GetResult {
    GetResult {
        payload: GetResultPayload::Stream(stream::empty().boxed()),
        range: 0..0,
        meta,
        attributes,
        extensions,
    }
}

/// Builds a synthetic head to save on a write, from the upstream `PutResult`
/// and the known object size.
///
/// The head is the cache entry's commit point: cached parts are not usable
/// until a `read_head` succeeds, so writing it last (after the upstream write
/// completes) publishes the entry.
fn build_head(cache_location: &Path, size: u64, result: &PutResult) -> ObjectMeta {
    ObjectMeta {
        location: cache_location.clone(),
        // `last_modified` is not used by the cache, add a stub instead of
        // executing an actual HEAD request after write. If this ever change,
        // the cache should be updated to use the upstream `last_modified`
        // instead of the stub value here.
        last_modified: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
            .expect("unix epoch is a valid timestamp"),
        size,
        e_tag: result.e_tag.clone(),
        version: result.version.clone(),
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CachedObjectStore({}, {})",
            self.object_store, self.cache_storage
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Every read is served read-through: serve from the disk cache, and on a
        // miss fetch from upstream and admit the parts (read-miss admission).
        if options.head {
            return self.cached_head(location, true).await;
        }
        self.cached_get_opts(location, options, false).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.cached_put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // TODO: add proper support for multipart uploads in the cache.
        self.object_store.put_multipart_opts(location, opts).await
    }

    /// Deletion of the cache entries associated with the object being
    /// deleted is not atomic with respect to the object deletion from
    /// the underlying object store. So for some period of time after
    /// the deletion, cached object parts are still visible in the cache.
    /// But assuming each object ever created by SlateDB is immutable and
    /// has a unique name, this is not a problem.
    ///
    /// If eviction is enabled, deletion of the associated cache entries
    /// happens asynchronously; when the control returns to the caller,
    /// the entries still might be present in the cache. If eviction is
    /// off, the deletion happens synchronously; when the control returns
    /// to the caller, it is guaranteed no entries present in the cache
    /// (assuming no errors happened during the deletion).
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let cache_storage = self.cache_storage.clone();
        let part_size_bytes = self.part_size_bytes;

        self.object_store
            .delete_stream(locations)
            .then(move |result| {
                let cache_storage = cache_storage.clone();
                async move {
                    if let Ok(ref location) = result {
                        let entry = cache_storage.entry(location, part_size_bytes);
                        entry.delete().await;
                    }
                    result
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.object_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.object_store.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.object_store.rename_opts(from, to, options).await
    }
}

/// Where a read (of the object head or of a single part) was served from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadResultSource {
    /// Served from the local disk cache.
    Disk,
    /// Fetched from Object Store.
    Upstream,
}

/// The head metadata returned by [`CachedObjectStore::maybe_prefetch_range`],
/// plus where the head was served from (`Disk` = warm, `Upstream` = cold
/// prefetch).
struct PrefetchedHead {
    meta: ObjectMeta,
    attributes: Attributes,
    extensions: Extensions,
    head_source: ReadResultSource,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidGetRange {
    #[error("Range start too large, requested: {requested}, length: {length}")]
    StartTooLarge { requested: u64, length: u64 },

    #[error("Range started at {start} and ended at {end}")]
    Inconsistent { start: u64, end: u64 },
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
/// A mirror of [`object_store::GetRange`] that implements [`Hash`] and [`Eq`],
/// allowing it to be used as a key in hash-based collections (e.g. `SingleFlight`).
enum GetRangeKey {
    Bounded(Range<u64>),
    Offset(u64),
    Suffix(u64),
}

impl From<GetRange> for GetRangeKey {
    fn from(range: GetRange) -> Self {
        match range {
            GetRange::Bounded(r) => GetRangeKey::Bounded(r),
            GetRange::Offset(o) => GetRangeKey::Offset(o),
            GetRange::Suffix(s) => GetRangeKey::Suffix(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload, path::Path};
    use std::sync::Arc;

    use super::CachedObjectStore;
    use crate::storage::part_cache::stats::CachedObjectStoreStats;
    use crate::storage::part_cache::storage::{LocalCacheStorage, PartID};
    use crate::storage::part_cache::storage_fs::{FsCacheEntry, FsCacheStorage};
    use crate::storage::part_cache::test_util::{
        ExtensionMarker, ExtensionObjectStore, gen_rand_bytes,
    };

    fn new_test_cache_folder() -> std::path::PathBuf {
        use rand::Rng;
        let mut rng = rand::rng();
        let dir_name: String = (0..10)
            .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
            .collect();
        let path = format!("/tmp/miscreant-partcache-test-{}", dir_name);
        let _ = std::fs::remove_dir_all(&path);
        std::path::PathBuf::from(path)
    }

    fn new_cached_store(object_store: Arc<dyn ObjectStore>) -> Arc<CachedObjectStore> {
        new_cached_store_with_puts(object_store, false)
    }

    fn new_cached_store_with_puts(
        object_store: Arc<dyn ObjectStore>,
        cache_puts: bool,
    ) -> Arc<CachedObjectStore> {
        let stats = Arc::new(CachedObjectStoreStats::new());
        let cache_storage = Arc::new(FsCacheStorage::new(
            new_test_cache_folder(),
            None,
            None,
            stats.clone(),
            1000,
        ));
        CachedObjectStore::new(
            object_store,
            cache_storage as Arc<dyn LocalCacheStorage>,
            1024,
            cache_puts,
            stats,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn should_save_unaligned_result_as_parts() -> object_store::Result<()> {
        // given
        let payload = gen_rand_bytes(1024 * 3 + 32);
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let stats = Arc::new(CachedObjectStoreStats::new());
        object_store
            .put(
                &Path::from("/data/testfile1"),
                PutPayload::from_bytes(payload.clone()),
            )
            .await?;
        let location = Path::from("/data/testfile1");
        let get_result = object_store.get(&location).await?;

        let cache_storage = Arc::new(FsCacheStorage::new(
            test_cache_folder.clone(),
            None,
            None,
            stats.clone(),
            1000,
        ));

        let part_size = 1024;
        let cached_store = CachedObjectStore::new(
            object_store.clone(),
            cache_storage as Arc<dyn LocalCacheStorage>,
            part_size,
            false,
            stats,
        )
        .unwrap();
        let entry = cached_store.cache_storage.entry(&location, 1024);

        // when
        let object_size_hint = cached_store.save_get_result(&location, get_result).await?;

        // then
        assert_eq!(object_size_hint, 1024 * 3 + 32);

        // assert the cached meta
        let head = entry.read_head().await?;
        assert_eq!(head.unwrap().0.size, 1024 * 3 + 32);

        // assert the parts
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 4);
        assert_eq!(
            entry.read_part(0, 0..part_size).await?,
            Some(payload.slice(0..1024))
        );
        assert_eq!(
            entry.read_part(1, 0..part_size).await?,
            Some(payload.slice(1024..2048))
        );
        assert_eq!(
            entry.read_part(2, 0..part_size).await?,
            Some(payload.slice(2048..3072))
        );
        // check that the unaligned part was also cached
        assert_eq!(
            entry.read_part(3, 0..32).await?,
            Some(payload.slice(3072..3104))
        );

        // delete part 2, known_cache_size is still known
        let evict_part_path =
            FsCacheEntry::make_part_path(test_cache_folder.clone(), &location, 2, 1024);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2, 0..part_size).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1, 3]);

        // delete part 3, known_cache_size become None
        let evict_part_path =
            FsCacheEntry::make_part_path(test_cache_folder.clone(), &location, 3, 1024);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(3, 0..part_size).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn should_save_aligned_result_as_parts() -> object_store::Result<()> {
        // given
        let payload = gen_rand_bytes(1024 * 3);
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let stats = Arc::new(CachedObjectStoreStats::new());
        object_store
            .put(
                &Path::from("/data/testfile1"),
                PutPayload::from_bytes(payload.clone()),
            )
            .await?;
        let location = Path::from("/data/testfile1");
        let get_result = object_store.get(&location).await?;
        let part_size = 1024;

        let cache_storage = Arc::new(FsCacheStorage::new(
            test_cache_folder.clone(),
            None,
            None,
            stats.clone(),
            1000,
        ));

        let cached_store = CachedObjectStore::new(
            object_store,
            cache_storage as Arc<dyn LocalCacheStorage>,
            part_size,
            false,
            stats,
        )
        .unwrap();
        let entry = cached_store.cache_storage.entry(&location, part_size);

        // when
        let object_size_hint = cached_store.save_get_result(&location, get_result).await?;

        // then
        assert_eq!(object_size_hint, 1024 * 3);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 3);
        assert_eq!(
            entry.read_part(0, 0..part_size).await?,
            Some(payload.slice(0..1024))
        );
        assert_eq!(
            entry.read_part(1, 0..part_size).await?,
            Some(payload.slice(1024..2048))
        );
        assert_eq!(
            entry.read_part(2, 0..part_size).await?,
            Some(payload.slice(2048..3072))
        );

        let evict_part_path =
            FsCacheEntry::make_part_path(test_cache_folder.clone(), &location, 2, part_size);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2, 0..part_size).await?, None);

        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn should_preserve_extensions_on_get_cache_miss() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let location = Path::from("/data/test_extensions_get");
        inner
            .put(
                &location,
                PutPayload::from_bytes(bytes::Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();

        let marking: Arc<dyn ObjectStore> = Arc::new(ExtensionObjectStore::new(inner));
        let cached_store = new_cached_store(marking);

        // when
        let result = cached_store
            .cached_get_opts(
                &location,
                GetOptions {
                    range: Some(GetRange::Bounded(0..5)),
                    ..Default::default()
                },
                false,
            )
            .await
            .expect("cache miss should fetch from inner store");

        // then
        assert!(result.extensions.get::<ExtensionMarker>().is_some());
        assert_eq!(
            result.bytes().await.unwrap(),
            bytes::Bytes::from_static(b"hello")
        );
    }

    #[tokio::test]
    async fn should_preserve_extensions_on_head_cache_miss() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let location = Path::from("/data/test_extensions_head");
        inner
            .put(
                &location,
                PutPayload::from_bytes(bytes::Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();

        let marking: Arc<dyn ObjectStore> = Arc::new(ExtensionObjectStore::new(inner));
        let cached_store = new_cached_store(marking);

        // when
        let result = cached_store
            .cached_head(&location, true)
            .await
            .expect("cache miss should fetch head from inner store");

        // then
        assert!(result.extensions.get::<ExtensionMarker>().is_some());
    }

    #[test]
    fn should_split_range_into_parts() {
        // given
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached_store = new_cached_store(object_store);

        struct Test {
            input: (Option<GetRange>, usize),
            expect: Vec<(PartID, std::ops::Range<usize>)>,
        }
        let tests = [
            Test {
                input: (None, 1024 * 3),
                expect: vec![(0, 0..1024), (1, 0..1024), (2, 0..1024)],
            },
            Test {
                input: (None, 1024 * 3 + 12),
                expect: vec![(0, 0..1024), (1, 0..1024), (2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (None, 12),
                expect: vec![(0, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(0..1024)), 1024),
                expect: vec![(0, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024)), 20000),
                expect: vec![(0, 128..1024)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024 + 12)), 20000),
                expect: vec![(0, 128..1024), (1, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024 * 2 + 12)), 20000),
                expect: vec![(0, 128..1024), (1, 0..1024), (2, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(1024 * 2..1024 * 3 + 12)), 200000),
                expect: vec![(2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(1024 * 2 - 2..1024 * 3 + 12)), 20000),
                expect: vec![(1, 1022..1024), (2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (Some(GetRange::Suffix(128)), 1024),
                expect: vec![(0, 896..1024)],
            },
            Test {
                input: (Some(GetRange::Suffix(1024 * 2 + 8)), 1024 * 4),
                expect: vec![(1, 1016..1024), (2, 0..1024), (3, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Offset(8)), 1024 * 4),
                expect: vec![(0, 8..1024), (1, 0..1024), (2, 0..1024), (3, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Offset(1024 * 2 + 8)), 1024 * 4),
                expect: vec![(2, 8..1024), (3, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Offset(1024 * 2 + 8)), 1024 * 4 + 2),
                expect: vec![(2, 8..1024), (3, 0..1024), (4, 0..2)],
            },
        ];

        // when/then
        for t in tests.iter() {
            let range = cached_store
                .canonicalize_range(t.input.0.clone(), t.input.1 as u64)
                .unwrap();
            let parts = cached_store.split_range_into_parts(range);
            assert_eq!(parts, t.expect, "input: {:?}", t.input);
        }
    }

    #[test]
    fn should_align_range() {
        // given
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached_store = new_cached_store(object_store);

        // when/then
        let aligned = cached_store.align_range(&(9..1025), 1024);
        assert_eq!(aligned, 0..2048);
        let aligned = cached_store.align_range(&(1024 + 1..2048 + 4), 1024);
        assert_eq!(aligned, 1024..3072);
    }

    #[test]
    fn should_align_get_range() {
        // given
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached_store = new_cached_store(object_store);

        // when/then
        let aligned = cached_store.align_get_range(&GetRange::Bounded(9..1025));
        assert_eq!(aligned, GetRange::Bounded(0..2048));
        let aligned = cached_store.align_get_range(&GetRange::Bounded(9..2048));
        assert_eq!(aligned, GetRange::Bounded(0..2048));
        let aligned = cached_store.align_get_range(&GetRange::Suffix(12));
        assert_eq!(aligned, GetRange::Suffix(1024));
        let aligned = cached_store.align_get_range(&GetRange::Suffix(1024));
        assert_eq!(aligned, GetRange::Suffix(1024));
        let aligned = cached_store.align_get_range(&GetRange::Offset(1024));
        assert_eq!(aligned, GetRange::Offset(1024));
        let aligned = cached_store.align_get_range(&GetRange::Offset(12));
        assert_eq!(aligned, GetRange::Offset(0));
    }

    #[tokio::test]
    async fn should_match_inner_store_for_all_ranges() -> object_store::Result<()> {
        // given
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached_store = new_cached_store(object_store.clone());

        let test_path = Path::from("/data/testdata1");
        let test_payload = gen_rand_bytes(1024 * 3 + 2);
        object_store
            .put(&test_path, PutPayload::from_bytes(test_payload.clone()))
            .await?;

        let test_ranges = vec![
            Some(GetRange::Offset(260817)),
            None,
            Some(GetRange::Bounded(1000..2048)),
            Some(GetRange::Bounded(1000..260817)),
            Some(GetRange::Suffix(10)),
            Some(GetRange::Suffix(260817)),
            Some(GetRange::Offset(1000)),
            Some(GetRange::Offset(0)),
            Some(GetRange::Offset(1028)),
            Some(GetRange::Offset(260817)),
            Some(GetRange::Offset(1024 * 3 + 2)),
            Some(GetRange::Offset(1024 * 3 + 1)),
            #[allow(clippy::reversed_empty_ranges)]
            Some(GetRange::Bounded(2900..2048)),
            Some(GetRange::Bounded(10..10)),
        ];

        // when/then: the cached store matches the inner store for every range,
        // byte for byte, including across part boundaries.
        for range in test_ranges.iter() {
            let want = object_store
                .get_opts(
                    &test_path,
                    GetOptions {
                        range: range.clone(),
                        ..Default::default()
                    },
                )
                .await;
            let got = cached_store
                .cached_get_opts(
                    &test_path,
                    GetOptions {
                        range: range.clone(),
                        ..Default::default()
                    },
                    false,
                )
                .await;
            match (want, got) {
                (Ok(want), Ok(got)) => {
                    assert_eq!(want.range, got.range);
                    assert_eq!(want.meta, got.meta);
                    assert_eq!(want.bytes().await?, got.bytes().await?);
                }
                (Err(want), Err(got)) => {
                    if want.to_string().to_lowercase().contains("range") {
                        assert!(got.to_string().to_lowercase().contains("range"));
                    }
                }
                (origin_result, cached_result) => {
                    panic!("expect: {:?}, got: {:?}", origin_result, cached_result);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn should_serve_repeat_read_from_cache_without_touching_inner() {
        // given: an object read once through the cache (admitted on the miss)
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached_store = new_cached_store(object_store.clone());
        let location = Path::from("/data/blob");
        let payload = gen_rand_bytes(1024 * 2 + 7);
        object_store
            .put(&location, PutPayload::from_bytes(payload.clone()))
            .await
            .unwrap();
        let first = cached_store
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(first, payload);

        // when: the object is removed from the inner store
        object_store.delete(&location).await.unwrap();

        // then: a second read is served entirely from the disk cache
        let second = cached_store
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(second, payload);
    }

    #[tokio::test]
    async fn should_invalidate_cache_entry_on_delete() {
        // given: an object cached via a read
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached_store = new_cached_store(object_store.clone());
        let location = Path::from("/data/blob");
        let payload = gen_rand_bytes(1024 * 3);
        object_store
            .put(&location, PutPayload::from_bytes(payload.clone()))
            .await
            .unwrap();
        cached_store
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let entry = cached_store.cache_storage.entry(&location, 1024);
        assert_eq!(entry.cached_parts().await.unwrap().len(), 3);

        // when
        cached_store.delete(&location).await.unwrap();

        // then: the cached parts are gone
        let entry = cached_store.cache_storage.entry(&location, 1024);
        assert_eq!(entry.cached_parts().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn should_cache_put_payload_only_when_cache_puts_enabled() {
        // given: caching a put with cache_puts on
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cached = new_cached_store_with_puts(object_store.clone(), true);
        let location = Path::from("/data/put_cached");
        let payload = gen_rand_bytes(2048);

        // when
        cached
            .put(&location, PutPayload::from_bytes(payload.clone()))
            .await
            .unwrap();

        // then: the payload is cached as parts
        let entry = cached.cache_storage.entry(&location, 1024);
        assert_eq!(entry.cached_parts().await.unwrap().len(), 2);

        // given: caching disabled (miscreant's configuration)
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let uncached = new_cached_store_with_puts(object_store.clone(), false);

        // when
        uncached
            .put(&location, PutPayload::from_bytes(payload.clone()))
            .await
            .unwrap();

        // then: nothing is cached on the put
        let entry = uncached.cache_storage.entry(&location, 1024);
        assert_eq!(entry.cached_parts().await.unwrap().len(), 0);
    }
}
