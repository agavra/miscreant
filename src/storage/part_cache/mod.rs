//! Disk part cache for the object store: an [`ObjectStore`](object_store::ObjectStore)
//! wrapper that serves reads from fixed-size part files on local disk, fetching
//! missing parts from the wrapped store and admitting them on the read-miss.
//! One instance is shared by SlateDB's SST reads and by the offloaded-blob
//! reads that go straight to the root store, under a single disk budget.
//!
//! This module is vendored from SlateDB's `src/cached_object_store/`
//! (Apache-2.0), upstream commit `b140de0b`, at the time miscreant's own copy
//! is the only way to reuse it: SlateDB's `CachedObjectStore` is `pub(crate)`
//! in the published 0.14.1 crate. The vendored files are kept as close to
//! upstream as practical so the diff stays reviewable; the per-file headers
//! note each adaptation (error type, metrics facade, clock/rand, import paths,
//! and dropping the `ObjectStoreCallTag` policy layer that 0.14.1 does not
//! feed). SlateDB is Apache-2.0; see `NOTICE` for attribution.
//!
//! TODO: replace this vendored module with SlateDB's user-constructible
//! `CachedObjectStore` once RFC 0027 ships (expected slatedb 0.15.0).

pub(crate) use cached_store::CachedObjectStore;
pub use cached_store::PartCacheError;
pub(crate) use stats::CachedObjectStoreStats;
pub(crate) use storage_fs::FsCacheStorage;

pub mod stats;

mod cached_store;
mod single_flight;
mod storage;
mod storage_fs;

#[cfg(test)]
mod test_util;
