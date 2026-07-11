// Adapted from SlateDB `src/cached_object_store/stats.rs` (Apache-2.0). See the
// module comment in `mod.rs` for provenance.
//
// Adaptation: upstream registers handles through SlateDB's `MetricsRecorderHelper`.
// Miscreant routes the same signals through the `metrics` facade instead, and
// records part accesses as a single `part_cache_access_total{result="hit"|"miss"}`
// counter plus a matching `part_cache_bytes_total{result=...}` byte counter (the
// validation signal for the offloaded-blob benchmark).

use std::fmt::{Debug, Formatter};

use metrics::{Counter, Gauge};

/// Part access counter, labelled `result="hit"|"miss"`.
pub const ACCESS_TOTAL: &str = "part_cache_access_total";
/// Bytes served per access, labelled `result="hit"|"miss"`.
pub const BYTES_TOTAL: &str = "part_cache_bytes_total";
/// Number of files currently tracked in the cache directory.
pub const SIZE_KEYS: &str = "part_cache_size_keys";
/// Current on-disk cache size in bytes.
pub const SIZE_BYTES: &str = "part_cache_size_bytes";
/// Files evicted to stay under the byte budget.
pub const EVICTED_KEYS: &str = "part_cache_evicted_keys_total";
/// Bytes evicted to stay under the byte budget.
pub const EVICTED_BYTES: &str = "part_cache_evicted_bytes_total";

#[derive(Clone)]
pub struct CachedObjectStoreStats {
    pub(super) object_store_cache_keys: Gauge,
    pub(super) object_store_cache_bytes: Gauge,
    pub(super) object_store_cache_evicted_keys: Counter,
    pub(super) object_store_cache_evicted_bytes: Counter,
}

impl Debug for CachedObjectStoreStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedObjectStoreStats").finish()
    }
}

impl Default for CachedObjectStoreStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CachedObjectStoreStats {
    pub(crate) fn new() -> Self {
        Self {
            object_store_cache_keys: metrics::gauge!(SIZE_KEYS),
            object_store_cache_bytes: metrics::gauge!(SIZE_BYTES),
            object_store_cache_evicted_keys: metrics::counter!(EVICTED_KEYS),
            object_store_cache_evicted_bytes: metrics::counter!(EVICTED_BYTES),
        }
    }

    /// Record one part access, classifying it as a cache hit or a miss and
    /// counting the bytes served.
    pub(super) fn record_part_access(&self, hit: bool, bytes: usize) {
        let result = if hit { "hit" } else { "miss" };
        metrics::counter!(ACCESS_TOTAL, "result" => result).increment(1);
        metrics::counter!(BYTES_TOTAL, "result" => result).increment(bytes as u64);
    }
}
