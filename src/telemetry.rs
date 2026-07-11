//! Central catalog of the metrics this server records: names, units, and
//! descriptions in one place, plus the byte-histogram bucket configuration
//! the Prometheus exporter needs at build time.
//!
//! See `docs/0001-init.md` for the request paths these metrics cover. Every
//! recording call site lives with the code it measures (the access-log
//! middleware, the push/fetch handlers, the storage layer); this module only
//! holds what must be centralized: the `describe_*` registrations (so a
//! scrape carries `# HELP`/`# TYPE` lines even before the first observation)
//! and the exponential byte buckets shared by the pack-size histograms.
//!
//! The `metrics` facade macros (`counter!`, `histogram!`, `gauge!`) route to
//! whichever recorder is installed globally, or silently do nothing when
//! none is: library code never needs to check whether a recorder exists.

use metrics::Unit;
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder};

/// Exponential byte-histogram buckets from 1 KiB to 1 GiB (factor of 4 per
/// step), for the histograms that measure pack sizes rather than durations —
/// far outside the exporter's default (sub-second latency) bucket set.
pub const BYTE_BUCKETS: &[f64] = &[
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262144.0,
    1048576.0,
    4194304.0,
    16777216.0,
    67108864.0,
    268435456.0,
    1073741824.0,
];

/// Histograms measuring bytes, which need [`BYTE_BUCKETS`] instead of the
/// exporter's duration-oriented defaults.
const BYTE_HISTOGRAMS: &[&str] = &["push_pack_bytes", "fetch_pack_bytes"];

/// Apply [`BYTE_BUCKETS`] to every histogram in [`BYTE_HISTOGRAMS`]. Must run
/// before the builder is installed or built: bucket configuration is fixed
/// at recorder construction.
pub fn configure_byte_buckets(
    mut builder: PrometheusBuilder,
) -> Result<PrometheusBuilder, BuildError> {
    for name in BYTE_HISTOGRAMS {
        builder =
            builder.set_buckets_for_metric(Matcher::Full((*name).to_owned()), BYTE_BUCKETS)?;
    }
    Ok(builder)
}

/// Register a unit and one-line description for every metric this server
/// records. Registrations target whichever recorder is currently installed
/// globally, so this must run after installing the process's recorder (an
/// earlier call would land on the default no-op recorder and be lost).
pub fn describe() {
    metrics::describe_counter!(
        "http_requests_total",
        Unit::Count,
        "HTTP requests to a git endpoint, by endpoint and status (excludes /metrics and /healthz)"
    );
    metrics::describe_histogram!(
        "http_request_duration_seconds",
        Unit::Seconds,
        "time from request start to response head, by endpoint"
    );
    metrics::describe_gauge!(
        "http_requests_in_flight",
        Unit::Count,
        "requests to a git endpoint currently being served, by endpoint"
    );

    metrics::describe_counter!(
        "push_total",
        Unit::Count,
        "git-receive-pack requests by overall outcome"
    );
    metrics::describe_counter!(
        "push_ref_updates_total",
        Unit::Count,
        "per-ref compare-and-swap outcomes across all pushes"
    );
    metrics::describe_histogram!(
        "push_pack_bytes",
        Unit::Bytes,
        "size of the pack received in a push request"
    );
    metrics::describe_histogram!(
        "ingest_duration_seconds",
        Unit::Seconds,
        "time spent indexing a received pack into staging"
    );
    metrics::describe_histogram!(
        "promote_duration_seconds",
        Unit::Seconds,
        "time spent validating and promoting a staged pack"
    );
    metrics::describe_counter!(
        "objects_promoted_total",
        Unit::Count,
        "objects newly written to committed storage by a push, by kind"
    );

    metrics::describe_counter!(
        "upload_pack_commands_total",
        Unit::Count,
        "git-upload-pack protocol-v2 commands dispatched, by command"
    );
    metrics::describe_counter!(
        "fetch_total",
        Unit::Count,
        "fetch command requests by outcome"
    );
    metrics::describe_histogram!(
        "fetch_objects_packed",
        Unit::Count,
        "objects packed per served fetch"
    );
    metrics::describe_histogram!(
        "fetch_pack_bytes",
        Unit::Bytes,
        "bytes of pack data actually streamed per served fetch"
    );
    metrics::describe_histogram!(
        "fetch_stream_seconds",
        Unit::Seconds,
        "wall-clock time writing and streaming the pack per served fetch"
    );
    metrics::describe_histogram!(
        "fetch_stream_input_wait_seconds",
        Unit::Seconds,
        "time the pack writer spent blocked waiting for input objects per served fetch"
    );
    metrics::describe_histogram!(
        "fetch_stream_output_wait_seconds",
        Unit::Seconds,
        "time the pack writer spent blocked on the response body channel per served fetch"
    );
    metrics::describe_histogram!(
        "fetch_object_read_seconds",
        Unit::Seconds,
        "time to read one object's stored stream for packing, by source"
    );

    metrics::describe_counter!(
        "store_cas_retries_total",
        Unit::Count,
        "ref compare-and-swap transactions retried after a conflict"
    );
    metrics::describe_counter!(
        "store_cas_exhausted_total",
        Unit::Count,
        "ref compare-and-swap batches that exhausted their retry budget"
    );
    metrics::describe_counter!(
        "repo_auto_created_total",
        Unit::Count,
        "repositories created on first push"
    );
    metrics::describe_counter!(
        "blobstore_operations_total",
        Unit::Count,
        "offloaded-blob object-storage operations, by op"
    );
    metrics::describe_counter!(
        "blobstore_bytes_total",
        Unit::Bytes,
        "bytes transferred to/from offloaded-blob object storage, by op"
    );
    metrics::describe_counter!(
        "commit_graph_backfills_total",
        Unit::Count,
        "lazy commit-graph backfills triggered while resolving a fetch or push"
    );

    metrics::describe_counter!(
        "part_cache_access_total",
        Unit::Count,
        "disk part-cache part accesses, by result (hit|miss)"
    );
    metrics::describe_counter!(
        "part_cache_bytes_total",
        Unit::Bytes,
        "bytes served per disk part-cache part access, by result (hit|miss)"
    );
    metrics::describe_gauge!(
        "part_cache_size_keys",
        Unit::Count,
        "files currently tracked in the disk part-cache directory"
    );
    metrics::describe_gauge!(
        "part_cache_size_bytes",
        Unit::Bytes,
        "current on-disk size of the disk part cache"
    );
    metrics::describe_counter!(
        "part_cache_evicted_keys_total",
        Unit::Count,
        "disk part-cache files evicted to stay under the byte budget"
    );
    metrics::describe_counter!(
        "part_cache_evicted_bytes_total",
        Unit::Bytes,
        "disk part-cache bytes evicted to stay under the byte budget"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_configure_buckets_for_every_byte_histogram() {
        // given/when
        let builder = configure_byte_buckets(PrometheusBuilder::new()).expect("configure buckets");

        // then: the builder accepted every byte histogram without error, and
        // the handle it produces renders (a compile/type-level check that the
        // returned builder is still usable).
        let _handle = builder.build_recorder().handle();
    }
}
