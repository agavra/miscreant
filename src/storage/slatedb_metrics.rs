//! Bridges SlateDB's internal metrics into miscreant's `metrics`-rs facade so
//! the storage engine's own counters (block-cache hits/misses, cache errors,
//! …) appear on the `/metrics` scrape alongside the server's metrics.
//!
//! SlateDB records nothing unless a [`MetricsRecorder`] is attached to its
//! builder (its default recorder is a no-op). This recorder forwards every
//! registration to whichever `metrics`-rs recorder is installed globally; when
//! none is installed (tests, the offline `rebuild-graph` path) the forwarding
//! is itself a no-op. Its metric names and labels are the static kinds SlateDB
//! emits (e.g. `entry_kind`, `result`) and never carry repo names, refs, or
//! oids, so they are forwarded verbatim.

use std::sync::Arc;

use metrics::{Counter, Gauge, Histogram, Key, Label, Level, Metadata};
use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

/// Metadata every forwarded SlateDB metric carries. The target marks the
/// origin so a scrape can be filtered to storage-engine metrics.
const METADATA: Metadata<'static> = Metadata::new("slatedb", Level::INFO, Some(module_path!()));

/// A [`MetricsRecorder`] that forwards SlateDB's metric registrations to the
/// process's globally installed `metrics`-rs recorder.
pub struct SlateDbMetricsBridge;

fn build_key(name: &str, labels: &[(&str, &str)]) -> Key {
    let labels: Vec<Label> = labels
        .iter()
        .map(|(k, v)| Label::new(k.to_string(), v.to_string()))
        .collect();
    Key::from_parts(name.to_string(), labels)
}

impl MetricsRecorder for SlateDbMetricsBridge {
    fn register_counter(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let key = build_key(name, labels);
        let counter = metrics::with_recorder(|rec| rec.register_counter(&key, &METADATA));
        Arc::new(CounterBridge(counter))
    }

    fn register_gauge(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let key = build_key(name, labels);
        let gauge = metrics::with_recorder(|rec| rec.register_gauge(&key, &METADATA));
        Arc::new(GaugeBridge(gauge))
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        // `metrics`-rs has no up/down counter primitive; a gauge tracks the
        // same signed running total.
        let key = build_key(name, labels);
        let gauge = metrics::with_recorder(|rec| rec.register_gauge(&key, &METADATA));
        Arc::new(UpDownCounterBridge(gauge))
    }

    fn register_histogram(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        // Bucket boundaries are fixed when the Prometheus exporter is built
        // (see `telemetry`), so SlateDB's requested boundaries are not applied.
        let key = build_key(name, labels);
        let histogram = metrics::with_recorder(|rec| rec.register_histogram(&key, &METADATA));
        Arc::new(HistogramBridge(histogram))
    }
}

struct CounterBridge(Counter);

impl CounterFn for CounterBridge {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct GaugeBridge(Gauge);

impl GaugeFn for GaugeBridge {
    fn set(&self, value: i64) {
        self.0.set(value as f64);
    }
}

struct UpDownCounterBridge(Gauge);

impl UpDownCounterFn for UpDownCounterBridge {
    fn increment(&self, value: i64) {
        if value >= 0 {
            self.0.increment(value as f64);
        } else {
            self.0.decrement(value.unsigned_abs() as f64);
        }
    }
}

struct HistogramBridge(Histogram);

impl HistogramFn for HistogramBridge {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}
