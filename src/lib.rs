pub mod config;
pub mod error;
pub mod git;
pub mod protocol;
pub mod storage;
pub mod telemetry;

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub use crate::config::Config;
use crate::storage::{BlobStore, ObjectDb, Store, StoreError};

/// Shared state handed to every request handler.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Store,
    pub objectdb: ObjectDb,
    /// Renders the `GET /metrics` response. Reflects live data only when it
    /// is the handle of the process's globally installed recorder (see
    /// [`AppState::with_metrics`]); otherwise it is a private, never-written
    /// registry that always renders empty — harmless for embedding, tests,
    /// and the offline `rebuild-graph` subcommand, none of which need a real
    /// scrape endpoint.
    pub metrics: PrometheusHandle,
}

impl AppState {
    /// Build application state, opening the store backing `config.storage_url`
    /// (relative `file` URLs are resolved to absolute paths first). The object
    /// database layers blob offload over the store's root object store.
    /// `/metrics` renders from a private, uninstalled recorder — use
    /// [`AppState::with_metrics`] to serve the process's real metrics.
    pub async fn new(config: Config) -> Result<Self, StoreError> {
        let metrics = PrometheusBuilder::new().build_recorder().handle();
        Self::with_metrics(config, metrics).await
    }

    /// Like [`AppState::new`], but `/metrics` renders from `metrics` — the
    /// handle of an installed recorder that the `metrics` facade macros
    /// actually publish into (see `main.rs`, which is the only place that
    /// installs a recorder globally).
    pub async fn with_metrics(
        config: Config,
        metrics: PrometheusHandle,
    ) -> Result<Self, StoreError> {
        let storage_url = config::normalize_storage_url(&config.storage_url)?;
        let store = Store::open(&storage_url).await?;
        let blobs = BlobStore::new(store.object_store());
        let objectdb = ObjectDb::new(store.clone(), blobs, config.inline_threshold);
        Ok(Self {
            config: Arc::new(config),
            store,
            objectdb,
            metrics,
        })
    }
}

/// Build the axum application router.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        // A repository path has arbitrary depth (`org/repo`), so the endpoint
        // suffix is matched inside the handler: a catch-all must be the final
        // path segment.
        .route(
            "/{*path}",
            get(protocol::http::info_refs).post(protocol::http::git_rpc),
        )
        .layer(middleware::from_fn(access_log))
        .with_state(state)
}

/// One access-log event per request under the dedicated `"access"` target so
/// operators enable it in isolation (`RUST_LOG=info,access=debug`) without
/// turning on module debug noise. `elapsed_to_start_ms` measures until the
/// handler returns the response head; a streamed body (a fetch pack) is still
/// being sent after this fires. The same timing feeds the HTTP request
/// metrics (`http_requests_total`, `http_request_duration_seconds`,
/// `http_requests_in_flight`) for the four git RPC endpoints; `/metrics` and
/// `/healthz` requests are logged here but excluded from those metrics.
async fn access_log(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().map(str::to_owned);
    let endpoint = protocol::http::classify_endpoint(&path, query.as_deref());
    let _in_flight = endpoint.map(InFlightGuard::new);

    let start = Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed();

    if let Some(endpoint) = endpoint {
        let status = response.status().as_u16().to_string();
        metrics::counter!("http_requests_total", "endpoint" => endpoint, "status" => status)
            .increment(1);
        metrics::histogram!("http_request_duration_seconds", "endpoint" => endpoint)
            .record(elapsed.as_secs_f64());
    }

    tracing::debug!(
        target: "access",
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        elapsed_to_start_ms = elapsed.as_millis() as u64,
        "request"
    );
    response
}

/// Holds `http_requests_in_flight{endpoint}` incremented for the lifetime of
/// one request. A plain `Drop` impl decrements on every exit path — normal
/// return, an early `return` in a handler, or a panic unwinding through the
/// middleware — so the gauge cannot leak above zero.
struct InFlightGuard {
    endpoint: &'static str,
}

impl InFlightGuard {
    fn new(endpoint: &'static str) -> Self {
        metrics::gauge!("http_requests_in_flight", "endpoint" => endpoint).increment(1.0);
        Self { endpoint }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        metrics::gauge!("http_requests_in_flight", "endpoint" => self.endpoint).decrement(1.0);
    }
}

async fn healthz() -> &'static str {
    "ok"
}

/// `GET /metrics` — a Prometheus text-exposition scrape of every metric this
/// process has recorded through `state.metrics`'s registry.
async fn metrics_endpoint(State(state): State<AppState>) -> Response {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4"),
        )],
        state.metrics.render(),
    )
        .into_response()
}
