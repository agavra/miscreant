pub mod config;
pub mod error;
pub mod git;
pub mod protocol;
pub mod storage;

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;

pub use crate::config::Config;
use crate::storage::{BlobStore, ObjectDb, Store, StoreError};

/// Shared state handed to every request handler.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Store,
    pub objectdb: ObjectDb,
}

impl AppState {
    /// Build application state, opening the store backing `config.storage_url`
    /// (relative `file` URLs are resolved to absolute paths first). The object
    /// database layers blob offload over the store's root object store.
    pub async fn new(config: Config) -> Result<Self, StoreError> {
        let storage_url = config::normalize_storage_url(&config.storage_url)?;
        let store = Store::open(&storage_url).await?;
        let blobs = BlobStore::new(store.object_store());
        let objectdb = ObjectDb::new(store.clone(), blobs, config.inline_threshold);
        Ok(Self {
            config: Arc::new(config),
            store,
            objectdb,
        })
    }
}

/// Build the axum application router.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
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
/// being sent after this fires.
async fn access_log(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let start = Instant::now();
    let response = next.run(request).await;
    tracing::debug!(
        target: "access",
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        elapsed_to_start_ms = start.elapsed().as_millis() as u64,
        "request"
    );
    response
}

async fn healthz() -> &'static str {
    "ok"
}
