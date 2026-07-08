pub mod config;
pub mod error;
pub mod git;
pub mod protocol;
pub mod storage;

use std::sync::Arc;

use axum::Router;
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
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}
