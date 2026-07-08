//! Object-storage-backed content store for offloaded blobs.
//!
//! See `docs/0001-init.md` §Overview (the inline-threshold split) and §Git
//! Object Storage: blob content over the threshold lives under
//! `blobs/<xx>/<rest>` (the first two hex characters of the object id, then
//! the remainder — mirroring git's loose-object fanout) in the same root
//! object store that hosts SlateDB under a sibling `slatedb/` prefix.

use std::sync::Arc;

use bytes::Bytes;
use gix_hash::oid;
use slatedb::object_store::path::Path;
use slatedb::object_store::{self, ObjectStore, ObjectStoreExt, PutPayload};

/// Prefix under the root object store where offloaded blob content lives.
const BLOBS_PREFIX: &str = "blobs";

/// Errors returned by [`BlobStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum BlobStoreError {
    /// The underlying object store operation failed.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

/// Content-addressed store for blob bytes offloaded from SlateDB, keyed by
/// object id under `blobs/<xx>/<rest>`.
#[derive(Clone)]
pub struct BlobStore {
    store: Arc<dyn ObjectStore>,
}

impl BlobStore {
    /// Wrap the root object store for blob storage under its `blobs/`
    /// prefix. `root_store` is the same handle SlateDB is layered on (see
    /// [`crate::storage::store::Store::object_store`]), so both live in one
    /// bucket.
    pub fn new(root_store: Arc<dyn ObjectStore>) -> Self {
        Self { store: root_store }
    }

    /// Write `bytes` under `oid`'s fanout path, overwriting any existing
    /// content. Callers only ever write a blob whose `oid` is the hash of
    /// `bytes`, so a rewrite is always a no-op in content terms.
    pub async fn put(&self, oid: &oid, bytes: Bytes) -> Result<(), BlobStoreError> {
        let len = bytes.len() as u64;
        self.store
            .put(&blob_path(oid), PutPayload::from(bytes))
            .await?;
        metrics::counter!("blobstore_operations_total", "op" => "put").increment(1);
        metrics::counter!("blobstore_bytes_total", "op" => "put").increment(len);
        Ok(())
    }

    /// Read the bytes stored at `oid`'s fanout path.
    pub async fn get(&self, oid: &oid) -> Result<Bytes, BlobStoreError> {
        let result = self.store.get(&blob_path(oid)).await?;
        let bytes = result.bytes().await?;
        metrics::counter!("blobstore_operations_total", "op" => "get").increment(1);
        metrics::counter!("blobstore_bytes_total", "op" => "get").increment(bytes.len() as u64);
        Ok(bytes)
    }

    /// Whether content exists at `oid`'s fanout path.
    pub async fn exists(&self, oid: &oid) -> Result<bool, BlobStoreError> {
        match self.store.head(&blob_path(oid)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }
}

/// Build the `blobs/<xx>/<rest>` fanout path for `oid`.
fn blob_path(oid: &oid) -> Path {
    let hex = oid.to_hex().to_string();
    let (fanout, rest) = hex.split_at(2);
    Path::from(format!("{BLOBS_PREFIX}/{fanout}/{rest}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_store() -> BlobStore {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        BlobStore::new(store)
    }

    fn oid(hex_byte: u8) -> gix_hash::ObjectId {
        // A deterministic distinct SHA-1 oid: 40 hex chars, all the same
        // nibble pair derived from `hex_byte` (which must itself be a hex
        // character).
        let s = vec![hex_byte; 40];
        gix_hash::ObjectId::from_hex(&s).expect("valid sha1 hex")
    }

    #[test]
    fn should_derive_fanout_path_from_first_two_hex_characters() {
        // given
        let id = oid(b'a');

        // when
        let path = blob_path(&id);

        // then
        let hex = id.to_hex().to_string();
        assert_eq!(
            path,
            Path::from(format!("blobs/{}/{}", &hex[..2], &hex[2..]))
        );
    }

    #[tokio::test]
    async fn should_round_trip_put_and_get() {
        // given
        let store = memory_store();
        let id = oid(b'b');
        let content = Bytes::from_static(b"large blob content");

        // when
        store.put(&id, content.clone()).await.expect("put");

        // then
        assert_eq!(store.get(&id).await.expect("get"), content);
        assert!(store.exists(&id).await.expect("exists"));
    }

    #[tokio::test]
    async fn should_report_missing_blob_as_not_existing() {
        // given
        let store = memory_store();
        let id = oid(b'c');

        // when/then
        assert!(!store.exists(&id).await.expect("exists"));
    }

    #[tokio::test]
    async fn should_treat_repeated_put_as_overwrite_of_identical_content() {
        // given
        let store = memory_store();
        let id = oid(b'd');
        let content = Bytes::from_static(b"content");

        // when
        // Content-addressing guarantees a second put of the same oid carries
        // identical bytes; the store still allows the write (idempotent
        // no-op skipping happens one layer up, in ObjectDb).
        store.put(&id, content.clone()).await.expect("first put");
        store.put(&id, content.clone()).await.expect("second put");

        // then
        assert_eq!(store.get(&id).await.expect("get"), content);
    }
}
