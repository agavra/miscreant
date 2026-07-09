//! Façade over git object content: small records and blob pointers live in
//! SlateDB via [`Store`]; blob content over the inline threshold lives in
//! object storage via [`BlobStore`].
//!
//! Object content is stored deflated: every inline record and every offloaded
//! blob holds the zlib stream of the object's bare content, computed once here
//! at write time. Reads inflate through this façade; the pack path
//! ([`ObjectDb::read_compressed`]) hands the stored streams straight out
//! without inflating. See `docs/0001-init.md` §Overview (the 64KB
//! inline-threshold split) and §Git Object Storage.

use bytes::Bytes;
use gix_hash::{ObjectId, oid};
use gix_object::Kind;

use crate::storage::blobs::{BlobStore, BlobStoreError};
use crate::storage::keys::RepoId;
use crate::storage::store::{Durability, Store, StoreError};
use crate::storage::values::ObjectRecord;
use crate::storage::zlib;

/// Errors returned by [`ObjectDb`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ObjectDbError {
    /// A SlateDB-backed store operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// An offloaded-blob object-storage operation failed.
    #[error(transparent)]
    Blob(#[from] BlobStoreError),
    /// A stored zlib stream did not inflate back to its recorded content
    /// length. Indicates storage corruption: every stream [`ObjectDb::put`]
    /// writes inflates cleanly.
    #[error("corrupt zlib stream for {oid}")]
    CorruptZlib {
        /// The object id whose stored stream failed to inflate.
        oid: ObjectId,
    },
}

/// Façade over one store's object content: content-addressed, so a `put` of
/// an already-known `oid` is a no-op regardless of the bytes supplied.
#[derive(Clone)]
pub struct ObjectDb {
    store: Store,
    blobs: BlobStore,
    inline_threshold: usize,
    compression_level: u32,
}

impl ObjectDb {
    /// Build a façade over `store`'s object segment and `blobs` for
    /// offloaded blob content. A blob body of exactly `inline_threshold`
    /// bytes (uncompressed) stays inline; anything larger is offloaded to
    /// `blobs`. Content is deflated at `compression_level` (0–9) as it is
    /// written.
    pub fn new(
        store: Store,
        blobs: BlobStore,
        inline_threshold: usize,
        compression_level: u32,
    ) -> Self {
        Self {
            store,
            blobs,
            inline_threshold,
            compression_level,
        }
    }

    /// Store `body` (the object's content, with no git header) as `oid` of
    /// kind `kind`, writing the object record with the requested
    /// [`Durability`]. A no-op if `oid` already has a record. The content is
    /// deflated once here; an offloaded blob's zlib stream finishes uploading
    /// to the blob store (awaited here) before its pointer record is written,
    /// regardless of durability mode — only the SlateDB record write itself is
    /// relaxed. Callers are responsible for `oid` being the hash of the
    /// canonical encoding of `kind`/`body`.
    pub async fn put(
        &self,
        repo: RepoId,
        oid: &oid,
        kind: Kind,
        body: Bytes,
        durability: Durability,
    ) -> Result<(), ObjectDbError> {
        if self.store.object_exists(repo, oid).await? {
            return Ok(());
        }

        let uncompressed_len = body.len() as u64;
        let record = match kind {
            Kind::Blob if body.len() > self.inline_threshold => {
                self.blobs.put(oid, self.compress(&body)).await?;
                ObjectRecord::BlobPointer {
                    size: uncompressed_len,
                }
            }
            Kind::Blob => ObjectRecord::BlobInline {
                uncompressed_len,
                zlib: self.compress(&body),
            },
            Kind::Tree => ObjectRecord::Tree {
                uncompressed_len,
                zlib: self.compress(&body),
            },
            Kind::Commit => ObjectRecord::Commit {
                uncompressed_len,
                zlib: self.compress(&body),
            },
            Kind::Tag => ObjectRecord::Tag {
                uncompressed_len,
                zlib: self.compress(&body),
            },
        };

        self.store
            .put_object(repo, oid, &record, durability)
            .await?;
        Ok(())
    }

    /// Read an object's kind and content bytes (no git header), inflating the
    /// stored zlib stream and fetching offloaded blob content from the blob
    /// store transparently. The single inflate choke point for every read-path
    /// consumer of object content. `None` if the repo has no record for `oid`.
    pub async fn get(
        &self,
        repo: RepoId,
        oid: &oid,
    ) -> Result<Option<(Kind, Bytes)>, ObjectDbError> {
        let Some(record) = self.store.get_object(repo, oid).await? else {
            return Ok(None);
        };
        let kind = record.kind();
        let (uncompressed_len, stream) = match record {
            ObjectRecord::BlobPointer { size } => (size, self.blobs.get(oid).await?),
            ObjectRecord::BlobInline {
                uncompressed_len,
                zlib,
            }
            | ObjectRecord::Tree {
                uncompressed_len,
                zlib,
            }
            | ObjectRecord::Commit {
                uncompressed_len,
                zlib,
            }
            | ObjectRecord::Tag {
                uncompressed_len,
                zlib,
            } => (uncompressed_len, zlib),
        };
        let content =
            zlib::inflate(&stream, uncompressed_len).ok_or_else(|| ObjectDbError::CorruptZlib {
                oid: oid.to_owned(),
            })?;
        Ok(Some((kind, content)))
    }

    /// Read an object's kind, content size, and the zlib stream of its content
    /// — the bytes a pack entry copies verbatim behind its type/size header.
    /// Offloaded blob content comes straight from the blob store (already a
    /// zlib stream); nothing is inflated. `None` if the repo has no record for
    /// `oid`.
    pub async fn read_compressed(
        &self,
        repo: RepoId,
        oid: &oid,
    ) -> Result<Option<(Kind, u64, Bytes)>, ObjectDbError> {
        let Some(record) = self.store.get_object(repo, oid).await? else {
            return Ok(None);
        };
        let kind = record.kind();
        let (uncompressed_len, stream) = match record {
            ObjectRecord::BlobPointer { size } => (size, self.blobs.get(oid).await?),
            ObjectRecord::BlobInline {
                uncompressed_len,
                zlib,
            }
            | ObjectRecord::Tree {
                uncompressed_len,
                zlib,
            }
            | ObjectRecord::Commit {
                uncompressed_len,
                zlib,
            }
            | ObjectRecord::Tag {
                uncompressed_len,
                zlib,
            } => (uncompressed_len, zlib),
        };
        Ok(Some((kind, uncompressed_len, stream)))
    }

    /// The object's kind and content size. Never touches the blob store and
    /// never inflates: every record carries its uncompressed content length.
    /// `None` if the repo has no record for `oid`.
    pub async fn size(
        &self,
        repo: RepoId,
        oid: &oid,
    ) -> Result<Option<(Kind, u64)>, ObjectDbError> {
        let Some(record) = self.store.get_object(repo, oid).await? else {
            return Ok(None);
        };
        Ok(Some((record.kind(), record.size())))
    }

    /// Whether the repo holds an object record for `oid`.
    pub async fn exists(&self, repo: RepoId, oid: &oid) -> Result<bool, ObjectDbError> {
        Ok(self.store.object_exists(repo, oid).await?)
    }

    /// Deflate `content` into a zlib stream at the configured level.
    fn compress(&self, content: &[u8]) -> Bytes {
        Bytes::from(zlib::deflate(content, self.compression_level))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{self, ObjectStore};
    use std::sync::Arc;

    async fn objectdb(inline_threshold: usize) -> (ObjectDb, RepoId) {
        let store = Store::open("memory://").await.expect("open store");
        let repo = store.create_repo("objects").await.expect("create").id;
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let blobs = BlobStore::new(backing);
        (ObjectDb::new(store, blobs, inline_threshold, 6), repo)
    }

    fn oid(hex_byte: u8) -> ObjectId {
        // A deterministic distinct SHA-1 oid: 40 hex chars, all the same
        // nibble pair derived from `hex_byte` (which must itself be a hex
        // character).
        let s = vec![hex_byte; 40];
        ObjectId::from_hex(&s).expect("valid sha1 hex")
    }

    #[tokio::test]
    async fn should_keep_blob_at_threshold_inline() {
        // given
        let (db, repo) = objectdb(8).await;
        let id = oid(b'a');
        let body = Bytes::from_static(b"12345678"); // exactly 8 bytes

        // when
        db.put(repo, &id, Kind::Blob, body.clone(), Durability::Durable)
            .await
            .expect("put");

        // then
        let record = db
            .store
            .get_object(repo, &id)
            .await
            .expect("get record")
            .expect("present");
        assert!(matches!(record, ObjectRecord::BlobInline { .. }));
        assert_eq!(
            db.get(repo, &id).await.expect("get").expect("present"),
            (Kind::Blob, body)
        );
    }

    #[tokio::test]
    async fn should_offload_blob_one_byte_over_threshold() {
        // given
        let (db, repo) = objectdb(8).await;
        let id = oid(b'b');
        let body = Bytes::from_static(b"123456789"); // 9 bytes, threshold + 1

        // when
        db.put(repo, &id, Kind::Blob, body.clone(), Durability::Durable)
            .await
            .expect("put");

        // then
        let record = db
            .store
            .get_object(repo, &id)
            .await
            .expect("get record")
            .expect("present");
        assert_eq!(record, ObjectRecord::BlobPointer { size: 9 });
        assert_eq!(
            db.get(repo, &id).await.expect("get").expect("present"),
            (Kind::Blob, body)
        );
    }

    #[tokio::test]
    async fn should_store_offloaded_blob_content_as_a_zlib_stream() {
        // given: a compressible blob large enough to be offloaded
        let (db, repo) = objectdb(8).await;
        let id = oid(b'a');
        let body = Bytes::from(b"abcabcabc".repeat(64));
        db.put(repo, &id, Kind::Blob, body.clone(), Durability::Durable)
            .await
            .expect("put");

        // when: reading the bytes the blob store actually holds
        let stored = db.blobs.get(&id).await.expect("blob store get");

        // then: they are the zlib stream (smaller than, and not equal to, the
        // content), and inflate back to the original blob
        assert_ne!(stored.as_ref(), body.as_ref());
        assert!(stored.len() < body.len());
        assert_eq!(
            crate::storage::zlib::inflate(&stored, body.len() as u64).expect("inflate"),
            body
        );
    }

    #[tokio::test]
    async fn should_round_trip_every_object_kind() {
        // given
        let (db, repo) = objectdb(65536).await;
        let cases = [
            (oid(b'1'), Kind::Blob, Bytes::from_static(b"blob content")),
            (oid(b'2'), Kind::Tree, Bytes::from_static(b"tree-body")),
            (oid(b'3'), Kind::Commit, Bytes::from_static(b"commit-body")),
            (oid(b'4'), Kind::Tag, Bytes::from_static(b"tag-body")),
        ];

        // when/then: each kind is its own put -> get -> compare cycle.
        for (id, kind, body) in cases {
            db.put(repo, &id, kind, body.clone(), Durability::Durable)
                .await
                .expect("put");
            assert_eq!(
                db.get(repo, &id).await.expect("get").expect("present"),
                (kind, body)
            );
        }
    }

    #[tokio::test]
    async fn should_report_size_for_inline_blob_without_reading_blob_store() {
        // given
        let (db, repo) = objectdb(65536).await;
        let id = oid(b'c');
        let body = Bytes::from_static(b"small");
        db.put(repo, &id, Kind::Blob, body.clone(), Durability::Durable)
            .await
            .expect("put");

        // when
        let size = db.size(repo, &id).await.expect("size").expect("present");

        // then
        assert_eq!(size, (Kind::Blob, body.len() as u64));
    }

    #[tokio::test]
    async fn should_report_size_for_offloaded_blob_from_pointer_payload_only() {
        // given
        let (db, repo) = objectdb(4).await;
        let id = oid(b'd');
        let body = Bytes::from_static(b"this is bigger than four bytes");
        db.put(repo, &id, Kind::Blob, body.clone(), Durability::Durable)
            .await
            .expect("put");

        // when
        let size = db.size(repo, &id).await.expect("size").expect("present");

        // then
        assert_eq!(size, (Kind::Blob, body.len() as u64));
    }

    #[tokio::test]
    async fn should_report_size_for_tree_from_body_length() {
        // given
        let (db, repo) = objectdb(65536).await;
        let id = oid(b'e');
        let body = Bytes::from_static(b"tree-body-bytes");
        db.put(repo, &id, Kind::Tree, body.clone(), Durability::Durable)
            .await
            .expect("put");

        // when
        let size = db.size(repo, &id).await.expect("size").expect("present");

        // then
        assert_eq!(size, (Kind::Tree, body.len() as u64));
    }

    #[tokio::test]
    async fn should_treat_second_put_of_existing_oid_as_no_op() {
        // given
        let (db, repo) = objectdb(65536).await;
        let id = oid(b'f');
        let first = Bytes::from_static(b"first content");
        db.put(repo, &id, Kind::Blob, first.clone(), Durability::Durable)
            .await
            .expect("first put");

        // when
        // A second put under the same oid with different bytes must not
        // overwrite the existing record (content-addressing means callers
        // never legitimately do this, but the no-op guard must still hold).
        let second = Bytes::from_static(b"different content entirely");
        db.put(repo, &id, Kind::Blob, second, Durability::Durable)
            .await
            .expect("noop");

        // then
        assert_eq!(
            db.get(repo, &id).await.expect("get").expect("present"),
            (Kind::Blob, first)
        );
    }

    #[tokio::test]
    async fn should_return_none_for_missing_object() {
        // given
        let (db, repo) = objectdb(65536).await;
        let id = oid(b'9');

        // when/then
        assert_eq!(db.get(repo, &id).await.expect("get"), None);
        assert_eq!(db.size(repo, &id).await.expect("size"), None);
        assert!(!db.exists(repo, &id).await.expect("exists"));
    }
}
