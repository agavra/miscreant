//! Typed encoding/decoding for the SlateDB-backed storage layer.
//!
//! See `docs/0001-init.md` §Storage for the on-disk layout this module
//! implements.

pub mod blobs;
pub mod keys;
pub mod objectdb;
pub mod store;
pub mod values;

pub use blobs::{BlobStore, BlobStoreError};
pub use objectdb::{ObjectDb, ObjectDbError};
pub use store::{RefOutcome, RefUpdate, RefUpdateResult, RepoMeta, Store, StoreError};
