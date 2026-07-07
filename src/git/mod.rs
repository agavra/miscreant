//! Git wire-format machinery built on the `gix-*` plumbing crates.

pub mod ingest;

pub use ingest::{IngestError, StagedPack, ingest_pack};
