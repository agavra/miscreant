//! Git wire-format machinery built on the `gix-*` plumbing crates.

pub mod ingest;
pub mod promote;
pub mod walk;

pub use ingest::{IngestError, StagedPack, ingest_pack};
pub use promote::{PromoteError, Promotion, validate_and_promote};
