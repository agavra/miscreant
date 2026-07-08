//! Protocol-boundary error classification.
//!
//! The domain modules (`storage`, `git`) return precise, matchable error
//! enums and know nothing about HTTP or the git wire format. This module is
//! the single place that maps each such error to how it must surface to a
//! client:
//!
//! - **client-caused** — the request itself is at fault (a malformed pack, a
//!   push that is not self-contained, a want the server does not have). The
//!   attached reason is short and safe to echo back in a git-protocol message
//!   (`unpack <reason>`, `ng <ref> <reason>`, a pkt-line `ERR`, or a 4xx).
//! - **not-found** — the addressed repository does not exist (a 404).
//! - **server-side** — anything the client is not responsible for (store/S3
//!   failures, corruption). The full source chain is logged with
//!   `tracing::error!`; the wire only ever sees a generic failure, so no
//!   internal detail leaks.
//!
//! Only `protocol/` depends on this module; the domain modules never reach
//! into it.

use crate::git::ingest::IngestError;
use crate::git::promote::PromoteError;
use crate::git::walk::WalkError;
use crate::storage::objectdb::ObjectDbError;
use crate::storage::store::StoreError;

/// How a domain error must surface at the protocol boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Class {
    /// The client's request caused the failure. The string is a short,
    /// client-safe reason for a git `unpack`/`ng` message or a pkt `ERR`.
    Client(String),
    /// The addressed repository or object does not exist.
    NotFound,
    /// An internal fault the client is not responsible for and must not see
    /// the detail of.
    Server,
}

/// Maps a domain error to its protocol [`Class`]. Implemented once per domain
/// error type; the implementations carry no HTTP or pkt-line knowledge.
pub trait Classify {
    /// The protocol classification of this error.
    fn class(&self) -> Class;
}

/// Classify `err`, logging its full source chain when the classification is
/// [`Class::Server`]. Callers route every domain error through this function
/// so that an internal fault is always recorded in full while only a generic
/// failure reaches the wire.
pub fn classify<E>(err: &E) -> Class
where
    E: Classify + std::error::Error,
{
    let class = err.class();
    if class == Class::Server {
        log_internal(err);
    }
    class
}

/// Emit a single `tracing::error!` carrying `err` and every error in its
/// `source()` chain, so a server-side fault is diagnosable from logs alone.
fn log_internal(err: &dyn std::error::Error) {
    let mut chain = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    tracing::error!(error = %chain, "internal error serving git request");
}

impl Classify for StoreError {
    fn class(&self) -> Class {
        // Every StoreError is an internal fault: a bad URL, an object-store or
        // SlateDB failure, corrupt keys/values/metadata, an unsupported object
        // format, or create contention. Repository absence is reported by the
        // caller as `NotFound` from an `Option`, never as a `StoreError`.
        Class::Server
    }
}

impl Classify for ObjectDbError {
    fn class(&self) -> Class {
        match self {
            ObjectDbError::Store(e) => e.class(),
            // A blob-store failure is object-storage trouble; a corrupt inline
            // header is storage corruption. Neither is the client's doing.
            ObjectDbError::Blob(_) | ObjectDbError::CorruptInlineBlob { .. } => Class::Server,
        }
    }
}

impl Classify for IngestError {
    fn class(&self) -> Class {
        match self {
            // The received pack is malformed, truncated, failed its checksum,
            // or references a base that exists nowhere: the client's fault.
            IngestError::Pack(_) => Class::Client("index-pack failed".to_owned()),
            // A store read for a thin-pack base failed: surface its own class.
            IngestError::BaseLookup(e) => e.class(),
            // Staging disk I/O, reopening staged files, an object that
            // verified its SHA yet will not decode, or an aborted task are all
            // our machinery failing.
            IngestError::Io(_)
            | IngestError::Bundle(_)
            | IngestError::Decode { .. }
            | IngestError::TaskAborted(_) => Class::Server,
        }
    }
}

impl Classify for PromoteError {
    fn class(&self) -> Class {
        match self {
            // The push is not self-contained: an object reachable from a tip
            // is in neither the pack nor committed storage.
            PromoteError::Connectivity { .. } => {
                Class::Client("missing necessary objects".to_owned())
            }
            PromoteError::Store(e) => e.class(),
            PromoteError::Objects(e) => e.class(),
            PromoteError::Staged(e) => e.class(),
            PromoteError::Walk(e) => e.class(),
            // A SHA-verified object that will not parse is storage/pack
            // corruption, not a client mistake.
            PromoteError::Decode { .. } => Class::Server,
        }
    }
}

impl Classify for WalkError {
    fn class(&self) -> Class {
        match self {
            // A want the server does not have, or of a kind the walk cannot
            // serve, is the client asking for something invalid.
            WalkError::UnknownWant(oid) => Class::Client(format!("not our ref {oid}")),
            WalkError::UnsupportedWant { .. } => {
                Class::Client("want is not a commit or tag".to_owned())
            }
            WalkError::Store(e) => e.class(),
            WalkError::Objects(e) => e.class(),
            // A reference to a missing object, a surprising kind, an
            // undecodable body, or a stalled discovery all indicate corrupt
            // server state.
            WalkError::MissingObject(_)
            | WalkError::UnexpectedKind { .. }
            | WalkError::Decode { .. }
            | WalkError::DiscoveryStalled { .. } => Class::Server,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gix_hash::ObjectId;

    fn oid(hex_byte: u8) -> ObjectId {
        ObjectId::from_hex(&[hex_byte; 40]).expect("valid sha1 hex")
    }

    #[test]
    fn should_classify_a_not_self_contained_push_as_client() {
        // given: a connectivity failure naming the object that is missing
        let err = PromoteError::Connectivity {
            missing: oid(b'a'),
            referenced_by: Some(oid(b'b')),
        };

        // when/then: it is client-caused with a safe, generic reason
        assert_eq!(
            err.class(),
            Class::Client("missing necessary objects".to_owned())
        );
    }

    #[test]
    fn should_classify_an_unknown_want_as_client_with_the_oid() {
        // given
        let want = oid(b'c');
        let err = WalkError::UnknownWant(want);

        // when/then
        assert_eq!(err.class(), Class::Client(format!("not our ref {want}")));
    }

    #[test]
    fn should_classify_store_faults_as_server() {
        // given/when/then: internal faults never surface their detail
        assert_eq!(StoreError::CreateContention.class(), Class::Server);
        assert_eq!(
            StoreError::UnsupportedObjectFormat("sha42".to_owned()).class(),
            Class::Server
        );
    }

    #[test]
    fn should_classify_promotion_corruption_as_server() {
        // given: an object that verified its SHA yet cannot be parsed
        let err = PromoteError::Objects(ObjectDbError::CorruptInlineBlob { oid: oid(b'd') });

        // when/then
        assert_eq!(err.class(), Class::Server);
    }
}
