//! The smart-git HTTP protocol surface.
//!
//! See `docs/0001-init.md` §Transfer Protocol, §Fetch API, and §Receive API.
//! `http` owns routing and repository resolution; `advertise` builds the
//! pkt-line service advertisements; `receive_pack` serves the push RPC;
//! `upload_pack` the fetch-side protocol-v2 command dispatch; and
//! `upload_pack_v0` the classic (v0) fetch protocol.

pub mod advertise;
pub mod http;
pub mod receive_pack;
pub mod upload_pack;
pub mod upload_pack_v0;

/// Largest payload per side-band data packet: the pkt-line data limit (65516)
/// minus the one-byte channel prefix side-band framing prepends.
pub(crate) const MAX_BAND_DATA: usize = 65515;
