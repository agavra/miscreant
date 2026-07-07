//! The smart-git HTTP protocol surface.
//!
//! See `docs/0001-init.md` §Transfer Protocol, §Fetch API, and §Receive API.
//! `http` owns routing and repository resolution; `advertise` builds the
//! pkt-line service advertisements.

pub mod advertise;
pub mod http;
