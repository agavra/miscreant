//! Pkt-line service advertisements for the smart-HTTP endpoints.
//!
//! See `docs/0001-init.md` §Fetch API (the capability list) and §Receive API
//! (discovery). Each advertisement is assembled into an in-memory buffer;
//! packfile bodies are streamed over side-band channels by later changes.

use std::io;

use gix_hash::Kind;
use gix_packetline_blocking::encode;

use crate::storage::values::RefTarget;

/// Content type of the protocol-v2 upload-pack advertisement.
pub const UPLOAD_ADVERTISEMENT_CONTENT_TYPE: &str = "application/x-git-upload-pack-advertisement";
/// Content type of the classic (v0) receive-pack advertisement.
pub const RECEIVE_ADVERTISEMENT_CONTENT_TYPE: &str = "application/x-git-receive-pack-advertisement";

/// The server's agent identifier, e.g. `miscreant/0.1.0`.
pub fn agent() -> String {
    format!("miscreant/{}", env!("CARGO_PKG_VERSION"))
}

/// Build the protocol-v2 upload-pack capability advertisement.
///
/// Layout: `# service=git-upload-pack\n` then a flush, then one pkt-line per
/// capability (`version 2`, `agent=…`, `ls-refs`, `fetch`,
/// `object-format=sha1`), terminated by a flush.
pub fn upload_pack(out: &mut Vec<u8>) -> io::Result<()> {
    encode::data_to_write(b"# service=git-upload-pack\n", &mut *out)?;
    encode::flush_to_write(&mut *out)?;
    encode::data_to_write(b"version 2\n", &mut *out)?;
    encode::data_to_write(format!("agent={}\n", agent()).as_bytes(), &mut *out)?;
    encode::data_to_write(b"ls-refs\n", &mut *out)?;
    encode::data_to_write(b"fetch\n", &mut *out)?;
    encode::data_to_write(b"object-format=sha1\n", &mut *out)?;
    encode::flush_to_write(&mut *out)?;
    Ok(())
}

/// Build the classic v0 receive-pack ref advertisement.
///
/// Layout: `# service=git-receive-pack\n` then a flush, then the ref lines,
/// terminated by a flush. Only direct refs are advertised (symbolic refs such
/// as `HEAD` are omitted on the receive side). The capability list rides on the
/// first ref line after a NUL; an empty repository emits the synthetic
/// `<null-oid> capabilities^{}` line so capabilities are still conveyed.
pub fn receive_pack(
    out: &mut Vec<u8>,
    refs: &[(String, RefTarget)],
    object_format: Kind,
) -> io::Result<()> {
    let caps = format!("report-status delete-refs ofs-delta agent={}", agent());

    encode::data_to_write(b"# service=git-receive-pack\n", &mut *out)?;
    encode::flush_to_write(&mut *out)?;

    let mut first = true;
    for (name, target) in refs {
        let RefTarget::Direct(oid) = target else {
            continue;
        };
        let line = if first {
            format!("{} {name}\0{caps}\n", oid.to_hex())
        } else {
            format!("{} {name}\n", oid.to_hex())
        };
        encode::data_to_write(line.as_bytes(), &mut *out)?;
        first = false;
    }

    if first {
        // No direct refs: advertise capabilities on the null-oid line.
        let null = object_format.null();
        let line = format!("{} capabilities^{{}}\0{caps}\n", null.to_hex());
        encode::data_to_write(line.as_bytes(), &mut *out)?;
    }

    encode::flush_to_write(&mut *out)?;
    Ok(())
}

/// Build a single `ERR <message>` pkt-line, used for protocol-level rejections.
pub fn error_line(message: &str) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    encode::error_to_write(message.as_bytes(), &mut out)?;
    Ok(out)
}
