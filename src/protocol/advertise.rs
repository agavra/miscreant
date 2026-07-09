//! Pkt-line service advertisements for the smart-HTTP endpoints.
//!
//! See `docs/0001-init.md` §Fetch API (the capability list) and §Receive API
//! (discovery). Each advertisement is assembled into an in-memory buffer;
//! packfile bodies are streamed over side-band channels by later changes.

use std::io;

use gix_hash::{Kind, ObjectId};
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
/// capability (`version 2`, `agent=…`, `ls-refs=unborn`, `fetch=filter`,
/// `object-info`, `object-format=sha1`), terminated by a flush. The `unborn`
/// value on `ls-refs` tells clients the server understands the `unborn`
/// argument, and the `filter` value on `fetch` tells them the server
/// understands the `filter` argument — clients only send either argument
/// when the server advertises support for it. `object-info` is a bare
/// capability (no value), matching how `ls-refs` and `fetch` announce
/// command support — clients discover the command from its presence here.
pub fn upload_pack(out: &mut Vec<u8>) -> io::Result<()> {
    encode::data_to_write(b"# service=git-upload-pack\n", &mut *out)?;
    encode::flush_to_write(&mut *out)?;
    encode::data_to_write(b"version 2\n", &mut *out)?;
    encode::data_to_write(format!("agent={}\n", agent()).as_bytes(), &mut *out)?;
    encode::data_to_write(b"ls-refs=unborn\n", &mut *out)?;
    encode::data_to_write(b"fetch=filter\n", &mut *out)?;
    encode::data_to_write(b"object-info\n", &mut *out)?;
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
    let caps = format!(
        "report-status delete-refs side-band-64k ofs-delta agent={}",
        agent()
    );

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

/// A ref resolved for the classic (v0) upload-pack advertisement: its name,
/// the object id it resolves to, and the peeled commit when it names an
/// annotated tag object.
pub struct UploadRef {
    /// Full ref name (`HEAD`, `refs/heads/main`, `refs/tags/v1`, …).
    pub name: String,
    /// The object id the ref resolves to.
    pub oid: ObjectId,
    /// The final non-tag object when `name` points at an annotated tag,
    /// advertised on a trailing `<peeled> <name>^{}` line.
    pub peeled: Option<ObjectId>,
}

/// Build the classic (v0) upload-pack ref advertisement.
///
/// Layout: `# service=git-upload-pack\n` then a flush, then one `<oid> <name>`
/// pkt-line per ref (each annotated tag followed immediately by its
/// `<peeled-oid> <name>^{}` line), terminated by a flush. `refs` is emitted in
/// order — the caller places `HEAD` first when it resolves, then the remaining
/// refs sorted by name. The capability list rides on the first ref line after
/// a NUL; when `HEAD` resolves it leads the list and carries the
/// `symref=HEAD:<target>` capability naming its symref target. The
/// `multi_ack_detailed` capability signals that the fetch handler negotiates
/// common history round by round. An empty repository (no resolvable refs)
/// emits the synthetic `<null-oid> capabilities^{}` line so capabilities are
/// still conveyed.
pub fn upload_pack_v0(
    out: &mut Vec<u8>,
    refs: &[UploadRef],
    head_symref_target: Option<&str>,
    object_format: Kind,
) -> io::Result<()> {
    let mut caps = format!(
        "multi_ack_detailed side-band-64k no-progress agent={}",
        agent()
    );
    if let Some(target) = head_symref_target {
        caps.push_str(&format!(" symref=HEAD:{target}"));
    }

    encode::data_to_write(b"# service=git-upload-pack\n", &mut *out)?;
    encode::flush_to_write(&mut *out)?;

    if refs.is_empty() {
        // No resolvable refs: convey capabilities on the null-oid line.
        let null = object_format.null();
        let line = format!("{} capabilities^{{}}\0{caps}\n", null.to_hex());
        encode::data_to_write(line.as_bytes(), &mut *out)?;
    } else {
        for (index, advertised) in refs.iter().enumerate() {
            let line = if index == 0 {
                format!("{} {}\0{caps}\n", advertised.oid.to_hex(), advertised.name)
            } else {
                format!("{} {}\n", advertised.oid.to_hex(), advertised.name)
            };
            encode::data_to_write(line.as_bytes(), &mut *out)?;
            if let Some(peeled) = advertised.peeled {
                let peeled_line = format!("{} {}^{{}}\n", peeled.to_hex(), advertised.name);
                encode::data_to_write(peeled_line.as_bytes(), &mut *out)?;
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a data pkt-line: a 4-hex length prefix (covering itself) then the
    /// payload verbatim.
    fn pkt(data: &[u8]) -> Vec<u8> {
        let mut out = format!("{:04x}", data.len() + 4).into_bytes();
        out.extend_from_slice(data);
        out
    }

    const FLUSH: &[u8] = b"0000";

    /// A distinct SHA-1 object id built from a single hex nibble.
    fn oid(nibble: u8) -> ObjectId {
        ObjectId::from_hex(&[nibble; 40]).expect("valid sha1 hex")
    }

    fn agent_cap() -> String {
        format!("agent={}", agent())
    }

    #[test]
    fn should_put_capabilities_and_symref_on_the_first_v0_upload_line() {
        // given: HEAD resolving to a commit, then a branch at the same commit
        let refs = vec![
            UploadRef {
                name: "HEAD".to_owned(),
                oid: oid(b'a'),
                peeled: None,
            },
            UploadRef {
                name: "refs/heads/main".to_owned(),
                oid: oid(b'a'),
                peeled: None,
            },
        ];

        // when
        let mut out = Vec::new();
        upload_pack_v0(&mut out, &refs, Some("refs/heads/main"), Kind::Sha1).expect("build");

        // then: HEAD leads with the capabilities and the symref target after a
        // NUL; the branch line is bare
        let caps = format!(
            "multi_ack_detailed side-band-64k no-progress {} symref=HEAD:refs/heads/main",
            agent_cap()
        );
        let mut expected = Vec::new();
        expected.extend(pkt(b"# service=git-upload-pack\n"));
        expected.extend_from_slice(FLUSH);
        expected.extend(pkt(
            format!("{} HEAD\0{caps}\n", oid(b'a').to_hex()).as_bytes()
        ));
        expected.extend(pkt(
            format!("{} refs/heads/main\n", oid(b'a').to_hex()).as_bytes()
        ));
        expected.extend_from_slice(FLUSH);
        assert_eq!(out, expected);
    }

    #[test]
    fn should_emit_a_peeled_line_after_an_annotated_tag() {
        // given: a single tag ref with a peeled target and no HEAD (so no
        // symref capability)
        let refs = vec![UploadRef {
            name: "refs/tags/v1".to_owned(),
            oid: oid(b'b'),
            peeled: Some(oid(b'c')),
        }];

        // when
        let mut out = Vec::new();
        upload_pack_v0(&mut out, &refs, None, Kind::Sha1).expect("build");

        // then: the tag line carries the caps, immediately followed by its
        // peeled `^{}` line
        let caps = format!(
            "multi_ack_detailed side-band-64k no-progress {}",
            agent_cap()
        );
        let mut expected = Vec::new();
        expected.extend(pkt(b"# service=git-upload-pack\n"));
        expected.extend_from_slice(FLUSH);
        expected.extend(pkt(format!(
            "{} refs/tags/v1\0{caps}\n",
            oid(b'b').to_hex()
        )
        .as_bytes()));
        expected.extend(pkt(
            format!("{} refs/tags/v1^{{}}\n", oid(b'c').to_hex()).as_bytes()
        ));
        expected.extend_from_slice(FLUSH);
        assert_eq!(out, expected);
    }

    #[test]
    fn should_advertise_capabilities_on_the_null_line_for_an_empty_repo() {
        // given/when: no resolvable refs at all
        let mut out = Vec::new();
        upload_pack_v0(&mut out, &[], None, Kind::Sha1).expect("build");

        // then: the synthetic capabilities line carries the caps on the null oid
        let caps = format!(
            "multi_ack_detailed side-band-64k no-progress {}",
            agent_cap()
        );
        let zeros = "0".repeat(40);
        let mut expected = Vec::new();
        expected.extend(pkt(b"# service=git-upload-pack\n"));
        expected.extend_from_slice(FLUSH);
        expected.extend(pkt(
            format!("{zeros} capabilities^{{}}\0{caps}\n").as_bytes()
        ));
        expected.extend_from_slice(FLUSH);
        assert_eq!(out, expected);
    }
}
