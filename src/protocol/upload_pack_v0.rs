//! The `git-upload-pack` endpoint for the classic (v0) fetch protocol.
//!
//! See `docs/0001-init.md` §Fetch API. A client that does not opt into
//! protocol v2 (no `Git-Protocol: version=2` header) speaks the original
//! protocol: the ref advertisement lists every ref as a `<oid> <name>`
//! pkt-line with the capability list riding on the first line after a NUL, and
//! a fetch request is a list of `want <oid>` lines (the first carrying the
//! client's capabilities), then `have <oid>` lines, terminated by `done` or a
//! flush.
//!
//! Ref enumeration, symref resolution, tag peeling, want planning, and pack
//! streaming are all shared with the protocol-v2 handler in
//! [`crate::protocol::upload_pack`]; this module only frames the v0-specific
//! advertisement and request/response wire format. All results use the
//! `application/x-git-upload-pack-result` content type.

use std::io;
use std::time::Instant;

use axum::http::StatusCode;
use axum::response::Response;
use bytes::Bytes;
use gix_hash::ObjectId;
use gix_packetline_blocking::{PacketLineRef, decode, encode};

use crate::AppState;
use crate::error::{self, Class};
use crate::git::walk::{FilterSpec, Walker};
use crate::protocol::upload_pack::LsRefsError;
use crate::protocol::{advertise, http, upload_pack};
use crate::storage::RepoMeta;
use crate::storage::values::RefTarget;

/// The name of the `HEAD` ref, advertised first when it resolves.
const HEAD_REF: &str = "HEAD";

/// Build the classic (v0) upload-pack ref advertisement for `meta`. Resolves
/// `HEAD` and every other ref through the shared symref/peel machinery, places
/// `HEAD` first (with its `symref=HEAD:<target>` capability) when it resolves,
/// then the remaining refs in name order. A failure is classified (logging any
/// server fault) and surfaced as an opaque error the caller renders as a 500.
pub(super) async fn advertise(state: &AppState, meta: &RepoMeta) -> Result<Vec<u8>, ()> {
    build_advertisement(state, meta).await.map_err(|err| {
        let _ = error::classify(&err);
    })
}

/// Resolve refs and assemble the advertisement bytes. Split out from
/// [`advertise`] so the error type stays internal to this module.
async fn build_advertisement(state: &AppState, meta: &RepoMeta) -> Result<Vec<u8>, LsRefsError> {
    let refs = state.store.list_refs(meta.id, None).await?;

    // `list_refs` returns refs in name order, with HEAD sorting first; split it
    // out so it can lead the advertisement carrying the symref capability.
    let mut head_target: Option<RefTarget> = None;
    let mut rest = Vec::new();
    for (name, target) in refs {
        if name == HEAD_REF {
            head_target = Some(target);
        } else {
            rest.push((name, target));
        }
    }

    let mut advertised = Vec::new();
    let mut symref_target: Option<String> = None;
    if let Some(target) = head_target
        && let upload_pack::Resolution::Resolved(oid) =
            upload_pack::resolve_ref(&state.store, meta.id, &target).await?
    {
        // HEAD is symbolic and resolvable: advertise it first and name its
        // symref target so the client checks out the right branch.
        if let RefTarget::Reference(inner) = &target {
            symref_target = Some(inner.clone());
        }
        advertised.push(advertise::UploadRef {
            name: HEAD_REF.to_owned(),
            oid,
            peeled: None,
        });
    }

    for (name, target) in rest {
        if let upload_pack::Resolution::Resolved(oid) =
            upload_pack::resolve_ref(&state.store, meta.id, &target).await?
        {
            let peeled = upload_pack::peel_ref(&state.objectdb, meta.id, oid).await?;
            advertised.push(advertise::UploadRef { name, oid, peeled });
        }
    }

    let mut body = Vec::new();
    advertise::upload_pack_v0(
        &mut body,
        &advertised,
        symref_target.as_deref(),
        meta.object_format,
    )?;
    Ok(body)
}

/// Serve a classic `POST /<repo>/git-upload-pack` request. `repo` is the
/// already validated repository name.
///
/// A clone (no haves) is answered with `NAK` followed by the pack for the
/// wants. Incremental fetches — any request carrying `have` lines — are
/// declined with an in-band `ERR` directing the client to protocol v2, which
/// serves negotiation.
pub async fn upload_pack(state: &AppState, repo: &str, body: Bytes) -> Response {
    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(ParseError) => {
            tracing::debug!("malformed upload-pack request");
            return malformed();
        }
    };

    // Upload-pack never auto-creates: an unknown repository is a 404, matching
    // the advertisement.
    let meta = match state.store.lookup_repo(repo).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return http::plain(StatusCode::NOT_FOUND, "repository not found"),
        Err(err) => return upload_pack::error_response(error::classify(&err)),
    };

    if request.wants.is_empty() {
        return upload_pack::reject_fetch("fetch requires at least one want");
    }

    // Incremental negotiation is not offered over the classic protocol; a
    // client with objects to reuse must speak protocol v2.
    if !request.haves.is_empty() {
        return upload_pack::reject_fetch("incremental fetch requires protocol v2");
    }

    let nak = match nak_line() {
        Ok(nak) => nak,
        Err(_) => return http::internal_error(),
    };
    // Without `done` there is no negotiation to advance and nothing common to
    // report, so only the NAK is sent; the pack follows once the client is
    // done.
    if !request.done {
        return http::git_response(StatusCode::OK, upload_pack::RESULT_CONTENT_TYPE, nak);
    }

    let start = Instant::now();
    let walker = Walker::new(state.store.clone(), state.objectdb.clone(), meta.id);
    let (collected, _common) =
        match upload_pack::plan_pack(&walker, &request.wants, &request.haves, &FilterSpec::None)
            .await
        {
            Ok(planned) => planned,
            Err(err) => {
                let class = error::classify(&err);
                if matches!(class, Class::Client(_)) {
                    metrics::counter!("fetch_total", "outcome" => "rejected").increment(1);
                }
                return upload_pack::error_response(class);
            }
        };
    let Ok(count) = u32::try_from(collected.len()) else {
        return upload_pack::reject_fetch("too many objects for one pack");
    };

    tracing::debug!(
        wants = request.wants.len(),
        objects_packed = count,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "fetch planned"
    );
    metrics::counter!("fetch_total", "outcome" => "ok").increment(1);
    metrics::histogram!("fetch_objects_packed").record(f64::from(count));

    upload_pack::stream_pack_response(
        state.objectdb.clone(),
        &meta,
        collected,
        count,
        nak,
        request.side_band_64k,
        request.no_progress,
    )
}

/// A 400 carrying an `ERR` pkt-line, for a body that is not framed as a valid
/// classic fetch request.
fn malformed() -> Response {
    match advertise::error_line("malformed upload-pack request") {
        Ok(body) => http::git_response(
            StatusCode::BAD_REQUEST,
            upload_pack::RESULT_CONTENT_TYPE,
            body,
        ),
        Err(_) => http::internal_error(),
    }
}

/// The `NAK` pkt-line that opens a clone response (nothing common to
/// acknowledge).
fn nak_line() -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    encode::data_to_write(b"NAK\n", &mut out)?;
    Ok(out)
}

/// A parsed classic fetch request.
#[derive(Debug, Default, PartialEq, Eq)]
struct Request {
    /// The objects the client asks for (tips it saw advertised).
    wants: Vec<ObjectId>,
    /// Commits the client claims to already have.
    haves: Vec<ObjectId>,
    /// The client sent `done`: it wants the pack immediately.
    done: bool,
    /// The client negotiated `side-band-64k`, so the pack is multiplexed.
    side_band_64k: bool,
    /// The client asked to suppress side-band progress messages.
    no_progress: bool,
}

/// The request body was not framed as a valid classic fetch request.
#[derive(Debug)]
struct ParseError;

/// Parse a classic fetch request: `want <oid>` lines (the first carrying the
/// client's space-separated capabilities after the oid), then `have <oid>`
/// lines, with flush packets separating the sections and either a `done`
/// pkt-line or the end of the body terminating it. Text lines may omit their
/// trailing newline. An empty body, a bad length prefix, or an unrecognized
/// line is malformed.
fn parse_request(body: &Bytes) -> Result<Request, ParseError> {
    let mut request = Request::default();
    let mut offset = 0;
    let mut first_want = true;
    let mut saw_line = false;

    while offset < body.len() {
        let (line, consumed) = match decode::streaming(&body[offset..]) {
            Ok(decode::Stream::Complete {
                line,
                bytes_consumed,
            }) => (line, bytes_consumed),
            // A truncated final packet or a bad length prefix is malformed.
            _ => return Err(ParseError),
        };
        offset += consumed;
        saw_line = true;
        match line {
            // A flush separates the want and have sections; either one is a
            // structural marker with nothing to record.
            PacketLineRef::Flush => {}
            PacketLineRef::Data(data) => {
                let text = std::str::from_utf8(data.strip_suffix(b"\n").unwrap_or(data))
                    .map_err(|_| ParseError)?;
                if text == "done" {
                    request.done = true;
                    break;
                } else if let Some(rest) = text.strip_prefix("want ") {
                    // Only the first want line carries capabilities, after the
                    // oid; a later want line is just `want <oid>`.
                    let (hex, caps) = match rest.split_once(' ') {
                        Some((hex, caps)) => (hex, Some(caps)),
                        None => (rest, None),
                    };
                    let oid = ObjectId::from_hex(hex.as_bytes()).map_err(|_| ParseError)?;
                    request.wants.push(oid);
                    if first_want {
                        if let Some(caps) = caps {
                            parse_caps(caps, &mut request);
                        }
                        first_want = false;
                    }
                } else if let Some(hex) = text.strip_prefix("have ") {
                    let oid = ObjectId::from_hex(hex.as_bytes()).map_err(|_| ParseError)?;
                    request.haves.push(oid);
                } else {
                    // The server advertises no capability (shallow, filter, …)
                    // that would license any other line, so anything else is
                    // malformed.
                    return Err(ParseError);
                }
            }
            // A delimiter or response-end packet has no place in a fetch request.
            _ => return Err(ParseError),
        }
    }

    if !saw_line {
        return Err(ParseError);
    }
    Ok(request)
}

/// Record the capabilities the endpoint acts on from the first want line's
/// space-separated capability list. A capability may carry a `=value` suffix
/// (e.g. `agent=git/2.x`); every capability the endpoint does not act on
/// (including `agent` and, in this handler, any multi-ack variant) is ignored.
fn parse_caps(caps: &str, request: &mut Request) {
    for token in caps.split(' ') {
        match token.split('=').next().unwrap_or(token) {
            "side-band-64k" => request.side_band_64k = true,
            "no-progress" => request.no_progress = true,
            _ => {}
        }
    }
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

    /// A distinct SHA-1 object id built from a single hex nibble.
    fn oid(nibble: u8) -> ObjectId {
        ObjectId::from_hex(&[nibble; 40]).expect("valid sha1 hex")
    }

    #[test]
    fn should_parse_first_want_capabilities_and_further_wants() {
        // given: a clone request — a first want carrying capabilities, a second
        // bare want, a flush, then done
        let mut body = pkt(format!(
            "want {} side-band-64k no-progress agent=git/2.43\n",
            oid(b'a')
        )
        .as_bytes());
        body.extend(pkt(format!("want {}\n", oid(b'b')).as_bytes()));
        body.extend_from_slice(b"0000");
        body.extend(pkt(b"done\n"));

        // when
        let request = parse_request(&Bytes::from(body)).expect("parse");

        // then: both wants are captured, the caps ride only on the first line,
        // and there are no haves
        assert_eq!(request.wants, vec![oid(b'a'), oid(b'b')]);
        assert!(request.side_band_64k);
        assert!(request.no_progress);
        assert!(request.done);
        assert!(request.haves.is_empty());
    }

    #[test]
    fn should_parse_haves_terminated_by_flush_without_done() {
        // given: a negotiation round — wants, a flush, haves, then a closing
        // flush and no done
        let mut body = pkt(format!("want {}\n", oid(b'a')).as_bytes());
        body.extend_from_slice(b"0000");
        body.extend(pkt(format!("have {}\n", oid(b'c')).as_bytes()));
        body.extend(pkt(format!("have {}\n", oid(b'd')).as_bytes()));
        body.extend_from_slice(b"0000");

        // when
        let request = parse_request(&Bytes::from(body)).expect("parse");

        // then: the haves are captured in order and the request is not done
        assert_eq!(request.wants, vec![oid(b'a')]);
        assert_eq!(request.haves, vec![oid(b'c'), oid(b'd')]);
        assert!(!request.done);
    }

    #[test]
    fn should_parse_done_terminated_request() {
        // given: wants, a flush, one have, then done
        let mut body = pkt(format!("want {}\n", oid(b'a')).as_bytes());
        body.extend_from_slice(b"0000");
        body.extend(pkt(format!("have {}\n", oid(b'c')).as_bytes()));
        body.extend(pkt(b"done\n"));

        // when
        let request = parse_request(&Bytes::from(body)).expect("parse");

        // then
        assert_eq!(request.haves, vec![oid(b'c')]);
        assert!(request.done);
    }

    #[test]
    fn should_tolerate_text_lines_without_a_trailing_newline() {
        // given: want/have/done lines with no trailing LF (a permitted variant)
        let mut body = pkt(format!("want {} side-band-64k", oid(b'a')).as_bytes());
        body.extend_from_slice(b"0000");
        body.extend(pkt(format!("have {}", oid(b'c')).as_bytes()));
        body.extend(pkt(b"done"));

        // when
        let request = parse_request(&Bytes::from(body)).expect("parse");

        // then: the lines parse identically to their newline-terminated forms
        assert_eq!(request.wants, vec![oid(b'a')]);
        assert!(request.side_band_64k);
        assert_eq!(request.haves, vec![oid(b'c')]);
        assert!(request.done);
    }

    #[test]
    fn should_reject_an_empty_body() {
        // given/when/then: no pkt-lines at all is malformed framing
        assert!(parse_request(&Bytes::new()).is_err());
    }

    #[test]
    fn should_reject_a_truncated_packet() {
        // given: a length prefix promising more bytes than follow
        let body = Bytes::from_static(b"0032want");

        // when/then
        assert!(parse_request(&body).is_err());
    }

    #[test]
    fn should_reject_an_unrecognized_line() {
        // given: a line the server never licenses (shallow is not advertised)
        let mut body = pkt(format!("want {}\n", oid(b'a')).as_bytes());
        body.extend(pkt(format!("shallow {}\n", oid(b'c')).as_bytes()));

        // when/then
        assert!(parse_request(&Bytes::from(body)).is_err());
    }

    #[test]
    fn should_reject_a_malformed_want_oid() {
        // given/when/then
        let body = pkt(b"want not-a-valid-oid\n");
        assert!(parse_request(&Bytes::from(body)).is_err());
    }
}
