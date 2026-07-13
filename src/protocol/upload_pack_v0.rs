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
use crate::git::walk::{FilterSpec, WalkError, Walker};
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
/// Smart-HTTP is stateless: every POST carries the client's full accumulated
/// have list, so each round is computed from scratch. A round without `done`
/// is a negotiation step — it acknowledges the common haves and reports
/// whether the server can build the pack yet, sending no pack. A round with
/// `done` runs the full walk and streams the pack, prefixed by the final
/// acknowledgement.
pub async fn upload_pack(state: &AppState, repo: &str, body: Bytes) -> Response {
    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(ParseError::Filter) => return upload_pack::reject_fetch("filter requires protocol v2"),
        Err(ParseError::Malformed) => {
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

    let walker = Walker::new(state.store.clone(), state.objectdb.clone(), meta.id);
    if request.done {
        stream_done_round(state, &meta, &walker, &request).await
    } else {
        negotiate(&walker, &request).await
    }
}

/// A negotiation round (no `done`): discover which of the client's haves the
/// server shares and whether that shared history already bounds the wants,
/// then answer with the acknowledgement block and no pack. Determining the
/// commons and the ready signal is exactly Phase-I commit discovery over the
/// round's wants and haves.
async fn negotiate(walker: &Walker, request: &Request) -> Response {
    let partition = match walker.partition_wants(&request.wants).await {
        Ok(partition) => partition,
        Err(err) => return classify_walk_error(err),
    };
    let selection = match walker
        .select_commits(&partition.history, &request.haves)
        .await
    {
        Ok(selection) => selection,
        Err(err) => return classify_walk_error(err),
    };

    // `ready` mirrors git's ok_to_give_up: the server has enough shared history
    // to build the pack when at least one have is common and discovery resolved
    // the wants without walking to the roots.
    let ready = !selection.common.is_empty() && selection.bounded_by_haves;
    let body = match ack_negotiation_block(&selection.common, ready, request.multi_ack_detailed) {
        Ok(body) => body,
        Err(_) => return http::internal_error(),
    };
    tracing::debug!(
        haves = request.haves.len(),
        common = selection.common.len(),
        ready,
        "fetch negotiation round"
    );
    http::git_response(StatusCode::OK, upload_pack::RESULT_CONTENT_TYPE, body)
}

/// A `done` round: run the full walk over the round's wants and haves and
/// stream the pack, prefixed by the final acknowledgement — `ACK
/// <last-common>` when any have is common, else `NAK`.
async fn stream_done_round(
    state: &AppState,
    meta: &RepoMeta,
    walker: &Walker,
    request: &Request,
) -> Response {
    let start = Instant::now();
    let plan = match upload_pack::plan_pack(
        walker,
        &request.wants,
        &request.haves,
        &FilterSpec::None,
        request.ofs_delta,
    )
    .await
    {
        Ok(planned) => planned,
        Err(err) => return classify_walk_error(err),
    };
    let Ok(count) = u32::try_from(plan.objects.len()) else {
        return upload_pack::reject_fetch("too many objects for one pack");
    };

    let prefix = match ack_done_line(&plan.common) {
        Ok(prefix) => prefix,
        Err(_) => return http::internal_error(),
    };

    tracing::debug!(
        wants = request.wants.len(),
        haves = request.haves.len(),
        common = plan.common.len(),
        objects_packed = count,
        elapsed_ms = start.elapsed().as_millis() as u64,
        partition_ms = plan.timings.partition_wants.as_millis() as u64,
        select_ms = plan.timings.select_commits.as_millis() as u64,
        collect_ms = plan.timings.collect.as_millis() as u64,
        schedule_ms = plan.timings.schedule_for_ofs.as_millis() as u64,
        "fetch planned"
    );
    metrics::counter!("fetch_total", "outcome" => "ok").increment(1);
    metrics::histogram!("fetch_objects_packed").record(f64::from(count));

    upload_pack::stream_pack_response(
        state.objectdb.clone(),
        meta,
        plan.objects,
        count,
        prefix,
        request.side_band_64k,
        request.no_progress,
        request.ofs_delta,
    )
}

/// Render a walk failure: a client-caused one (an unknown want) becomes an
/// in-band `ERR` and counts as a rejected fetch; anything else is a 404 or a
/// generic 500. Mirrors the protocol-v2 handler's classification so both
/// protocols reject an invalid want identically.
fn classify_walk_error(err: WalkError) -> Response {
    let class = error::classify(&err);
    if matches!(class, Class::Client(_)) {
        metrics::counter!("fetch_total", "outcome" => "rejected").increment(1);
    }
    upload_pack::error_response(class)
}

/// Build a negotiation-round acknowledgement block: with `multi_ack_detailed`,
/// an `ACK <oid> common` line for each common have (in request order), then an
/// `ACK <last-common> ready` line when `ready`, and always a terminating
/// `NAK`. Without that capability the detailed lines are suppressed and only
/// the `NAK` is sent — the client keeps offering haves until it sends `done`.
fn ack_negotiation_block(
    common: &[ObjectId],
    ready: bool,
    multi_ack_detailed: bool,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if multi_ack_detailed {
        for oid in common {
            encode::data_to_write(
                format!("ACK {} common\n", oid.to_hex()).as_bytes(),
                &mut out,
            )?;
        }
        if ready && let Some(last) = common.last() {
            encode::data_to_write(
                format!("ACK {} ready\n", last.to_hex()).as_bytes(),
                &mut out,
            )?;
        }
    }
    encode::data_to_write(b"NAK\n", &mut out)?;
    Ok(out)
}

/// Build the `done`-round acknowledgement line that prefixes the pack:
/// `ACK <last-common>` (no status suffix) when any have is common, else `NAK`.
fn ack_done_line(common: &[ObjectId]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    match common.last() {
        Some(last) => {
            encode::data_to_write(format!("ACK {}\n", last.to_hex()).as_bytes(), &mut out)?;
        }
        None => {
            encode::data_to_write(b"NAK\n", &mut out)?;
        }
    }
    Ok(out)
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
    /// The client accepts in-pack offset deltas.
    ofs_delta: bool,
    /// The client negotiated `multi_ack_detailed`, so negotiation rounds may
    /// carry `ACK <oid> common`/`ready` lines; without it only `NAK` is sent.
    multi_ack_detailed: bool,
}

/// Why a classic fetch request could not be parsed.
#[derive(Debug)]
enum ParseError {
    /// The body was not framed as a valid classic fetch request.
    Malformed,
    /// The client sent a `filter` line: partial-clone filtering is a
    /// protocol-v2 feature the classic protocol does not carry.
    Filter,
}

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
            _ => return Err(ParseError::Malformed),
        };
        offset += consumed;
        saw_line = true;
        match line {
            // A flush separates the want and have sections; either one is a
            // structural marker with nothing to record.
            PacketLineRef::Flush => {}
            PacketLineRef::Data(data) => {
                let text = std::str::from_utf8(data.strip_suffix(b"\n").unwrap_or(data))
                    .map_err(|_| ParseError::Malformed)?;
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
                    let oid =
                        ObjectId::from_hex(hex.as_bytes()).map_err(|_| ParseError::Malformed)?;
                    request.wants.push(oid);
                    if first_want {
                        if let Some(caps) = caps {
                            parse_caps(caps, &mut request);
                        }
                        first_want = false;
                    }
                } else if let Some(hex) = text.strip_prefix("have ") {
                    let oid =
                        ObjectId::from_hex(hex.as_bytes()).map_err(|_| ParseError::Malformed)?;
                    request.haves.push(oid);
                } else if text.strip_prefix("filter ").is_some() {
                    // Partial-clone filtering is a protocol-v2 feature; the
                    // classic protocol advertises no `filter` capability, so a
                    // client that sends one anyway is refused explicitly rather
                    // than silently served an unfiltered pack.
                    return Err(ParseError::Filter);
                } else {
                    // The server advertises no other capability (shallow, …)
                    // that would license any further line, so anything else is
                    // malformed.
                    return Err(ParseError::Malformed);
                }
            }
            // A delimiter or response-end packet has no place in a fetch request.
            _ => return Err(ParseError::Malformed),
        }
    }

    if !saw_line {
        return Err(ParseError::Malformed);
    }
    Ok(request)
}

/// Record the capabilities the endpoint acts on from the first want line's
/// space-separated capability list. A capability may carry a `=value` suffix
/// (e.g. `agent=git/2.x`); every capability the endpoint does not act on
/// (including `agent`) is ignored.
fn parse_caps(caps: &str, request: &mut Request) {
    for token in caps.split(' ') {
        match token.split('=').next().unwrap_or(token) {
            "side-band-64k" => request.side_band_64k = true,
            "no-progress" => request.no_progress = true,
            "multi_ack_detailed" => request.multi_ack_detailed = true,
            "ofs-delta" => request.ofs_delta = true,
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
            "want {} multi_ack_detailed side-band-64k no-progress ofs-delta agent=git/2.43\n",
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
        assert!(request.multi_ack_detailed);
        assert!(request.side_band_64k);
        assert!(request.no_progress);
        assert!(request.ofs_delta);
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
    fn should_refuse_a_filter_line_as_a_protocol_v2_feature() {
        // given: a classic fetch body carrying a partial-clone filter line
        let mut body = pkt(format!("want {}\n", oid(b'a')).as_bytes());
        body.extend(pkt(b"filter blob:none\n"));
        body.extend_from_slice(b"0000");
        body.extend(pkt(b"done\n"));

        // when
        let err = parse_request(&Bytes::from(body)).expect_err("filter is refused");

        // then: the filter-specific refusal, not a generic malformed body
        assert!(matches!(err, ParseError::Filter));
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

    #[test]
    fn should_ack_each_common_in_order_then_ready_then_nak() {
        // given: two common haves and a ready signal, multi_ack_detailed on
        let common = vec![oid(b'c'), oid(b'd')];

        // when
        let block = ack_negotiation_block(&common, true, true).expect("block");

        // then: an `ACK … common` per common in request order, then an
        // `ACK <last> ready`, then the terminating NAK
        let mut expected = pkt(format!("ACK {} common\n", oid(b'c').to_hex()).as_bytes());
        expected.extend(pkt(
            format!("ACK {} common\n", oid(b'd').to_hex()).as_bytes()
        ));
        expected.extend(pkt(format!("ACK {} ready\n", oid(b'd').to_hex()).as_bytes()));
        expected.extend(pkt(b"NAK\n"));
        assert_eq!(block, expected);
    }

    #[test]
    fn should_omit_the_ready_ack_when_not_ready() {
        // given: one common but no ready signal
        let common = vec![oid(b'c')];

        // when
        let block = ack_negotiation_block(&common, false, true).expect("block");

        // then: the common is acknowledged, but no ready line precedes the NAK
        let mut expected = pkt(format!("ACK {} common\n", oid(b'c').to_hex()).as_bytes());
        expected.extend(pkt(b"NAK\n"));
        assert_eq!(block, expected);
    }

    #[test]
    fn should_send_only_nak_when_multi_ack_detailed_not_negotiated() {
        // given: commons and a ready signal, but the client did not negotiate
        // multi_ack_detailed
        let common = vec![oid(b'c')];

        // when
        let block = ack_negotiation_block(&common, true, false).expect("block");

        // then: none of the detailed lines are sent, only the NAK
        assert_eq!(block, pkt(b"NAK\n"));
    }

    #[test]
    fn should_ack_the_last_common_on_a_done_round() {
        // given: two commons
        let common = vec![oid(b'c'), oid(b'e')];

        // when
        let line = ack_done_line(&common).expect("line");

        // then: a single plain ACK of the last common (no status suffix)
        assert_eq!(
            line,
            pkt(format!("ACK {}\n", oid(b'e').to_hex()).as_bytes())
        );
    }

    #[test]
    fn should_nak_on_a_done_round_without_common() {
        // given/when: no common haves
        let line = ack_done_line(&[]).expect("line");

        // then
        assert_eq!(line, pkt(b"NAK\n"));
    }
}
