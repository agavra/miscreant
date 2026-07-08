//! The `git-receive-pack` endpoint: the push side of the smart-HTTP protocol.
//!
//! See `docs/0001-init.md` §Receive API. A push arrives as pkt-line ref-update
//! commands — `<old-oid> <new-oid> <refname>`, the first carrying the client's
//! capabilities after a NUL — terminated by a flush, then a raw pack whenever
//! any command creates or updates a ref. A zero old-oid is a create (the ref
//! must not yet exist); a zero new-oid is a delete. The handler ingests and
//! validates the pack against committed storage, promotes its objects,
//! applies the ref updates as compare-and-swap, and returns a report-status
//! result. Push-level failures (a bad pack, a push that is not self-contained)
//! are reported as `unpack <reason>` with every ref `ng`; the HTTP status is
//! 200 even when refs are rejected. Only malformed request framing is a 4xx.

use std::io;

use axum::http::StatusCode;
use axum::response::Response;
use bytes::Bytes;
use gix_hash::ObjectId;
use gix_packetline_blocking::{Channel, PacketLineRef, decode, encode};

use crate::AppState;
use crate::error::{self, Class};
use crate::git::{ingest_pack, validate_and_promote};
use crate::protocol::http;
use crate::storage::store::{RefOutcome, RefUpdate, RefUpdateResult, RepoMeta};

/// Content type of a receive-pack RPC result.
const RESULT_CONTENT_TYPE: &str = "application/x-git-receive-pack-result";

/// Largest payload per side-band data packet: the wire limit (65516) minus
/// the one-byte channel prefix side-band framing prepends.
const MAX_BAND_DATA: usize = 65515;

/// The reason attached to every ref when the pack itself could not be
/// processed. It mirrors git's own per-command status in that case, where the
/// authoritative failure is carried by the preceding `unpack <reason>` line.
const NG_UNPACKER_ERROR: &str = "unpacker error";

/// Serve a `POST /<repo>/git-receive-pack` request. `repo` is the already
/// validated repository name.
pub async fn receive_pack(state: &AppState, repo: &str, body: Bytes) -> Response {
    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(ParseError) => {
            return http::plain(StatusCode::BAD_REQUEST, "malformed receive-pack request");
        }
    };

    // Resolve or auto-create the repository, mirroring the advertisement.
    let meta = match resolve_repo(state, repo).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return http::plain(StatusCode::NOT_FOUND, "repository not found"),
        Err(()) => return http::internal_error(),
    };

    // Ingest the pack. A delete-only push carries none, and `ingest_pack`
    // treats zero bytes as an empty pack, so this is safe to call regardless.
    let staged = match ingest_pack(
        io::Cursor::new(request.pack.clone()),
        &state.objectdb,
        &meta,
        &state.config.staging_root,
    )
    .await
    {
        Ok(staged) => staged,
        Err(err) => return push_failed(&request, error::classify(&err)),
    };

    // Validate connectivity and promote the new objects, with the non-delete
    // new-oids as the reachability tips.
    let tips: Vec<ObjectId> = request.commands.iter().filter_map(|cmd| cmd.new).collect();
    if let Err(err) =
        validate_and_promote(&staged, &tips, &state.objectdb, &state.store, meta.id).await
    {
        return push_failed(&request, error::classify(&err));
    }

    // Apply the ref updates as compare-and-swap.
    let updates: Vec<RefUpdate> = request
        .commands
        .iter()
        .map(|cmd| RefUpdate {
            name: cmd.name.clone(),
            expected_old: cmd.old,
            new: cmd.new,
        })
        .collect();
    let results = match state.store.update_refs(meta.id, &updates).await {
        Ok(results) => results,
        Err(err) => {
            // A ref-update failure is always a server fault; classify to log
            // its full chain, then return a generic 500.
            let _ = error::classify(&err);
            return http::internal_error();
        }
    };

    report_success(&request, &results)
}

/// Resolve the target repository, auto-creating it when configured (a
/// receive-pack POST mirrors its advertisement, which already auto-created).
/// `Ok(None)` means auto-create is off and the repository does not exist;
/// `Err(())` means a store fault (already logged).
async fn resolve_repo(state: &AppState, repo: &str) -> Result<Option<RepoMeta>, ()> {
    let result = if state.config.auto_create_repos {
        state.store.get_or_create_repo(repo).await.map(Some)
    } else {
        state.store.lookup_repo(repo).await
    };
    result.map_err(|err| {
        let _ = error::classify(&err);
    })
}

/// Render a push-level failure. A [`Class::Client`] failure is a rejected push
/// reported as `unpack <reason>` with every command `ng` (HTTP 200); anything
/// else is a 404 or a generic 500.
fn push_failed(request: &Request, class: Class) -> Response {
    match class {
        Class::Client(reason) => {
            let statuses = request
                .commands
                .iter()
                .map(|cmd| {
                    (
                        cmd.name.clone(),
                        RefStatus::Ng(NG_UNPACKER_ERROR.to_owned()),
                    )
                })
                .collect::<Vec<_>>();
            build_report(&request.caps, &format!("unpack {reason}"), &statuses)
        }
        Class::NotFound => http::plain(StatusCode::NOT_FOUND, "repository not found"),
        Class::Server => http::internal_error(),
    }
}

/// Render a successful pack: `unpack ok` followed by each command's
/// compare-and-swap outcome.
fn report_success(request: &Request, results: &[RefUpdateResult]) -> Response {
    let statuses = results
        .iter()
        .map(|result| {
            let status = match &result.outcome {
                RefOutcome::Updated => RefStatus::Ok,
                RefOutcome::Rejected(reason) => RefStatus::Ng(reason.clone()),
            };
            (result.name.clone(), status)
        })
        .collect::<Vec<_>>();
    build_report(&request.caps, "unpack ok", &statuses)
}

/// A single command's outcome in the report-status body.
enum RefStatus {
    Ok,
    Ng(String),
}

/// Build the report-status response: the `unpack` line, one `ok`/`ng` line per
/// command, and a terminating flush, multiplexed over side-band channel 1 when
/// the client requested `side-band-64k`. When the client did not request
/// `report-status` it wants no report, so the body is empty. The HTTP status
/// is always 200.
fn build_report(caps: &Caps, unpack_line: &str, statuses: &[(String, RefStatus)]) -> Response {
    if !caps.report_status {
        return http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, Vec::new());
    }
    match report_body(unpack_line, statuses, caps.side_band_64k) {
        Ok(body) => http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body),
        Err(_) => http::internal_error(),
    }
}

/// Encode the report-status pkt-lines, wrapping them in side-band data packets
/// when `side_band` is set.
fn report_body(
    unpack_line: &str,
    statuses: &[(String, RefStatus)],
    side_band: bool,
) -> io::Result<Vec<u8>> {
    let mut inner = Vec::new();
    encode::data_to_write(format!("{unpack_line}\n").as_bytes(), &mut inner)?;
    for (name, status) in statuses {
        let line = match status {
            RefStatus::Ok => format!("ok {name}\n"),
            RefStatus::Ng(reason) => format!("ng {name} {reason}\n"),
        };
        encode::data_to_write(line.as_bytes(), &mut inner)?;
    }
    encode::flush_to_write(&mut inner)?;

    if !side_band {
        return Ok(inner);
    }
    let mut outer = Vec::new();
    for chunk in inner.chunks(MAX_BAND_DATA) {
        encode::band_to_write(Channel::Data, chunk, &mut outer)?;
    }
    encode::flush_to_write(&mut outer)?;
    Ok(outer)
}

/// One parsed ref-update command. `old`/`new` are `None` for the all-zero
/// object id, marking a create (as `old`) or a delete (as `new`).
struct Command {
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    name: String,
}

/// The client capabilities the endpoint acts on. `delete-refs` and `agent`
/// are accepted and need no state; every other capability is ignored.
#[derive(Default)]
struct Caps {
    /// The client wants a report-status response.
    report_status: bool,
    /// The report must be multiplexed over side-band channel 1.
    side_band_64k: bool,
}

/// A fully parsed receive-pack request body.
struct Request {
    commands: Vec<Command>,
    caps: Caps,
    /// The raw pack bytes following the command list (empty for a delete-only
    /// push, which carries none).
    pack: Bytes,
}

/// The request body was not framed as a valid receive-pack request.
#[derive(Debug)]
struct ParseError;

/// Parse the command list (up to its flush) and take the trailing pack bytes.
fn parse_request(body: &Bytes) -> Result<Request, ParseError> {
    let mut offset = 0;
    let mut commands = Vec::new();
    let mut caps = Caps::default();
    let mut first = true;

    loop {
        let (line, consumed) = match decode::streaming(&body[offset..]) {
            Ok(decode::Stream::Complete {
                line,
                bytes_consumed,
            }) => (line, bytes_consumed),
            // Incomplete input (including an empty body with no flush) or a bad
            // length prefix is malformed framing.
            _ => return Err(ParseError),
        };
        offset += consumed;
        match line {
            PacketLineRef::Flush => break,
            PacketLineRef::Data(data) => {
                let (command, line_caps) = parse_command(data)?;
                if first {
                    if let Some(line_caps) = line_caps {
                        caps = line_caps;
                    }
                    first = false;
                }
                commands.push(command);
            }
            // A delimiter or response-end packet has no place in a push.
            _ => return Err(ParseError),
        }
    }

    Ok(Request {
        commands,
        caps,
        pack: body.slice(offset..),
    })
}

/// Parse one command line into a [`Command`] and, when a NUL is present (only
/// the first command carries it), the capabilities that follow.
fn parse_command(data: &[u8]) -> Result<(Command, Option<Caps>), ParseError> {
    // The command line may or may not end in LF; drop one if present.
    let data = data.strip_suffix(b"\n").unwrap_or(data);
    let (command_bytes, caps) = match data.iter().position(|&byte| byte == 0) {
        Some(nul) => (&data[..nul], Some(parse_caps(&data[nul + 1..]))),
        None => (data, None),
    };

    let text = std::str::from_utf8(command_bytes).map_err(|_| ParseError)?;
    let mut fields = text.splitn(3, ' ');
    let old = fields.next().ok_or(ParseError)?;
    let new = fields.next().ok_or(ParseError)?;
    let name = fields.next().ok_or(ParseError)?;
    if name.is_empty() {
        return Err(ParseError);
    }

    Ok((
        Command {
            old: parse_oid(old)?,
            new: parse_oid(new)?,
            name: name.to_owned(),
        },
        caps,
    ))
}

/// Parse a hex object id, mapping the all-zero id to `None` (create/delete).
fn parse_oid(hex: &str) -> Result<Option<ObjectId>, ParseError> {
    let id = ObjectId::from_hex(hex.as_bytes()).map_err(|_| ParseError)?;
    Ok((!id.is_null()).then_some(id))
}

/// Parse the space-separated capability list, recording only the capabilities
/// the endpoint acts on. A capability may carry a `=value` suffix (e.g.
/// `agent=git/2.x`), and the list is preceded by a leading space, so an empty
/// token is expected and simply matches nothing.
fn parse_caps(bytes: &[u8]) -> Caps {
    let mut caps = Caps::default();
    for token in String::from_utf8_lossy(bytes).split(' ') {
        match token.split('=').next().unwrap_or(token) {
            "report-status" => caps.report_status = true,
            "side-band-64k" => caps.side_band_64k = true,
            _ => {}
        }
    }
    caps
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

    fn hex(byte: u8) -> String {
        String::from_utf8(vec![byte; 40]).unwrap()
    }

    #[test]
    fn should_parse_a_create_command_with_capabilities() {
        // given: one create command carrying caps after a NUL, then a flush
        // and a (stand-in) pack, exactly as a real client frames it
        let new = hex(b'a');
        let first = format!(
            "{} {new} refs/heads/main\0 report-status side-band-64k agent=git/2",
            "0".repeat(40)
        );
        let mut body = pkt(first.as_bytes());
        body.extend_from_slice(b"0000");
        body.extend_from_slice(b"PACKDATA");
        let body = Bytes::from(body);

        // when
        let request = parse_request(&body).expect("parse");

        // then: the create is recognized (zero old-oid -> None) and caps read
        assert_eq!(request.commands.len(), 1);
        assert_eq!(request.commands[0].old, None);
        assert_eq!(
            request.commands[0].new,
            Some(ObjectId::from_hex(new.as_bytes()).unwrap())
        );
        assert_eq!(request.commands[0].name, "refs/heads/main");
        assert!(request.caps.report_status);
        assert!(request.caps.side_band_64k);
        assert_eq!(request.pack.as_ref(), b"PACKDATA");
    }

    #[test]
    fn should_parse_a_delete_only_push_with_no_pack() {
        // given: a single delete command (zero new-oid) then a flush, no pack
        let old = hex(b'b');
        let line = format!("{old} {} refs/heads/gone\0report-status", "0".repeat(40));
        let mut body = pkt(line.as_bytes());
        body.extend_from_slice(b"0000");
        let body = Bytes::from(body);

        // when
        let request = parse_request(&body).expect("parse");

        // then
        assert_eq!(request.commands[0].new, None);
        assert_eq!(
            request.commands[0].old,
            Some(ObjectId::from_hex(old.as_bytes()).unwrap())
        );
        assert!(request.pack.is_empty());
    }

    #[test]
    fn should_reject_a_body_without_a_flush() {
        // given: a command line with no terminating flush
        let line = format!("{} {} refs/heads/main", "0".repeat(40), hex(b'a'));
        let body = Bytes::from(pkt(line.as_bytes()));

        // when/then
        assert!(parse_request(&body).is_err());
    }

    #[test]
    fn should_reject_an_empty_body() {
        // given/when/then: no pkt-lines at all is malformed framing
        assert!(parse_request(&Bytes::new()).is_err());
    }

    #[test]
    fn should_wrap_the_report_in_side_band_when_requested() {
        // given: a one-command success report requested over side-band
        let statuses = vec![("refs/heads/main".to_owned(), RefStatus::Ok)];

        // when
        let plain = report_body("unpack ok", &statuses, false).expect("plain");
        let banded = report_body("unpack ok", &statuses, true).expect("banded");

        // then: the plain body is a bare pkt-line stream, while the banded one
        // carries the same bytes inside a channel-1 data packet
        assert!(plain.starts_with(b"000e"));
        // side-band packet: length prefix, channel byte 0x01, then the inner
        // report, and a trailing outer flush.
        assert_eq!(banded[4], Channel::Data as u8);
        assert!(banded.ends_with(b"0000"));
        assert_eq!(&banded[5..5 + plain.len()], plain.as_slice());
    }
}
