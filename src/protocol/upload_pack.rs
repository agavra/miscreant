//! The `git-upload-pack` endpoint: protocol-v2 command dispatch for the fetch
//! side of the smart-HTTP protocol.
//!
//! See `docs/0001-init.md` §Fetch API and §command=ls-refs. A request arrives
//! as a `command=<name>` pkt-line, then capability lines (`agent`,
//! `object-format`) up to a delimiter packet, then command arguments up to a
//! flush. The endpoint serves protocol v2 only — a missing or wrong
//! `Git-Protocol` header is a 400, mirroring the advertisement — and dispatches
//! on the command name. `ls-refs` is served here; `fetch`, `object-format`, and
//! `object-info` are recognized v2 commands not yet served and draw an `ERR`
//! pkt-line, as does any unknown command. All results use the
//! `application/x-git-upload-pack-result` content type.

use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::{Kind, TagRef};
use gix_packetline_blocking::{PacketLineRef, decode, encode};

use crate::AppState;
use crate::error::{self, Class, Classify};
use crate::protocol::{advertise, http};
use crate::storage::keys::RepoId;
use crate::storage::values::RefTarget;
use crate::storage::{ObjectDb, ObjectDbError, RepoMeta, Store, StoreError};

/// Content type of an upload-pack RPC result.
const RESULT_CONTENT_TYPE: &str = "application/x-git-upload-pack-result";

/// Upper bound on symref indirection when resolving a ref to an object id. A
/// chain longer than this leaves the ref unresolved and it is omitted from the
/// advertisement. A chain that dangles within the cap (its last hop names a
/// nonexistent ref) is also unresolved, but may still be advertised as an
/// unborn ref when the client requested `unborn`.
const MAX_SYMREF_DEPTH: usize = 5;

/// Upper bound on tag-object indirection when peeling a ref to its final
/// non-tag object. A chain longer than this leaves the ref unpeeled.
const MAX_PEEL_DEPTH: usize = 5;

/// Serve a `POST /<repo>/git-upload-pack` request. `repo` is the already
/// validated repository name.
pub async fn upload_pack(
    state: &AppState,
    repo: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    // Only protocol v2 is served; the advertisement requires the same header.
    if !http::wants_v2(headers) {
        return version_error();
    }

    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(ParseError) => {
            return http::plain(StatusCode::BAD_REQUEST, "malformed upload-pack request");
        }
    };

    // The server only ever creates and advertises SHA-1 repositories, so a
    // client naming any other object format cannot be served.
    if let Some(format) = &request.object_format
        && format != "sha1"
    {
        return err_line(&format!("unsupported object format: {format}"));
    }

    // Upload-pack never auto-creates: an unknown repository is a 404, matching
    // the advertisement.
    let meta = match state.store.lookup_repo(repo).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return http::plain(StatusCode::NOT_FOUND, "repository not found"),
        Err(err) => return error_response(&err),
    };

    match request.command.as_str() {
        "ls-refs" => ls_refs(state, &meta, &request.args).await,
        command @ ("fetch" | "object-format" | "object-info") => {
            err_line(&format!("unsupported command: {command}"))
        }
        command => err_line(&format!("unknown command: {command}")),
    }
}

/// A 400 carrying the `ERR` pkt-line the advertisement also uses when a request
/// does not opt into protocol v2.
fn version_error() -> Response {
    match advertise::error_line("git protocol version 2 required") {
        Ok(body) => http::git_response(StatusCode::BAD_REQUEST, RESULT_CONTENT_TYPE, body),
        Err(_) => http::internal_error(),
    }
}

/// An in-band protocol rejection: a single `ERR` pkt-line the client surfaces
/// as a fatal error, carried in an otherwise-successful result response.
fn err_line(message: &str) -> Response {
    match advertise::error_line(message) {
        Ok(body) => http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body),
        Err(_) => http::internal_error(),
    }
}

/// Map a domain error to its wire response through [`error::classify`]: a
/// client-caused failure becomes an in-band `ERR` pkt-line, an absent
/// repository a 404, and an internal fault a logged 500.
fn error_response<E: Classify + std::error::Error>(err: &E) -> Response {
    match error::classify(err) {
        Class::Client(reason) => err_line(&reason),
        Class::NotFound => http::plain(StatusCode::NOT_FOUND, "repository not found"),
        Class::Server => http::internal_error(),
    }
}

/// Serve the `ls-refs` command: advertise refs (optionally restricted by
/// `ref-prefix`), each resolved to an object id, with `symref-target` and
/// `peeled` attributes when requested. A symbolic ref that dangles within the
/// depth cap (e.g. HEAD on an empty repository) is reported as `unborn` when
/// requested, or omitted otherwise. Output is `<oid> <refname>` (or
/// `unborn <refname>`) lines in refname order (HEAD sorts first) terminated
/// by a flush.
async fn ls_refs(state: &AppState, meta: &RepoMeta, args: &[String]) -> Response {
    let parsed = match LsRefsArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(arg) => return err_line(&format!("unexpected ls-refs argument: {arg}")),
    };
    match build_ls_refs(state, meta.id, &parsed).await {
        Ok(body) => http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body),
        Err(err) => error_response(&err),
    }
}

/// Assemble the `ls-refs` response body: one pkt-line per resolvable ref,
/// terminated by a flush.
async fn build_ls_refs(
    state: &AppState,
    repo: RepoId,
    args: &LsRefsArgs,
) -> Result<Vec<u8>, LsRefsError> {
    let refs = gather_refs(&state.store, repo, &args.prefixes).await?;
    let mut body = Vec::new();
    for (name, target) in refs {
        let oid = match resolve_ref(&state.store, repo, &target).await? {
            Resolution::Resolved(oid) => oid,
            // A chain that dangles within the depth cap (e.g. HEAD on an
            // empty repository) has no object id, but is still a named
            // branch-to-be: advertise it as unborn when asked.
            Resolution::Dangling => {
                if args.unborn
                    && let RefTarget::Reference(inner) = &target
                {
                    let mut line = format!("unborn {name}");
                    if args.symrefs {
                        line.push_str(&format!(" symref-target:{inner}"));
                    }
                    line.push('\n');
                    encode::data_to_write(line.as_bytes(), &mut body)?;
                }
                continue;
            }
            // A chain exceeding the depth cap has no reliable target to
            // report and is omitted outright.
            Resolution::TooDeep => continue,
        };
        let mut line = format!("{} {name}", oid.to_hex());
        if args.symrefs
            && let RefTarget::Reference(inner) = &target
        {
            line.push_str(&format!(" symref-target:{inner}"));
        }
        if args.peel
            && let Some(peeled) = peel_ref(&state.objectdb, repo, oid).await?
        {
            line.push_str(&format!(" peeled:{}", peeled.to_hex()));
        }
        line.push('\n');
        encode::data_to_write(line.as_bytes(), &mut body)?;
    }
    encode::flush_to_write(&mut body)?;
    Ok(body)
}

/// Collect the refs to advertise, in refname order. With no prefixes every ref
/// is returned; otherwise the union of the refs under each prefix, deduplicated
/// and ordered by name.
async fn gather_refs(
    store: &Store,
    repo: RepoId,
    prefixes: &[String],
) -> Result<Vec<(String, RefTarget)>, StoreError> {
    if prefixes.is_empty() {
        return store.list_refs(repo, None).await;
    }
    let mut refs = std::collections::BTreeMap::new();
    for prefix in prefixes {
        for (name, target) in store.list_refs(repo, Some(prefix)).await? {
            refs.insert(name, target);
        }
    }
    Ok(refs.into_iter().collect())
}

/// The outcome of following a ref target to a direct object id.
enum Resolution {
    /// The chain terminated at a direct object id.
    Resolved(ObjectId),
    /// The chain dangled within the depth cap: some hop names a ref that does
    /// not exist (e.g. HEAD on an empty repository pointing at a branch that
    /// was never created).
    Dangling,
    /// The chain did not terminate within [`MAX_SYMREF_DEPTH`] hops.
    TooDeep,
}

/// Follow a ref target to a direct object id, chasing symref chains up to
/// [`MAX_SYMREF_DEPTH`].
async fn resolve_ref(
    store: &Store,
    repo: RepoId,
    target: &RefTarget,
) -> Result<Resolution, StoreError> {
    let mut current = target.clone();
    for _ in 0..MAX_SYMREF_DEPTH {
        match current {
            RefTarget::Direct(oid) => return Ok(Resolution::Resolved(oid)),
            RefTarget::Reference(name) => match store.get_ref(repo, &name).await? {
                Some(next) => current = next,
                None => return Ok(Resolution::Dangling),
            },
        }
    }
    Ok(Resolution::TooDeep)
}

/// Peel a ref's object to its final non-tag object, following tag chains up to
/// [`MAX_PEEL_DEPTH`]. Returns `Some(oid)` only when the ref points at a tag
/// object; a ref pointing directly at a commit, tree, or blob is not peeled,
/// and neither is a chain exceeding the cap.
async fn peel_ref(
    objectdb: &ObjectDb,
    repo: RepoId,
    oid: ObjectId,
) -> Result<Option<ObjectId>, LsRefsError> {
    let mut current = oid;
    let mut peeled = false;
    for _ in 0..MAX_PEEL_DEPTH {
        let Some((kind, body)) = objectdb.get(repo, &current).await? else {
            return Ok(None);
        };
        if kind != Kind::Tag {
            return Ok(peeled.then_some(current));
        }
        let tag =
            TagRef::from_bytes(&body, current.kind()).map_err(|source| LsRefsError::Decode {
                oid: current,
                source,
            })?;
        current = tag.target();
        peeled = true;
    }
    Ok(peeled.then_some(current))
}

/// A parsed protocol-v2 command request.
struct Request {
    /// The command name (the value after `command=`).
    command: String,
    /// The `object-format` capability value, if the client sent one.
    object_format: Option<String>,
    /// The command arguments (the pkt-lines after the delimiter packet).
    args: Vec<String>,
}

/// The request body was not framed as a valid protocol-v2 command request.
#[derive(Debug)]
struct ParseError;

/// Parse a command request: a `command=<name>` line and capability lines up to
/// a delimiter packet, then argument lines up to a flush. The delimiter and
/// arguments are optional — a command with no arguments omits both — but the
/// terminating flush is required.
fn parse_request(body: &Bytes) -> Result<Request, ParseError> {
    let mut offset = 0;
    let mut command = None;
    let mut object_format = None;
    let mut args = Vec::new();
    let mut in_args = false;

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
            PacketLineRef::Delimiter => {
                // A second delimiter has no place in a command request.
                if in_args {
                    return Err(ParseError);
                }
                in_args = true;
            }
            PacketLineRef::Data(data) => {
                let text = std::str::from_utf8(data.strip_suffix(b"\n").unwrap_or(data))
                    .map_err(|_| ParseError)?;
                if in_args {
                    args.push(text.to_owned());
                } else if let Some(name) = text.strip_prefix("command=") {
                    // A request carries exactly one command.
                    if command.replace(name.to_owned()).is_some() {
                        return Err(ParseError);
                    }
                } else if let Some(format) = text.strip_prefix("object-format=") {
                    object_format = Some(format.to_owned());
                }
                // Any other capability (e.g. `agent=…`) is accepted and ignored.
            }
            // A response-end packet is server-to-client only.
            PacketLineRef::ResponseEnd => return Err(ParseError),
        }
    }

    Ok(Request {
        command: command.ok_or(ParseError)?,
        object_format,
        args,
    })
}

/// Parsed `ls-refs` arguments.
#[derive(Debug, Default)]
struct LsRefsArgs {
    /// Ref-name prefixes restricting the advertisement (empty = all refs).
    prefixes: Vec<String>,
    /// Include a `symref-target` attribute for symbolic refs.
    symrefs: bool,
    /// Include a `peeled` attribute for refs pointing at tag objects.
    peel: bool,
    /// Report dangling symbolic refs (within the depth cap) as `unborn`
    /// instead of omitting them.
    unborn: bool,
}

impl LsRefsArgs {
    /// Parse `ls-refs` argument lines. An unrecognized argument is returned
    /// for an `ERR` reply.
    fn parse(args: &[String]) -> Result<Self, &str> {
        let mut parsed = LsRefsArgs::default();
        for arg in args {
            if let Some(prefix) = arg.strip_prefix("ref-prefix ") {
                parsed.prefixes.push(prefix.to_owned());
            } else {
                match arg.as_str() {
                    "symrefs" => parsed.symrefs = true,
                    "peel" => parsed.peel = true,
                    "unborn" => parsed.unborn = true,
                    other => return Err(other),
                }
            }
        }
        Ok(parsed)
    }
}

/// A failure while building the `ls-refs` response.
#[derive(Debug, thiserror::Error)]
enum LsRefsError {
    /// A ref-store read failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// An object read failed while peeling a tag.
    #[error(transparent)]
    Objects(#[from] ObjectDbError),
    /// A stored tag object that verified its SHA on ingest will not decode:
    /// storage corruption, never a client fault.
    #[error("cannot decode tag object {oid}")]
    Decode {
        /// The tag whose body failed to parse.
        oid: ObjectId,
        /// The underlying parse failure.
        #[source]
        source: gix_object::decode::Error,
    },
    /// Encoding a pkt-line into the in-memory response buffer failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Classify for LsRefsError {
    fn class(&self) -> Class {
        match self {
            LsRefsError::Store(e) => e.class(),
            LsRefsError::Objects(e) => e.class(),
            // Undecodable stored tags and buffer-encoding faults are internal.
            LsRefsError::Decode { .. } | LsRefsError::Io(_) => Class::Server,
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

    #[test]
    fn should_parse_command_capabilities_and_args() {
        // given: a full ls-refs request — command, a capability, a delimiter,
        // then two arguments, terminated by a flush
        let mut body = pkt(b"command=ls-refs\n");
        body.extend(pkt(b"object-format=sha1\n"));
        body.extend_from_slice(b"0001");
        body.extend(pkt(b"symrefs\n"));
        body.extend(pkt(b"ref-prefix refs/heads/\n"));
        body.extend_from_slice(b"0000");

        // when
        let request = parse_request(&Bytes::from(body)).expect("parse");

        // then: the command, object-format, and arguments are separated
        assert_eq!(request.command, "ls-refs");
        assert_eq!(request.object_format.as_deref(), Some("sha1"));
        assert_eq!(request.args, vec!["symrefs", "ref-prefix refs/heads/"]);
    }

    #[test]
    fn should_parse_a_command_with_no_delimiter_or_args() {
        // given: a bare command followed directly by a flush
        let mut body = pkt(b"command=ls-refs\n");
        body.extend_from_slice(b"0000");

        // when
        let request = parse_request(&Bytes::from(body)).expect("parse");

        // then
        assert_eq!(request.command, "ls-refs");
        assert!(request.args.is_empty());
    }

    #[test]
    fn should_reject_a_body_without_a_command() {
        // given: capabilities and a flush but no command line
        let mut body = pkt(b"agent=git/2\n");
        body.extend_from_slice(b"0000");

        // when/then
        assert!(parse_request(&Bytes::from(body)).is_err());
    }

    #[test]
    fn should_reject_a_body_without_a_flush() {
        // given: a command line with no terminating flush
        let body = pkt(b"command=ls-refs\n");

        // when/then
        assert!(parse_request(&Bytes::from(body)).is_err());
    }

    #[test]
    fn should_collect_ref_prefixes_and_flags() {
        // given: two prefixes plus the symrefs and peel flags
        let args = vec![
            "ref-prefix refs/heads/".to_owned(),
            "peel".to_owned(),
            "ref-prefix refs/tags/".to_owned(),
            "symrefs".to_owned(),
        ];

        // when
        let parsed = LsRefsArgs::parse(&args).expect("parse args");

        // then
        assert_eq!(parsed.prefixes, vec!["refs/heads/", "refs/tags/"]);
        assert!(parsed.symrefs);
        assert!(parsed.peel);
    }

    #[test]
    fn should_parse_the_unborn_argument() {
        // given/when
        let parsed = LsRefsArgs::parse(&["unborn".to_owned()]).expect("parse args");

        // then: only the unborn flag is set
        assert!(parsed.prefixes.is_empty());
        assert!(!parsed.symrefs);
        assert!(!parsed.peel);
        assert!(parsed.unborn);
    }

    #[test]
    fn should_reject_an_unknown_ls_refs_argument() {
        // given/when/then
        let args = ["deepen 1".to_owned()];
        let err = LsRefsArgs::parse(&args).unwrap_err();
        assert_eq!(err, "deepen 1");
    }
}
