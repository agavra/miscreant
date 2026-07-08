//! The `git-upload-pack` endpoint: protocol-v2 command dispatch for the fetch
//! side of the smart-HTTP protocol.
//!
//! See `docs/0001-init.md` §Fetch API, §command=ls-refs, and §command=fetch.
//! A request arrives as a `command=<name>` pkt-line, then capability lines
//! (`agent`, `object-format`) up to a delimiter packet, then command
//! arguments up to a flush. The endpoint serves protocol v2 only — a missing
//! or wrong `Git-Protocol` header is a 400, mirroring the advertisement —
//! and dispatches on the command name. `ls-refs` and `fetch` are served
//! here; `object-format` and `object-info` are recognized v2 commands not
//! yet served and draw an `ERR` pkt-line, as does any unknown command. All
//! results use the `application/x-git-upload-pack-result` content type.
//!
//! `fetch` negotiation is single-round: a request without `done` is answered
//! with an `acknowledgments` section (`ACK` per recognized have, `NAK` when
//! there are none) followed by `ready`, a delimiter, and the `packfile`
//! section in the same response; with `done` the `packfile` section comes
//! directly. Pack bytes are streamed multiplexed over side-band-64k, never
//! buffered whole.

use std::convert::Infallible;
use std::io;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use gix_hash::ObjectId;
use gix_object::{Kind, TagRef};
use gix_packetline_blocking::{Channel, PacketLineRef, decode, encode};
use tokio::sync::mpsc;

use crate::AppState;
use crate::error::{self, Class, Classify};
use crate::git::pack_out::{PackOutError, write_pack};
use crate::git::walk::{FilterSpec, Walker};
use crate::protocol::{MAX_BAND_DATA, advertise, http};
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

/// Number of object-database lookups kept in flight while feeding the pack
/// writer. Lookups run concurrently against storage, but their results are
/// consumed in collected order, so the emitted pack is deterministic.
const OBJECT_LOOKAHEAD: usize = 32;

/// Capacity, in side-band packets, of the channel between the blocking pack
/// writer and the HTTP response stream. Each packet carries at most
/// [`MAX_BAND_DATA`] bytes, so this bounds the response memory held per
/// in-flight fetch.
const BODY_CHANNEL_PACKETS: usize = 8;

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
        "fetch" => fetch(state, &meta, &request.args).await,
        command @ ("object-format" | "object-info") => {
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

/// Serve the `fetch` command: run commit discovery and object collection
/// (`docs/0001-init.md` §command=fetch Phases I and II), then stream the
/// pack (Phase III). Walk failures surface before the response body starts,
/// as an in-band `ERR` pkt-line through the same classifier `ls-refs` uses;
/// once streaming has begun, a failure can only be reported on side-band
/// channel 3.
async fn fetch(state: &AppState, meta: &RepoMeta, args: &[String]) -> Response {
    let parsed = match FetchArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(message) => return err_line(&message),
    };
    if parsed.wants.is_empty() {
        return err_line("fetch requires at least one want");
    }

    let walker = Walker::new(state.store.clone(), state.objectdb.clone(), meta.id);
    let selection = match walker.select_commits(&parsed.wants, &parsed.haves).await {
        Ok(selection) => selection,
        Err(err) => return error_response(&err),
    };
    let collected = match walker
        .collect(&selection.to_send, &selection.common, &parsed.filter)
        .await
    {
        Ok(collected) => collected,
        Err(err) => return error_response(&err),
    };
    let Ok(count) = u32::try_from(collected.len()) else {
        return err_line("too many objects for one pack");
    };

    let prefix = match negotiation_prefix(parsed.done, &selection.common) {
        Ok(prefix) => prefix,
        Err(_) => return http::internal_error(),
    };

    // Look up object contents concurrently (up to OBJECT_LOOKAHEAD in
    // flight), hand them in collected order to the blocking pack writer,
    // and stream its side-band packets out as the response body.
    let (object_tx, object_rx) = mpsc::channel(OBJECT_LOOKAHEAD);
    tokio::spawn(feed_objects(
        state.objectdb.clone(),
        meta.id,
        collected,
        object_tx,
    ));

    let (body_tx, body_rx) = mpsc::channel::<Bytes>(BODY_CHANNEL_PACKETS);
    let object_hash = meta.object_format;
    let no_progress = parsed.no_progress;
    tokio::task::spawn_blocking(move || {
        stream_pack(object_rx, body_tx, count, object_hash, no_progress);
    });

    let body = Body::from_stream(
        stream::iter([Ok::<_, Infallible>(Bytes::from(prefix))])
            .chain(stream::unfold(body_rx, |mut rx| async move {
                rx.recv().await.map(|chunk| (Ok(chunk), rx))
            })),
    );
    http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body)
}

/// The pkt-lines that precede the pack bytes. Without `done` this is the
/// `acknowledgments` section — `ACK` per recognized have or a lone `NAK` —
/// then `ready` (every want resolved, or the request would have failed
/// already) and a delimiter; either way it ends with the `packfile` section
/// header.
fn negotiation_prefix(done: bool, common: &[ObjectId]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if !done {
        encode::data_to_write(b"acknowledgments\n", &mut out)?;
        if common.is_empty() {
            encode::data_to_write(b"NAK\n", &mut out)?;
        } else {
            for oid in common {
                encode::data_to_write(format!("ACK {}\n", oid.to_hex()).as_bytes(), &mut out)?;
            }
        }
        encode::data_to_write(b"ready\n", &mut out)?;
        encode::delim_to_write(&mut out)?;
    }
    encode::data_to_write(b"packfile\n", &mut out)?;
    Ok(out)
}

/// Read each collected object's kind and body from the object database with
/// up to [`OBJECT_LOOKAHEAD`] lookups in flight, forwarding results to the
/// pack writer in collected order. Stops at the first failure (forwarded so
/// the writer can report it) or when the writer hangs up.
async fn feed_objects(
    objectdb: ObjectDb,
    repo: RepoId,
    oids: Vec<ObjectId>,
    tx: mpsc::Sender<Result<(ObjectId, Kind, Bytes), FetchObjectError>>,
) {
    let mut lookups = stream::iter(oids)
        .map(|oid| {
            let objectdb = objectdb.clone();
            async move { (oid, objectdb.get(repo, &oid).await) }
        })
        .buffered(OBJECT_LOOKAHEAD);
    while let Some((oid, result)) = lookups.next().await {
        let item = match result {
            Ok(Some((kind, body))) => Ok((oid, kind, body)),
            Ok(None) => Err(FetchObjectError::Missing(oid)),
            Err(err) => Err(FetchObjectError::Objects(err)),
        };
        let stop = item.is_err();
        if tx.send(item).await.is_err() || stop {
            return;
        }
    }
}

/// The blocking half of the fetch response: pull looked-up objects from
/// `objects`, compress them into pack entries, and send the pack multiplexed
/// as side-band channel-1 packets on `body`, with progress on channel 2
/// (unless suppressed) and a flush pkt closing a successful response. A
/// failure after streaming has begun is reported on channel 3 — the only
/// remaining path to the client — except when the client itself hung up.
fn stream_pack(
    mut objects: mpsc::Receiver<Result<(ObjectId, Kind, Bytes), FetchObjectError>>,
    body: mpsc::Sender<Bytes>,
    count: u32,
    object_hash: gix_hash::Kind,
    no_progress: bool,
) {
    if !no_progress {
        let line = format!("packing {count} objects\n");
        let _ = send_band(&body, Channel::Progress, line.as_bytes());
    }

    let mut writer = BandWriter {
        tx: body.clone(),
        buf: Vec::new(),
    };
    let input = std::iter::from_fn(|| objects.blocking_recv());
    match write_pack(input, count, object_hash, &mut writer) {
        Ok(_) => {
            if !no_progress {
                let line = format!("packed {count} objects\n");
                let _ = send_band(&body, Channel::Progress, line.as_bytes());
            }
            let mut flush = Vec::new();
            if encode::flush_to_write(&mut flush).is_ok() {
                let _ = body.blocking_send(Bytes::from(flush));
            }
        }
        Err(err) => {
            // A hung-up client is not a fault and has no one left to tell.
            if let PackOutError::Write(gix_hash::io::Error::Io(io_err)) = &err
                && io_err.kind() == io::ErrorKind::BrokenPipe
            {
                return;
            }
            let reason = match error::classify(&err) {
                Class::Client(reason) => reason,
                Class::NotFound | Class::Server => "internal error".to_owned(),
            };
            let _ = send_band(&body, Channel::Error, reason.as_bytes());
        }
    }
}

/// Encode one side-band packet on `channel` and send it to the response
/// body. A closed channel (the client went away) surfaces as `BrokenPipe`.
fn send_band(tx: &mpsc::Sender<Bytes>, channel: Channel, data: &[u8]) -> io::Result<()> {
    let mut packet = Vec::with_capacity(data.len() + 5);
    encode::band_to_write(channel, data, &mut packet)?;
    tx.blocking_send(Bytes::from(packet))
        .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
}

/// An `io::Write` that frames written bytes into side-band-64k channel-1
/// (pack data) packets, sending each completed packet to the HTTP response
/// channel. Bytes accumulate until a full packet is available; `flush` sends
/// any partial remainder.
struct BandWriter {
    tx: mpsc::Sender<Bytes>,
    buf: Vec<u8>,
}

impl io::Write for BandWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        while self.buf.len() >= MAX_BAND_DATA {
            let rest = self.buf.split_off(MAX_BAND_DATA);
            let full = std::mem::replace(&mut self.buf, rest);
            send_band(&self.tx, Channel::Data, &full)?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let data = std::mem::take(&mut self.buf);
            send_band(&self.tx, Channel::Data, &data)?;
        }
        Ok(())
    }
}

/// A failure while looking up a collected object for packing.
#[derive(Debug, thiserror::Error)]
enum FetchObjectError {
    /// The object database read failed.
    #[error(transparent)]
    Objects(#[from] ObjectDbError),
    /// A collected object has no record. The walk selected it from live
    /// state moments earlier, so absence indicates storage corruption.
    #[error("object {0} is missing from storage")]
    Missing(ObjectId),
}

impl Classify for FetchObjectError {
    fn class(&self) -> Class {
        match self {
            FetchObjectError::Objects(e) => e.class(),
            FetchObjectError::Missing(_) => Class::Server,
        }
    }
}

/// Parsed `fetch` arguments.
#[derive(Debug, Default)]
struct FetchArgs {
    /// The objects the client asks for (tips it saw advertised).
    wants: Vec<ObjectId>,
    /// Commits the client claims to already have.
    haves: Vec<ObjectId>,
    /// The client is done negotiating and wants the pack immediately.
    done: bool,
    /// Suppress side-band progress messages.
    no_progress: bool,
    /// The partial-clone filter restricting which blobs are sent.
    filter: FilterSpec,
}

impl FetchArgs {
    /// Parse `fetch` argument lines. `thin-pack`, `ofs-delta`, and
    /// `include-tag` are accepted and ignored: the server only sends full
    /// (non-delta) entries, which every client accepts, and tag objects are
    /// sent only for explicitly wanted tag refs. A `filter` argument naming a
    /// spec [`FilterSpec::parse`] does not recognize, or any other
    /// unrecognized argument, is rejected with the returned `ERR` message.
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut parsed = FetchArgs::default();
        for arg in args {
            if let Some(hex) = arg.strip_prefix("want ") {
                let oid = ObjectId::from_hex(hex.as_bytes())
                    .map_err(|_| format!("invalid want: {hex}"))?;
                parsed.wants.push(oid);
            } else if let Some(hex) = arg.strip_prefix("have ") {
                let oid = ObjectId::from_hex(hex.as_bytes())
                    .map_err(|_| format!("invalid have: {hex}"))?;
                parsed.haves.push(oid);
            } else if let Some(spec) = arg.strip_prefix("filter ") {
                parsed.filter =
                    FilterSpec::parse(spec).ok_or_else(|| format!("unsupported filter: {spec}"))?;
            } else {
                match arg.as_str() {
                    "done" => parsed.done = true,
                    "no-progress" => parsed.no_progress = true,
                    "thin-pack" | "ofs-delta" | "include-tag" => {}
                    other => return Err(format!("unexpected fetch argument: {other}")),
                }
            }
        }
        Ok(parsed)
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

    /// A distinct SHA-1 object id built from a single hex nibble.
    fn oid(nibble: u8) -> ObjectId {
        ObjectId::from_hex(&[nibble; 40]).expect("valid sha1 hex")
    }

    #[test]
    fn should_parse_fetch_wants_haves_and_flags() {
        // given: the argument mix a real client sends mid-negotiation
        let args = vec![
            format!("want {}", oid(b'a')),
            format!("want {}", oid(b'b')),
            "thin-pack".to_owned(),
            "ofs-delta".to_owned(),
            "include-tag".to_owned(),
            "no-progress".to_owned(),
            format!("have {}", oid(b'c')),
            "done".to_owned(),
        ];

        // when
        let parsed = FetchArgs::parse(&args).expect("parse args");

        // then: wants/haves keep order; ignored args set no flags
        assert_eq!(parsed.wants, vec![oid(b'a'), oid(b'b')]);
        assert_eq!(parsed.haves, vec![oid(b'c')]);
        assert!(parsed.done);
        assert!(parsed.no_progress);
    }

    #[test]
    fn should_parse_a_blob_none_filter_argument() {
        // given/when
        let parsed = FetchArgs::parse(&["filter blob:none".to_owned()]).expect("parse args");

        // then
        assert_eq!(parsed.filter, FilterSpec::BlobNone);
    }

    #[test]
    fn should_parse_a_blob_limit_filter_argument() {
        // given/when
        let parsed = FetchArgs::parse(&["filter blob:limit=1k".to_owned()]).expect("parse args");

        // then
        assert_eq!(parsed.filter, FilterSpec::BlobLimit(1024));
    }

    #[test]
    fn should_reject_an_unsupported_fetch_filter_argument() {
        // given/when
        let err = FetchArgs::parse(&["filter tree:0".to_owned()]).unwrap_err();

        // then
        assert_eq!(err, "unsupported filter: tree:0");
    }

    #[test]
    fn should_reject_an_unknown_fetch_argument() {
        // given/when
        let err = FetchArgs::parse(&["deepen 1".to_owned()]).unwrap_err();

        // then
        assert_eq!(err, "unexpected fetch argument: deepen 1");
    }

    #[test]
    fn should_reject_a_malformed_want_oid() {
        // given/when
        let err = FetchArgs::parse(&["want not-hex".to_owned()]).unwrap_err();

        // then
        assert_eq!(err, "invalid want: not-hex");
    }

    #[test]
    fn should_open_with_acknowledgments_and_ready_when_not_done() {
        // given: one recognized have
        let common = vec![oid(b'd')];

        // when
        let prefix = negotiation_prefix(false, &common).expect("build prefix");

        // then: acknowledgments, the ACK, ready, a delimiter, then the
        // packfile section header
        let mut expected = pkt(b"acknowledgments\n");
        expected.extend(pkt(format!("ACK {}\n", oid(b'd')).as_bytes()));
        expected.extend(pkt(b"ready\n"));
        expected.extend_from_slice(b"0001");
        expected.extend(pkt(b"packfile\n"));
        assert_eq!(prefix, expected);
    }

    #[test]
    fn should_nak_when_no_haves_are_recognized() {
        // given/when
        let prefix = negotiation_prefix(false, &[]).expect("build prefix");

        // then: a lone NAK stands in for the missing ACKs
        let mut expected = pkt(b"acknowledgments\n");
        expected.extend(pkt(b"NAK\n"));
        expected.extend(pkt(b"ready\n"));
        expected.extend_from_slice(b"0001");
        expected.extend(pkt(b"packfile\n"));
        assert_eq!(prefix, expected);
    }

    #[test]
    fn should_skip_acknowledgments_after_done() {
        // given/when: the client already said done
        let prefix = negotiation_prefix(true, &[oid(b'e')]).expect("build prefix");

        // then: the packfile section starts immediately
        assert_eq!(prefix, pkt(b"packfile\n"));
    }
}
