//! The `git-upload-pack` endpoint: protocol-v2 command dispatch for the fetch
//! side of the smart-HTTP protocol.
//!
//! See `docs/0001-init.md` §Fetch API, §command=ls-refs, §command=fetch,
//! §command=object-format, and §command=object-info. A request arrives as a
//! `command=<name>` pkt-line, then capability lines (`agent`,
//! `object-format`) up to a delimiter packet, then command arguments up to a
//! flush. The endpoint serves protocol v2 only — a missing or wrong
//! `Git-Protocol` header is a 400, mirroring the advertisement — and
//! dispatches on the command name: `ls-refs`, `fetch`, `object-format`, and
//! `object-info` are all served here; any other command draws an `ERR`
//! pkt-line. All results use the `application/x-git-upload-pack-result`
//! content type.
//!
//! `fetch` negotiation is single-round: a request without `done` is answered
//! with an `acknowledgments` section (`ACK` per recognized have, `NAK` when
//! there are none) followed by `ready`, a delimiter, and the `packfile`
//! section in the same response; with `done` the `packfile` section comes
//! directly. Pack bytes are streamed multiplexed over side-band-64k, never
//! buffered whole.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::io;
use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use gix_hash::ObjectId;
use gix_object::{Kind, TagRef};
use gix_packetline_blocking::{Channel, PacketLineRef, decode, encode};
use tokio::sync::mpsc;
use tracing::{Instrument, Span};

use crate::AppState;
use crate::error::{self, Class, Classify};
use crate::git::pack_out::{PackEntry, PackOptions, PackOutError, write_pack_with_options};
use crate::git::walk::{CollectedObjects, FilterSpec, WalkError, Walker};
use crate::protocol::{MAX_BAND_DATA, advertise, http};
use crate::storage::keys::RepoId;
use crate::storage::values::RefTarget;
use crate::storage::{ObjectDb, ObjectDbError, RepoMeta, Store, StoreError};

/// Content type of an upload-pack RPC result. Shared with the classic (v0)
/// fetch handler, which produces the same result media type.
pub(super) const RESULT_CONTENT_TYPE: &str = "application/x-git-upload-pack-result";

/// Upper bound on symref indirection when resolving a ref to an object id. A
/// chain longer than this leaves the ref unresolved and it is omitted from the
/// advertisement. A chain that dangles within the cap (its last hop names a
/// nonexistent ref) is also unresolved, but may still be advertised as an
/// unborn ref when the client requested `unborn`.
pub(super) const MAX_SYMREF_DEPTH: usize = 5;

/// Upper bound on tag-object indirection when peeling a ref to its final
/// non-tag object. A chain longer than this leaves the ref unpeeled.
pub(super) const MAX_PEEL_DEPTH: usize = 5;

/// Number of object-database lookups kept in flight while feeding the pack
/// writer. Lookups run concurrently against storage, but their results are
/// consumed in collected order, so the emitted pack is deterministic.
const OBJECT_LOOKAHEAD: usize = 32;

/// Capacity, in side-band packets, of the channel between the blocking pack
/// writer and the HTTP response stream. Each packet carries at most
/// [`MAX_BAND_DATA`] bytes, so this bounds the response memory held per
/// in-flight fetch.
const BODY_CHANNEL_PACKETS: usize = 8;

/// One object selected for a response pack. `delta_group` is a path-name hash
/// assigned only to blobs discovered through a tree walk; it lets the pack
/// writer search a bounded set of nearby, likely related bases.
#[derive(Debug, Clone, Copy)]
pub(super) struct ScheduledObject {
    oid: ObjectId,
    delta_group: Option<u32>,
}

/// A fully scheduled response pack plus the common haves used for protocol
/// acknowledgements.
#[derive(Debug)]
pub(super) struct PackPlan {
    pub objects: Vec<ScheduledObject>,
    pub common: Vec<ObjectId>,
}

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
        tracing::debug!("missing protocol v2 header");
        return version_error();
    }

    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(ParseError) => {
            tracing::debug!("malformed upload-pack request");
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
        Err(err) => return error_response(error::classify(&err)),
    };

    let command_label = match request.command.as_str() {
        "ls-refs" => "ls-refs",
        "fetch" => "fetch",
        "object-format" => "object-format",
        "object-info" => "object-info",
        _ => "unknown",
    };
    metrics::counter!("upload_pack_commands_total", "command" => command_label).increment(1);

    match request.command.as_str() {
        "ls-refs" => ls_refs(state, &meta, &request.args).await,
        "fetch" => fetch(state, &meta, &request.args).await,
        "object-format" => object_format(&meta),
        "object-info" => object_info(state, &meta, &request.args).await,
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
/// as a fatal error, carried in an otherwise-successful result response. Every
/// client-caused upload-pack rejection funnels through here, so one debug event
/// records the reason for the whole endpoint.
pub(super) fn err_line(message: &str) -> Response {
    tracing::debug!(reason = %message, "request rejected");
    match advertise::error_line(message) {
        Ok(body) => http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body),
        Err(_) => http::internal_error(),
    }
}

/// Reject a `fetch` request with an in-band `ERR`, counting it toward
/// `fetch_total{outcome="rejected"}`. Only argument-shape rejections decided
/// directly in [`fetch`] go through here; a rejection classified from a
/// domain error (via [`error::classify`]) is counted at its own call site so
/// a server-side fault (never "rejected") is not double-counted.
pub(super) fn reject_fetch(message: &str) -> Response {
    metrics::counter!("fetch_total", "outcome" => "rejected").increment(1);
    err_line(message)
}

/// Render a domain error's protocol [`Class`] (from [`error::classify`],
/// called once by the caller so a server fault is logged exactly once): a
/// client-caused failure becomes an in-band `ERR` pkt-line, an absent
/// repository a 404, and an internal fault a generic 500.
pub(super) fn error_response(class: Class) -> Response {
    match class {
        Class::Client(reason) => err_line(&reason),
        Class::NotFound => http::plain(StatusCode::NOT_FOUND, "repository not found"),
        Class::Server => http::internal_error(),
    }
}

/// Serve the `fetch` command: resolve the wants into the objects to pack
/// (`docs/0001-init.md` §command=fetch Phases I and II, plus arbitrary-oid
/// backfill wants), then stream the pack (Phase III). Resolution failures
/// surface before the response body starts, as an in-band `ERR` pkt-line
/// through the same classifier `ls-refs` uses; once streaming has begun, a
/// failure can only be reported on side-band channel 3.
async fn fetch(state: &AppState, meta: &RepoMeta, args: &[String]) -> Response {
    let start = Instant::now();
    let parsed = match FetchArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(message) => return reject_fetch(&message),
    };
    if parsed.wants.is_empty() {
        return reject_fetch("fetch requires at least one want");
    }

    let walker = Walker::new(state.store.clone(), state.objectdb.clone(), meta.id);
    let plan = match plan_pack(
        &walker,
        &parsed.wants,
        &parsed.haves,
        &parsed.filter,
        parsed.ofs_delta,
    )
    .await
    {
        Ok(planned) => planned,
        Err(err) => {
            let class = error::classify(&err);
            if matches!(class, Class::Client(_)) {
                metrics::counter!("fetch_total", "outcome" => "rejected").increment(1);
            }
            return error_response(class);
        }
    };
    let Ok(count) = u32::try_from(plan.objects.len()) else {
        return reject_fetch("too many objects for one pack");
    };

    // The read-path story for one fetch. Timed to the point the pack is
    // planned and streaming begins; the pack bytes flow out after this.
    tracing::debug!(
        wants = parsed.wants.len(),
        haves = parsed.haves.len(),
        common = plan.common.len(),
        objects_packed = count,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "fetch planned"
    );
    metrics::counter!("fetch_total", "outcome" => "ok").increment(1);
    metrics::histogram!("fetch_objects_packed").record(f64::from(count));

    let prefix = match negotiation_prefix(parsed.done, &plan.common) {
        Ok(prefix) => prefix,
        Err(_) => return http::internal_error(),
    };

    // Protocol v2 always multiplexes the pack over side-band-64k.
    stream_pack_response(
        state.objectdb.clone(),
        meta,
        plan.objects,
        count,
        prefix,
        true,
        parsed.no_progress,
        parsed.ofs_delta,
    )
}

/// Spawn the pack-streaming pipeline shared by the protocol-v2 and classic
/// (v0) fetch handlers, and return the response whose body is `prefix` (the
/// pkt-lines that precede the pack) followed by the pack itself. Object
/// contents are looked up concurrently (up to [`OBJECT_LOOKAHEAD`] in flight)
/// and handed in collected order to a blocking pack writer, which streams the
/// pack out as the response body — side-band-64k multiplexed when `sideband`
/// is set, raw pack bytes otherwise. Both the lookup task and the writer carry
/// the request span so any events they emit inherit the repo/endpoint context.
pub(super) fn stream_pack_response(
    objectdb: ObjectDb,
    meta: &RepoMeta,
    objects: Vec<ScheduledObject>,
    count: u32,
    prefix: Vec<u8>,
    sideband: bool,
    no_progress: bool,
    ofs_deltas: bool,
) -> Response {
    let repo = meta.id;
    let object_hash = meta.object_format;
    let span = Span::current();
    let (object_tx, object_rx) = mpsc::channel(OBJECT_LOOKAHEAD);
    tokio::spawn(feed_objects(objectdb, repo, objects, object_tx).instrument(span.clone()));

    let (body_tx, body_rx) = mpsc::channel::<Bytes>(BODY_CHANNEL_PACKETS);
    tokio::task::spawn_blocking(move || {
        let _guard = span.enter();
        stream_pack(
            object_rx,
            body_tx,
            count,
            object_hash,
            sideband,
            no_progress,
            ofs_deltas,
        );
    });

    let body = Body::from_stream(
        stream::iter([Ok::<_, Infallible>(Bytes::from(prefix))])
            .chain(stream::unfold(body_rx, |mut rx| async move {
                rx.recv().await.map(|chunk| (Ok(chunk), rx))
            })),
    );
    http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body)
}

/// Resolve a parsed fetch into the objects to pack and the common haves to
/// acknowledge. Commit and annotated-tag wants drive commit discovery and
/// object collection; blob and tree wants are arbitrary-oid backfill wants
/// (git's `allowAnySHA1InWant`) packed directly by point lookup.
///
/// When every want is a blob or tree — a partial clone faulting in objects it
/// pruned earlier — commit discovery and collection are skipped entirely and
/// the pack is exactly the wanted objects (`docs/0001-init.md` §Filters). A
/// mixed request runs the walk over the commit/tag wants and then adds the
/// directly wanted objects; those are packed even when a filter would drop
/// them from traversal, because an explicitly requested object is never
/// filtered out. Directly wanted objects the walk already collected are added
/// once.
pub(super) async fn plan_pack(
    walker: &Walker,
    wants: &[ObjectId],
    haves: &[ObjectId],
    filter: &FilterSpec,
    ofs_deltas: bool,
) -> Result<PackPlan, WalkError> {
    let partition = walker.partition_wants(wants).await?;
    if partition.history.is_empty() {
        return Ok(PackPlan {
            objects: plain_schedule(partition.direct),
            common: Vec::new(),
        });
    }

    let selection = walker.select_commits(&partition.history, haves).await?;
    let mut collected = if ofs_deltas {
        walker
            .collect_with_paths(&selection.to_send, &selection.common, filter)
            .await?
    } else {
        CollectedObjects {
            objects: walker
                .collect(&selection.to_send, &selection.common, filter)
                .await?,
            blob_paths: HashMap::new(),
        }
    };
    let mut seen: HashSet<ObjectId> = collected.objects.iter().copied().collect();
    for oid in partition.direct {
        if seen.insert(oid) {
            collected.objects.push(oid);
        }
    }
    let objects = if ofs_deltas {
        schedule_for_ofs(walker, collected).await?
    } else {
        plain_schedule(collected.objects)
    };
    Ok(PackPlan {
        objects,
        common: selection.common,
    })
}

/// Preserve Phase II's deterministic object order when the client did not
/// negotiate offset deltas. This keeps the usual fetch path entirely free of
/// delta planning and blob inflation.
fn plain_schedule(objects: Vec<ObjectId>) -> Vec<ScheduledObject> {
    objects
        .into_iter()
        .map(|oid| ScheduledObject {
            oid,
            delta_group: None,
        })
        .collect()
}

/// Git-inspired pack scheduling for `OFS_DELTA`: non-blobs retain their walk
/// order, while path-discovered blobs are grouped by a suffix-biased path hash
/// and then by exact path and size. This puts likely related revisions close
/// together before the bounded delta window runs, without retaining blob
/// contents in the plan.
async fn schedule_for_ofs(
    walker: &Walker,
    collected: CollectedObjects,
) -> Result<Vec<ScheduledObject>, WalkError> {
    let info: HashMap<ObjectId, (Kind, u64)> = walker
        .object_info(&collected.objects)
        .await?
        .into_iter()
        .map(|(oid, kind, size)| (oid, (kind, size)))
        .collect();
    let mut fixed = Vec::new();
    let mut blobs = Vec::new();
    for (position, oid) in collected.objects.into_iter().enumerate() {
        let Some(path) = collected.blob_paths.get(&oid) else {
            fixed.push((
                position,
                ScheduledObject {
                    oid,
                    delta_group: None,
                },
            ));
            continue;
        };
        let (kind, size) = info[&oid];
        if kind != Kind::Blob {
            fixed.push((
                position,
                ScheduledObject {
                    oid,
                    delta_group: None,
                },
            ));
            continue;
        }
        blobs.push((name_hash(path), path.clone(), size, position, oid));
    }
    blobs.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
    });
    fixed.sort_by_key(|(position, _)| *position);
    let mut scheduled = fixed
        .into_iter()
        .map(|(_, object)| object)
        .collect::<Vec<_>>();
    scheduled.extend(
        blobs
            .into_iter()
            .map(|(group, _, _, _, oid)| ScheduledObject {
                oid,
                delta_group: Some(group),
            }),
    );
    Ok(scheduled)
}

/// A stable, suffix-biased path hash. Exact paths are still a secondary sort
/// key, while suffix affinity gives renamed files in similar directories a
/// chance to share the bounded candidate window.
fn name_hash(path: &[u8]) -> u32 {
    path.iter().rev().fold(5381u32, |hash, byte| {
        hash.wrapping_mul(33).wrapping_add(u32::from(*byte))
    })
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

/// Read each collected object's stored zlib stream from the object database
/// with up to [`OBJECT_LOOKAHEAD`] lookups in flight, forwarding ready-to-pack
/// entries to the pack writer in collected order. Nothing is inflated: the
/// stored streams are what the pack copies out. Stops at the first failure
/// (forwarded so the writer can report it) or when the writer hangs up.
async fn feed_objects(
    objectdb: ObjectDb,
    repo: RepoId,
    objects: Vec<ScheduledObject>,
    tx: mpsc::Sender<Result<PackEntry, FetchObjectError>>,
) {
    let mut lookups = stream::iter(objects)
        .map(|object| {
            let objectdb = objectdb.clone();
            async move { (object, objectdb.read_compressed(repo, &object.oid).await) }
        })
        .buffered(OBJECT_LOOKAHEAD);
    while let Some((object, result)) = lookups.next().await {
        let item = match result {
            Ok(Some((kind, decompressed_size, zlib))) => Ok(PackEntry {
                id: object.oid,
                kind,
                decompressed_size,
                zlib,
                delta_group: object.delta_group,
            }),
            Ok(None) => Err(FetchObjectError::Missing(object.oid)),
            Err(err) => Err(FetchObjectError::Objects(err)),
        };
        let stop = item.is_err();
        if tx.send(item).await.is_err() || stop {
            return;
        }
    }
}

/// The blocking half of the fetch response: pull ready-to-pack entries from
/// `objects`, write them into the pack (copying each stored zlib stream
/// verbatim — nothing is compressed here), and send the pack out on `body`.
/// With `sideband` set the pack is multiplexed as side-band channel-1
/// packets, with progress on channel 2 (unless suppressed) and a flush pkt
/// closing a successful response, and a failure after streaming has begun is
/// reported on channel 3 — the only remaining path to the client — except
/// when the client itself hung up. Without side-band the pack bytes are sent
/// raw with no framing, no progress, and no trailing flush; a mid-stream
/// failure then has no in-band channel and leaves the client a truncated pack
/// to reject.
fn stream_pack(
    mut objects: mpsc::Receiver<Result<PackEntry, FetchObjectError>>,
    body: mpsc::Sender<Bytes>,
    count: u32,
    object_hash: gix_hash::Kind,
    sideband: bool,
    no_progress: bool,
    ofs_deltas: bool,
) {
    let input = std::iter::from_fn(|| objects.blocking_recv());
    if !sideband {
        let mut writer = RawWriter {
            tx: body.clone(),
            buf: Vec::new(),
            bytes_written: 0,
        };
        let outcome = write_pack_with_options(
            input,
            count,
            object_hash,
            &mut writer,
            PackOptions { ofs_deltas },
        );
        metrics::histogram!("fetch_pack_bytes").record(writer.bytes_written as f64);
        if let Err(err) = outcome
            && !is_broken_pipe(&err)
        {
            // There is no side-band error channel here; the failure is logged
            // and the client is left with a short pack that fails its own
            // index-pack.
            let _ = error::classify(&err);
        }
        return;
    }

    if !no_progress {
        let line = format!("packing {count} objects\n");
        let _ = send_band(&body, Channel::Progress, line.as_bytes());
    }

    let mut writer = BandWriter {
        tx: body.clone(),
        buf: Vec::new(),
        bytes_written: 0,
    };
    let outcome = write_pack_with_options(
        input,
        count,
        object_hash,
        &mut writer,
        PackOptions { ofs_deltas },
    );
    // Counted here because this is where the true byte count exists: bytes
    // actually handed to the response channel, whether the pack completed or
    // ended mid-stream (a client abort still moved this many bytes).
    metrics::histogram!("fetch_pack_bytes").record(writer.bytes_written as f64);
    match outcome {
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
            if is_broken_pipe(&err) {
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

/// Whether a pack-write failure is the client hanging up mid-stream (a broken
/// pipe on the response channel), which is not a server fault and has no one
/// left to report to.
fn is_broken_pipe(err: &PackOutError<FetchObjectError>) -> bool {
    matches!(
        err,
        PackOutError::Write(gix_hash::io::Error::Io(io_err)) if io_err.kind() == io::ErrorKind::BrokenPipe
    )
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
    /// Total bytes handed to [`io::Write::write`] so far — the pack's true
    /// output size, independent of side-band/pkt-line framing overhead.
    bytes_written: u64,
}

impl io::Write for BandWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.bytes_written += data.len() as u64;
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

/// An `io::Write` that forwards written bytes verbatim to the HTTP response
/// channel, batching them into [`MAX_BAND_DATA`]-sized chunks so the number of
/// channel messages stays bounded. Unlike [`BandWriter`] it adds no side-band
/// framing: it is the response body for a classic fetch the client did not
/// negotiate side-band-64k on, where the raw pack follows the ACK/NAK pkt
/// directly.
struct RawWriter {
    tx: mpsc::Sender<Bytes>,
    buf: Vec<u8>,
    /// Total bytes handed to [`io::Write::write`] so far — the pack's true
    /// output size.
    bytes_written: u64,
}

impl io::Write for RawWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.bytes_written += data.len() as u64;
        self.buf.extend_from_slice(data);
        while self.buf.len() >= MAX_BAND_DATA {
            let rest = self.buf.split_off(MAX_BAND_DATA);
            let full = std::mem::replace(&mut self.buf, rest);
            self.tx
                .blocking_send(Bytes::from(full))
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let data = std::mem::take(&mut self.buf);
            self.tx
                .blocking_send(Bytes::from(data))
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
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
    /// The client accepts in-pack offset deltas.
    ofs_delta: bool,
    /// The partial-clone filter restricting which blobs are sent.
    filter: FilterSpec,
}

impl FetchArgs {
    /// Parse `fetch` argument lines. `ofs-delta` enables self-contained blob
    /// deltas; `thin-pack` remains accepted but has no effect until ref-delta
    /// support is added. Tag objects are sent only for explicitly wanted tag
    /// refs. A `filter` argument naming a spec [`FilterSpec::parse`] does not
    /// recognize, or any other unrecognized argument, is rejected with the
    /// returned `ERR` message.
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
                    "ofs-delta" => parsed.ofs_delta = true,
                    "thin-pack" | "include-tag" => {}
                    other => return Err(format!("unexpected fetch argument: {other}")),
                }
            }
        }
        Ok(parsed)
    }
}

/// Serve the `object-format` command: a single pkt-line carrying the repo's
/// `object-format` metadata value, terminated by a flush. Real git never
/// sends this command (`docs/0001-init.md` §command=object-format describes
/// it as a nonstandard addition); a client that does gets the same value the
/// `object-format=<x>` capability already advertises.
fn object_format(meta: &RepoMeta) -> Response {
    let mut body = Vec::new();
    let line = format!("{}\n", meta.object_format);
    if encode::data_to_write(line.as_bytes(), &mut body).is_err()
        || encode::flush_to_write(&mut body).is_err()
    {
        return http::internal_error();
    }
    http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body)
}

/// Serve the `object-info` command: for each requested `oid`, its content
/// size read via [`ObjectDb::size`] — never the blob store, so an offloaded
/// blob's size comes from its pointer record, not an object-storage round
/// trip. The response is a `size` header line followed by one `<oid> <size>`
/// line per requested oid, in request order, then a flush
/// (`docs/0001-init.md` §command=object-info). An oid absent from the
/// repository is an in-band `ERR`, per protocol-v2's server-error convention
/// for a request the server cannot satisfy.
async fn object_info(state: &AppState, meta: &RepoMeta, args: &[String]) -> Response {
    let parsed = match ObjectInfoArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(message) => return err_line(&message),
    };

    let mut body = Vec::new();
    if encode::data_to_write(b"size\n", &mut body).is_err() {
        return http::internal_error();
    }
    for oid in &parsed.oids {
        let size = match state.objectdb.size(meta.id, oid).await {
            Ok(Some((_, size))) => size,
            Ok(None) => return err_line(&format!("unknown object {oid}")),
            Err(err) => return error_response(error::classify(&err)),
        };
        let line = format!("{} {size}\n", oid.to_hex());
        if encode::data_to_write(line.as_bytes(), &mut body).is_err() {
            return http::internal_error();
        }
    }
    if encode::flush_to_write(&mut body).is_err() {
        return http::internal_error();
    }
    http::git_response(StatusCode::OK, RESULT_CONTENT_TYPE, body)
}

/// Parsed `object-info` arguments.
#[derive(Debug, Default)]
struct ObjectInfoArgs {
    /// The client wants size information in the response. Accepted but not
    /// load-bearing: size is the only attribute this server can report, so
    /// it is always included regardless of this flag.
    size: bool,
    /// The objects to report on, in request order.
    oids: Vec<ObjectId>,
}

impl ObjectInfoArgs {
    /// Parse `object-info` argument lines. An unrecognized argument or an oid
    /// that fails to parse as hex is returned as the `ERR` reply message.
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut parsed = ObjectInfoArgs::default();
        for arg in args {
            if let Some(hex) = arg.strip_prefix("oid ") {
                let oid = ObjectId::from_hex(hex.as_bytes())
                    .map_err(|_| format!("invalid oid: {hex}"))?;
                parsed.oids.push(oid);
            } else {
                match arg.as_str() {
                    "size" => parsed.size = true,
                    other => return Err(format!("unexpected object-info argument: {other}")),
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
        Err(err) => error_response(error::classify(&err)),
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
    let mut advertised = 0usize;
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
                    advertised += 1;
                } else {
                    // A dangling HEAD is the normal state of every
                    // pre-first-push repository, and this event is
                    // client-reachable at will (auto-create an empty repo,
                    // then `ls-refs` without `unborn`): not warn-worthy.
                    tracing::debug!(
                        reference = %name,
                        reason = "dangling",
                        "symref routed around during advertisement"
                    );
                }
                continue;
            }
            // A chain exceeding the depth cap has no reliable target to
            // report and is omitted outright.
            Resolution::TooDeep => {
                tracing::warn!(
                    reference = %name,
                    reason = "too-deep",
                    "symref routed around during advertisement"
                );
                continue;
            }
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
        advertised += 1;
    }
    encode::flush_to_write(&mut body)?;
    tracing::debug!(refs = advertised, "ls-refs served");
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
pub(super) enum Resolution {
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
pub(super) async fn resolve_ref(
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
pub(super) async fn peel_ref(
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

/// A failure while resolving refs for advertisement: building the protocol-v2
/// `ls-refs` response or the classic (v0) ref advertisement. Both follow
/// symref chains and peel tags through the same store and object-database
/// reads, so they share this error's variants and classification.
#[derive(Debug, thiserror::Error)]
pub(super) enum LsRefsError {
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

        // then: wants/haves keep order; ofs-delta is honored while the
        // unsupported thin-pack/include-tag variants remain harmless.
        assert_eq!(parsed.wants, vec![oid(b'a'), oid(b'b')]);
        assert_eq!(parsed.haves, vec![oid(b'c')]);
        assert!(parsed.done);
        assert!(parsed.no_progress);
        assert!(parsed.ofs_delta);
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

    #[test]
    fn should_parse_object_info_size_flag_and_oids_in_order() {
        // given: the size flag and two oid requests
        let args = vec![
            "size".to_owned(),
            format!("oid {}", oid(b'a')),
            format!("oid {}", oid(b'b')),
        ];

        // when
        let parsed = ObjectInfoArgs::parse(&args).expect("parse args");

        // then
        assert!(parsed.size);
        assert_eq!(parsed.oids, vec![oid(b'a'), oid(b'b')]);
    }

    #[test]
    fn should_reject_an_unknown_object_info_argument() {
        // given/when
        let err = ObjectInfoArgs::parse(&["deepen 1".to_owned()]).unwrap_err();

        // then
        assert_eq!(err, "unexpected object-info argument: deepen 1");
    }

    #[test]
    fn should_reject_a_malformed_object_info_oid() {
        // given/when
        let err = ObjectInfoArgs::parse(&["oid not-hex".to_owned()]).unwrap_err();

        // then
        assert_eq!(err, "invalid oid: not-hex");
    }
}
