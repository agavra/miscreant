//! Fetch object discovery: decide which commits to send (Phase I) and which
//! trees/blobs/tags those commits require (Phase II).
//!
//! See `docs/0001-init.md` §command=fetch. The walk preserves that section's
//! invariants: a HAVE always dominates a WANT, a have's entire ancestry is
//! HAVE, traversal visits the highest generation first, and discovery stops as
//! soon as every want has been resolved (emitted or downgraded to HAVE).
//! Commit metadata (generation, root tree, parents) comes from the
//! commit-graph segment, which is rebuilt lazily from the objects themselves
//! whenever records are missing ([`Walker::get_commit_info`]), or wholesale
//! for a whole repository ([`Walker::rebuild_commit_graph`]).
//!
//! Both phases traverse level by level rather than one object at a time. Every
//! store lookup is a point read whose latency is dominated by round-trip time,
//! so the walk trades serial depth for fan-out width: it gathers a whole level
//! of ids (a commit-graph generation, or a tree depth), issues their lookups as
//! one bounded-width concurrent batch ([`WALK_LOOKAHEAD`]), then applies the
//! results synchronously in a deterministic order. Serial depth collapses from
//! the object count to the dependency depth, and the results are consumed in
//! input order so the output stays fully deterministic.

use std::collections::{BinaryHeap, HashMap, HashSet};

use futures::stream::{self, StreamExt};
use gix_hash::{ObjectId, oid};
use gix_object::{CommitRef, Kind, TagRef, TreeRefIter};

use crate::storage::keys::RepoId;
use crate::storage::values::{CommitGraphRecord, RefTarget};
use crate::storage::{Durability, ObjectDb, ObjectDbError, Store, StoreError};

/// Upper bound on symref indirection when resolving a ref to a direct object
/// id during [`Walker::rebuild_commit_graph`]. A chain that dangles or does
/// not terminate within this many hops contributes no root commit.
const MAX_REBUILD_SYMREF_DEPTH: usize = 5;

/// Upper bound on annotated-tag indirection when peeling a resolved ref
/// object down to a commit during [`Walker::rebuild_commit_graph`]. A chain
/// that does not terminate within this many hops contributes no root commit.
const MAX_REBUILD_TAG_PEEL_DEPTH: usize = 5;

/// Number of store lookups the walk keeps in flight per level. Each level of
/// commit-graph records, tree reads, or blob-size checks is fetched as one
/// concurrent batch bounded to this width, and the results are consumed in
/// input order so the walk stays deterministic. A little wider than the pack
/// feeder's `OBJECT_LOOKAHEAD` (see `src/protocol/upload_pack.rs`): these are
/// small point reads (records and sizes), not whole object bodies.
const WALK_LOOKAHEAD: usize = 64;

/// Errors surfaced by the fetch walk.
#[derive(Debug, thiserror::Error)]
pub enum WalkError {
    /// A SlateDB-backed store operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// An object read through the object database failed.
    #[error(transparent)]
    Objects(#[from] ObjectDbError),
    /// A requested want names an object the repository does not have.
    #[error("want {0} is not a known object")]
    UnknownWant(ObjectId),
    /// A requested want resolved to an object kind the walk cannot serve.
    #[error("want {oid} is a {kind}, not a commit or annotated tag")]
    UnsupportedWant {
        /// The offending object id (the want itself or a peeled target).
        oid: ObjectId,
        /// The kind that object turned out to be.
        kind: Kind,
    },
    /// An object referenced by another object is absent from storage. This
    /// indicates corruption: promotion only commits full closures.
    #[error("object {0} is referenced but missing from storage")]
    MissingObject(ObjectId),
    /// An oid was expected to name a different kind of object.
    #[error("object {oid} is a {actual}, expected a {expected}")]
    UnexpectedKind {
        /// The object whose kind was surprising.
        oid: ObjectId,
        /// The kind the walk required at this position.
        expected: Kind,
        /// The kind actually recorded in storage.
        actual: Kind,
    },
    /// A stored object body failed to parse (indicates corruption).
    #[error("object {oid} could not be decoded")]
    Decode {
        /// The object whose body failed to parse.
        oid: ObjectId,
        /// The underlying parse failure.
        #[source]
        source: gix_object::decode::Error,
    },
    /// Commit discovery's frontier emptied while wants remained unresolved.
    /// Every commit marked WANT is pushed onto the frontier and can only
    /// leave the pending set by being popped from it, so this is believed
    /// unreachable even with corrupt commit-graph generations. Surfaced as a
    /// hard error rather than silently returning an incomplete pack.
    #[error("commit discovery stalled with unresolved wants: {unresolved:?}")]
    DiscoveryStalled {
        /// The wants still pending when the frontier ran dry.
        unresolved: Vec<ObjectId>,
    },
}

/// A partial-clone filter restricting which blobs [`Walker::collect`] adds to
/// the send set. Commits, trees, and tag objects are never filtered — see
/// `docs/0001-init.md` §Filters (partial clone).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FilterSpec {
    /// No filter: every reachable object is collected.
    #[default]
    None,
    /// Omit every blob.
    BlobNone,
    /// Include a blob only when its content size is at most the given limit,
    /// read from the object record (inline header or pointer payload) —
    /// never by fetching the blob's content.
    BlobLimit(u64),
}

impl FilterSpec {
    /// Parse the payload of a `filter <spec>` fetch argument (the text after
    /// `filter `). Returns `None` for any filter kind other than `blob:none`
    /// and `blob:limit=<n>` (tree-depth, sparse, and combined filters are not
    /// implemented); the caller reports that back to the client as an
    /// in-band protocol error naming the rejected spec.
    pub fn parse(spec: &str) -> Option<Self> {
        if spec == "blob:none" {
            return Some(FilterSpec::BlobNone);
        }
        let limit = spec.strip_prefix("blob:limit=")?;
        parse_unit_size(limit).map(FilterSpec::BlobLimit)
    }
}

/// Parse a size with an optional single-character unit suffix (`k`/`m`/`g`,
/// case-insensitive, base 1024) — the syntax `git`'s `blob:limit` filter
/// accepts from the user and transmits verbatim in the `filter` fetch
/// argument.
fn parse_unit_size(s: &str) -> Option<u64> {
    let (digits, multiplier) = match s.as_bytes().last()? {
        b'k' | b'K' => (&s[..s.len() - 1], 1024u64),
        b'm' | b'M' => (&s[..s.len() - 1], 1024 * 1024),
        b'g' | b'G' => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    digits.parse::<u64>().ok()?.checked_mul(multiplier)
}

/// The outcome of commit discovery (Phase I).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Objects to send: commits in descending generation order, followed by
    /// the wanted annotated tag objects (in want order). Feed this to
    /// [`Walker::collect`].
    pub to_send: Vec<ObjectId>,
    /// The haves the server recognized, for later acknowledgement. Haves the
    /// server does not know are silently dropped.
    pub common: Vec<ObjectId>,
    /// Whether discovery resolved every want while commits still remained
    /// unexplored on the frontier — the recognized haves bounded the traversal
    /// short of the roots. `false` when the frontier drained (the walk reached
    /// the roots), as it always does for a clone with no usable haves. Combined
    /// with a non-empty `common`, this is the negotiation signal that the
    /// server has enough shared history to build the pack now (git's
    /// `ok_to_give_up`); it is derived from the walk and never steers it.
    pub bounded_by_haves: bool,
}

/// Raw fetch wants split by how they are served, produced by
/// [`Walker::partition_wants`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WantPartition {
    /// Wants that drive commit discovery: commits, and annotated tags (which
    /// [`Walker::select_commits`] peels to a target commit). Kept in want
    /// order, duplicates included — discovery is idempotent over them.
    pub history: Vec<ObjectId>,
    /// Wants packed directly by point lookup: blobs and trees, requested by
    /// arbitrary oid (git's `allowAnySHA1InWant`). Deduplicated in first-seen
    /// order.
    pub direct: Vec<ObjectId>,
}

/// Marker for a commit's role during discovery: wanted by the client, or
/// already had by it. HAVE dominates WANT — once a commit is HAVE it never
/// becomes WANT again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flag {
    Want,
    Have,
}

/// A frontier entry: ordered by generation (then oid, purely for
/// determinism), so the max-heap pops the highest generation first. Parents
/// have strictly smaller generations than their descendants, so a HAVE
/// normally propagates to a shared ancestor before that ancestor is popped —
/// this ordering is what makes discovery prune instead of over-serving.
/// Termination safety does not depend on it: see `Discovery::pending`.
#[derive(Debug, PartialEq, Eq)]
struct QueueEntry {
    generation: u32,
    oid: ObjectId,
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.generation
            .cmp(&other.generation)
            .then_with(|| self.oid.cmp(&other.oid))
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Mutable state of the Phase I walk: the generation-keyed max-heap, the
/// WANT/HAVE marks, and the set of commits still WANTed and not yet
/// resolved.
#[derive(Default)]
struct Discovery {
    frontier: BinaryHeap<QueueEntry>,
    state: HashMap<ObjectId, Flag>,
    /// Commits marked WANT that are not yet resolved (emitted into
    /// `to_send` or downgraded to HAVE); discovery terminates when this
    /// empties. A set rather than a count because `insert`/`remove` are
    /// idempotent: corrupt commit-graph generations can reorder the heap so
    /// a commit is emitted before its downgrade arrives, and the redundant
    /// removal must be a no-op — the worst case is an extra pack entry
    /// (over-serving), never a silently incomplete pack.
    pending: HashSet<ObjectId>,
}

impl Discovery {
    /// Mark `id` with `flag`, updating `pending` and pushing the commit onto
    /// the frontier at `generation` when its state changed: a new WANT joins
    /// `pending`, a WANT downgraded by a HAVE leaves it, and an established
    /// HAVE absorbs everything. Both `pending` operations are idempotent, so
    /// re-marking an id that already left (or never entered) the relevant
    /// state is always safe. This is pure state mutation — the caller supplies
    /// `generation` from a batch of record lookups, keeping I/O out of the
    /// per-parent inner loop.
    fn mark(&mut self, id: ObjectId, flag: Flag, generation: u32) {
        let resolved = match (self.state.get(&id), flag) {
            (Some(Flag::Have), _) | (Some(Flag::Want), Flag::Want) => return,
            (Some(Flag::Want), Flag::Have) => {
                self.pending.remove(&id);
                Flag::Have
            }
            (None, Flag::Have) => Flag::Have,
            (None, Flag::Want) => {
                self.pending.insert(id);
                Flag::Want
            }
        };
        self.state.insert(id, resolved);
        self.frontier.push(QueueEntry {
            generation,
            oid: id,
        });
    }
}

/// A tree entry relevant to the walk. Gitlink (submodule) entries reference
/// objects that live in another repository and are skipped entirely.
enum TreeEntry {
    /// A subtree to descend into.
    Subtree(ObjectId),
    /// A blob or symlink: a leaf that is sent but never descended into.
    Leaf(ObjectId),
}

/// An insertion-ordered set of object ids: the backing set memoizes
/// membership while the vector preserves first-insertion order.
#[derive(Default)]
struct OrderedOidSet {
    order: Vec<ObjectId>,
    seen: HashSet<ObjectId>,
}

impl OrderedOidSet {
    fn insert(&mut self, id: ObjectId) {
        if self.seen.insert(id) {
            self.order.push(id);
        }
    }

    fn contains(&self, id: &ObjectId) -> bool {
        self.seen.contains(id)
    }

    fn into_vec(self) -> Vec<ObjectId> {
        self.order
    }
}

/// Fetch-side object discovery over one repository's commit graph and
/// objects. Cheap to construct per request (the handles are shared).
pub struct Walker {
    store: Store,
    objects: ObjectDb,
    repo: RepoId,
}

impl Walker {
    /// Build a walker over `repo`, using `store` for commit-graph records
    /// and `objects` for object content.
    pub fn new(store: Store, objects: ObjectDb, repo: RepoId) -> Self {
        Self {
            store,
            objects,
            repo,
        }
    }

    /// The commit-graph record for `id`, backfilling missing records from
    /// the commit objects themselves (the graph is derived data and may be
    /// absent or partial; see `docs/0001-init.md` §command=fetch on
    /// rebuilding the index when unavailable). Backfill walks the ancestry
    /// with an explicit stack until it reaches commits that already have
    /// records, then writes records children-after-parents so generations
    /// are always `1 + max(parent generations)` (`1` for roots).
    pub async fn get_commit_info(&self, id: &oid) -> Result<CommitGraphRecord, WalkError> {
        if let Some(record) = self.store.get_commit_graph(self.repo, id).await? {
            return Ok(record);
        }
        metrics::counter!("commit_graph_backfills_total").increment(1);
        self.backfill_commit_info(id).await
    }

    /// Split raw wants by how the fetch must serve each one. Commit and
    /// annotated-tag wants drive commit discovery and object collection
    /// (Phases I and II); blob and tree wants are arbitrary-oid backfill
    /// wants that a partial clone faults in directly, so they bypass the
    /// commit walk and are packed by point lookup (`docs/0001-init.md`
    /// §Filters). A want naming an object the repository does not have is a
    /// [`WalkError::UnknownWant`], surfaced by the caller as an in-band
    /// protocol error.
    ///
    /// Kinds are read via [`ObjectDb::size`], so no blob content is fetched
    /// just to classify a want. Every want's kind is looked up as one
    /// concurrent batch, then classified in want order so `history` keeps want
    /// order and `direct` keeps first-seen order.
    pub async fn partition_wants(&self, wants: &[ObjectId]) -> Result<WantPartition, WalkError> {
        let mut history = Vec::new();
        let mut direct = OrderedOidSet::default();
        for (want, size) in self.object_sizes(wants).await {
            let (kind, _) = size?.ok_or(WalkError::UnknownWant(want))?;
            match kind {
                Kind::Commit | Kind::Tag => history.push(want),
                Kind::Tree | Kind::Blob => direct.insert(want),
            }
        }
        Ok(WantPartition {
            history,
            direct: direct.into_vec(),
        })
    }

    /// Bulk-recompute the commit-graph record for every commit reachable
    /// from this repository's refs, overwriting whatever was already
    /// stored. This differs from [`Walker::get_commit_info`]'s lazy
    /// backfill in the one way that matters for a rebuild: backfill stops
    /// as soon as it reaches a commit that already has a record, so it can
    /// only fill gaps, whereas a rebuild never trusts an existing record —
    /// every generation is recomputed from the commit objects themselves,
    /// so a rebuild also corrects a record that had gone stale or been
    /// corrupted.
    ///
    /// Refs are resolved to a root commit by following symref chains and
    /// peeling annotated tag chains through the object database; a ref that
    /// does not ultimately name a commit (dangling, too deep, or pointing
    /// at a tree or blob) contributes no root and is silently skipped. The
    /// full closure of every root is then walked once with an iterative
    /// post-order pass shared across all roots, so a commit reachable from
    /// more than one ref is only computed once. Every write uses
    /// [`Durability::Relaxed`]; a single [`Store::flush`] at the end makes
    /// a completed rebuild durable, mirroring the barrier promotion uses.
    ///
    /// Returns the number of commit-graph records written.
    pub async fn rebuild_commit_graph(&self) -> Result<usize, WalkError> {
        let refs = self.store.list_refs(self.repo, None).await?;
        let mut roots = Vec::new();
        for (name, target) in refs {
            match self.resolve_commit_tip(target).await? {
                Some(commit_id) => roots.push(commit_id),
                None => tracing::warn!(
                    reference = %name,
                    "ref does not resolve to a commit; skipped during graph rebuild"
                ),
            }
        }

        // Parsed commit bodies and resolved generations for this rebuild
        // only — neither is seeded from (or ever consults) existing
        // commit-graph records, since recomputing from the commit objects
        // is the whole point of a rebuild.
        let mut parsed: HashMap<ObjectId, (ObjectId, Vec<ObjectId>)> = HashMap::new();
        let mut generations: HashMap<ObjectId, u32> = HashMap::new();

        for root in roots {
            if generations.contains_key(&root) {
                continue;
            }
            let mut stack = vec![root];
            while let Some(&top) = stack.last() {
                if generations.contains_key(&top) {
                    stack.pop();
                    continue;
                }
                let (root_tree, parents) = match parsed.get(&top) {
                    Some(entry) => entry.clone(),
                    None => {
                        let entry = self.parse_commit(&top).await?;
                        parsed.insert(top, entry.clone());
                        entry
                    }
                };
                let missing: Vec<ObjectId> = parents
                    .iter()
                    .filter(|parent| !generations.contains_key(*parent))
                    .copied()
                    .collect();
                if missing.is_empty() {
                    let generation = 1 + parents
                        .iter()
                        .filter_map(|parent| generations.get(parent))
                        .max()
                        .copied()
                        .unwrap_or(0);
                    let record = CommitGraphRecord {
                        generation,
                        root_tree,
                        parents,
                    };
                    self.store
                        .put_commit_graph(self.repo, &top, &record, Durability::Relaxed)
                        .await?;
                    generations.insert(top, generation);
                    stack.pop();
                } else {
                    stack.extend(missing);
                }
            }
        }

        self.store.flush().await?;
        Ok(generations.len())
    }

    /// Resolve one ref's target to a root commit for
    /// [`Walker::rebuild_commit_graph`]: follow symref chains to a direct
    /// object id, then peel annotated tag chains through the object
    /// database until a non-tag object is reached. Returns `None` for
    /// anything that does not bottom out at a commit — a dangling or
    /// too-deep symref chain, a target object gone missing, or a chain that
    /// peels to a tree or blob — so a rebuild routes around a broken ref
    /// instead of failing the whole repository's graph.
    async fn resolve_commit_tip(&self, target: RefTarget) -> Result<Option<ObjectId>, WalkError> {
        let mut current = target;
        let mut resolved = None;
        for _ in 0..MAX_REBUILD_SYMREF_DEPTH {
            match current {
                RefTarget::Direct(id) => {
                    resolved = Some(id);
                    break;
                }
                RefTarget::Reference(name) => match self.store.get_ref(self.repo, &name).await? {
                    Some(next) => current = next,
                    None => return Ok(None), // dangling symref
                },
            }
        }
        let Some(mut cursor) = resolved else {
            return Ok(None); // symref chain exceeded the depth cap
        };

        for _ in 0..MAX_REBUILD_TAG_PEEL_DEPTH {
            let Some((kind, body)) = self.objects.get(self.repo, &cursor).await? else {
                return Ok(None); // target object does not exist
            };
            match kind {
                Kind::Commit => return Ok(Some(cursor)),
                Kind::Tag => {
                    let tag = TagRef::from_bytes(&body, cursor.kind()).map_err(|source| {
                        WalkError::Decode {
                            oid: cursor,
                            source,
                        }
                    })?;
                    cursor = tag.target();
                }
                _ => return Ok(None), // tree or blob: not a commit
            }
        }
        Ok(None) // tag chain exceeded the depth cap
    }

    /// Phase I: decide which commits to send. `wants` are ref tips requested
    /// by the client (commits, or annotated tags which are peeled to their
    /// target commit while the tag objects themselves are queued for
    /// sending). `haves` are commits the client claims to possess: unknown
    /// haves are ignored (standard git behavior), known ones are reported in
    /// [`Selection::common`], and every known have dominates its entire
    /// ancestry. An unknown want is an error.
    ///
    /// The frontier is drained one commit-graph generation at a time. Each
    /// level is every frontier entry sharing the current maximum generation;
    /// the records for its commits were fetched when they were marked, so only
    /// their parents' records need loading, and those are loaded as one
    /// concurrent batch. The WANT/HAVE marking then runs synchronously in a
    /// deterministic order (descending generation, then descending oid within
    /// a level). This is equivalent to visiting commits one at a time because a
    /// parent's generation is strictly smaller than its child's, so nothing
    /// applied within a level can change another same-level commit's flag, and
    /// serial depth drops from the commit count to the generation depth. A
    /// per-walk record map keeps each commit-graph record fetched from the
    /// store at most once.
    pub async fn select_commits(
        &self,
        wants: &[ObjectId],
        haves: &[ObjectId],
    ) -> Result<Selection, WalkError> {
        let mut discovery = Discovery::default();
        let mut records: HashMap<ObjectId, CommitGraphRecord> = HashMap::new();
        let mut common = Vec::new();

        // Prefetch every have's record at once, then mark the recognized ones
        // in first-seen order. An unknown (or non-commit) have is dropped, but
        // only the have itself is excused — a missing object deeper in the
        // ancestry is corruption and still errors.
        for (have, record) in self.prefetch_records(&mut records, haves).await {
            match record {
                Ok(info) => {
                    common.push(have);
                    discovery.mark(have, Flag::Have, info.generation);
                }
                Err(WalkError::MissingObject(missing)) if missing == have => {}
                Err(WalkError::UnexpectedKind { oid, .. }) if oid == have => {}
                Err(err) => return Err(err),
            }
        }

        // Peel the wants to commits (queuing the tag objects), then prefetch
        // and mark the commit wants. A want that is also a have is already
        // HAVE, so its (skipped) WANT mark would be a no-op regardless.
        let (commit_wants, tags) = self.peel_wants(wants).await?;
        for (want, record) in self.prefetch_records(&mut records, &commit_wants).await {
            discovery.mark(want, Flag::Want, record?.generation);
        }

        // Visit the highest generation first; stop once no wants are pending.
        // A commit can sit in the frontier more than once (re-pushed when a
        // WANT was downgraded to HAVE, or under corrupt generations that
        // reorder the heap), so `done` guards against reprocessing.
        let mut to_send = Vec::new();
        let mut done: HashSet<ObjectId> = HashSet::new();
        while !discovery.pending.is_empty() {
            let Some(&QueueEntry {
                generation: level_generation,
                ..
            }) = discovery.frontier.peek()
            else {
                // Every id in `pending` was pushed onto the frontier when it
                // was marked WANT (see `Discovery::mark`) and can only leave
                // `pending` by being visited here — either emitted below or
                // downgraded to HAVE by a parent's mark reached from some
                // other level. So the frontier cannot run dry while `pending`
                // is non-empty, on healthy *or* corrupt commit-graph data:
                // this is believed unreachable. Treat it as a hard error
                // instead of silently truncating the pack, in case that
                // invariant is ever wrong.
                return Err(WalkError::DiscoveryStalled {
                    unresolved: discovery.pending.iter().copied().collect(),
                });
            };

            // Drain every frontier entry at the current maximum generation
            // into one level, in descending-oid order, skipping duplicates and
            // commits already visited in an earlier level.
            let mut level = Vec::new();
            let mut level_seen = HashSet::new();
            while let Some(entry) = discovery.frontier.peek() {
                if entry.generation != level_generation {
                    break;
                }
                let id = discovery
                    .frontier
                    .pop()
                    .expect("peeked entry is present")
                    .oid;
                if !done.contains(&id) && level_seen.insert(id) {
                    level.push(id);
                }
            }

            // The level's own records are already cached (fetched when the
            // commits were marked); load their parents' records as one batch
            // so the marking below is pure synchronous state mutation.
            let parents: Vec<ObjectId> = level
                .iter()
                .filter_map(|id| records.get(id))
                .flat_map(|info| info.parents.iter().copied())
                .collect();
            for (_, record) in self.prefetch_records(&mut records, &parents).await {
                record?; // a parent missing from the graph is corruption
            }

            for id in level {
                if !done.insert(id) {
                    continue;
                }
                let Some(flag) = discovery.state.get(&id).copied() else {
                    continue; // unreachable: everything in the frontier was marked
                };
                if flag == Flag::Want {
                    to_send.push(id);
                    discovery.pending.remove(&id);
                }
                let parents = records
                    .get(&id)
                    .map(|info| info.parents.clone())
                    .unwrap_or_default();
                for parent in parents {
                    let generation = records[&parent].generation;
                    discovery.mark(parent, flag, generation);
                }
            }
        }

        // Discovery ended with `pending` empty (the only non-error exit). If
        // the frontier still holds commits, some ancestry was left unexplored
        // because a recognized have dominated it — the haves bounded the walk.
        // If it drained, the walk reached the roots, as a clone with no usable
        // haves always does.
        let bounded_by_haves = !discovery.frontier.is_empty();

        to_send.extend(tags);
        Ok(Selection {
            to_send,
            common,
            bounded_by_haves,
        })
    }

    /// Phase II: expand the commits chosen by [`Walker::select_commits`]
    /// into the full set of objects to pack, skipping everything already
    /// reachable from the client's haves. Returns a deduplicated list in
    /// insertion order: each commit followed by its new trees and blobs, with
    /// wanted tag objects wherever they appear in `to_send`. Within a commit
    /// the trees and blobs come out in breadth-first order (each depth of the
    /// tree read as one concurrent batch), which the pack tolerates because
    /// entry order does not affect pack correctness (see `src/git/pack_out.rs`).
    ///
    /// The have-side expansion walks every have's root tree in one shared
    /// level-order traversal and visits each reachable tree/blob once (the
    /// have-set doubles as its own seen-set); the want-side walk prunes an
    /// entire subtree at the first tree the client already has and adds blobs
    /// only when they are absent from the have-set, so an incremental fetch is
    /// proportional to the diff, not the snapshot. `filter` restricts which
    /// blobs are added on the want side (`blob:none` also keeps blobs out of
    /// the have-set; see [`FilterSpec`]) — commits, trees, and tags are always
    /// collected in full.
    pub async fn collect(
        &self,
        to_send: &[ObjectId],
        haves: &[ObjectId],
        filter: &FilterSpec,
    ) -> Result<Vec<ObjectId>, WalkError> {
        // Prefetch every have's record at once, then walk all recognized haves'
        // root trees together. Mirror discovery's excusing: an unknown (or
        // non-commit) have is ignored; deeper misses are corruption and error.
        let mut records: HashMap<ObjectId, CommitGraphRecord> = HashMap::new();
        let mut have_roots = Vec::new();
        for (have, record) in self.prefetch_records(&mut records, haves).await {
            match record {
                Ok(info) => have_roots.push(info.root_tree),
                Err(WalkError::MissingObject(missing)) if missing == have => {}
                Err(WalkError::UnexpectedKind { oid, .. }) if oid == have => {}
                Err(err) => return Err(err),
            }
        }
        let mut have_objects: HashSet<ObjectId> = HashSet::new();
        self.expand_haves(&have_roots, &mut have_objects, filter)
            .await?;

        // Classify the whole send list in one batch, then prefetch every
        // commit's record so each commit's tree walk starts without a lookup.
        let mut classified = Vec::with_capacity(to_send.len());
        let mut commit_ids = Vec::new();
        for (id, size) in self.object_sizes(to_send).await {
            let (kind, _) = size?.ok_or(WalkError::MissingObject(id))?;
            if kind == Kind::Commit {
                commit_ids.push(id);
            }
            classified.push((id, kind));
        }
        for (_, record) in self.prefetch_records(&mut records, &commit_ids).await {
            record?;
        }

        let mut objects = OrderedOidSet::default();
        for (id, kind) in classified {
            match kind {
                Kind::Commit => {
                    objects.insert(id);
                    let root_tree = records[&id].root_tree;
                    self.walk_tree(root_tree, &mut objects, &have_objects, filter)
                        .await?;
                }
                Kind::Tag => objects.insert(id),
                actual => {
                    return Err(WalkError::UnexpectedKind {
                        oid: id,
                        expected: Kind::Commit,
                        actual,
                    });
                }
            }
        }
        Ok(objects.into_vec())
    }

    /// Resolve raw wants into commit wants plus the annotated tag objects to
    /// send. Each want's tag chain is chased tag-by-tag (each tag object in the
    /// chain is sent) until a commit is reached; any other target kind is
    /// unsupported. The chains run as one concurrent batch but their results
    /// are merged in want order, so `commit_wants` keeps want order and `tags`
    /// keeps first-seen order, and the first want to fail (in want order)
    /// surfaces its error.
    async fn peel_wants(
        &self,
        wants: &[ObjectId],
    ) -> Result<(Vec<ObjectId>, Vec<ObjectId>), WalkError> {
        let chains: Vec<Result<(ObjectId, Vec<ObjectId>), WalkError>> =
            stream::iter(wants.iter().copied())
                .map(|want| self.peel_want(want))
                .buffered(WALK_LOOKAHEAD)
                .collect()
                .await;

        let mut commit_wants = Vec::new();
        let mut tags = OrderedOidSet::default();
        for chain in chains {
            let (commit, chain_tags) = chain?;
            commit_wants.push(commit);
            for tag in chain_tags {
                tags.insert(tag);
            }
        }
        Ok((commit_wants, tags.into_vec()))
    }

    /// Peel one want to its target commit, returning that commit and the tag
    /// objects walked through on the way (in chain order). The chain is serial
    /// — each hop's target is only known after decoding the current tag.
    async fn peel_want(&self, want: ObjectId) -> Result<(ObjectId, Vec<ObjectId>), WalkError> {
        let mut cursor = want;
        let mut tags = Vec::new();
        loop {
            let Some((kind, body)) = self.objects.get(self.repo, &cursor).await? else {
                return Err(if cursor == want {
                    WalkError::UnknownWant(want)
                } else {
                    // A tag in the chain references a missing object:
                    // server-side corruption, not a client mistake.
                    WalkError::MissingObject(cursor)
                });
            };
            match kind {
                Kind::Commit => return Ok((cursor, tags)),
                Kind::Tag => {
                    tags.push(cursor);
                    let tag = TagRef::from_bytes(&body, cursor.kind()).map_err(|source| {
                        WalkError::Decode {
                            oid: cursor,
                            source,
                        }
                    })?;
                    cursor = tag.target();
                }
                kind => return Err(WalkError::UnsupportedWant { oid: cursor, kind }),
            }
        }
    }

    /// Backfill of [`Walker::get_commit_info`]: compute and persist records
    /// for `id` and every ancestor that lacks one. Iterative post-order over
    /// an explicit stack — a commit is resolved only after all its parents
    /// are, so each record's generation is `1 + max(parent generations)`.
    ///
    /// Persisted with relaxed durability and no flush barrier: a
    /// commit-graph record is derived, rebuildable data, so losing one to a
    /// crash before its WAL write is flushed only means a future call
    /// recomputes it from the commit objects, which are already durable.
    async fn backfill_commit_info(&self, id: &oid) -> Result<CommitGraphRecord, WalkError> {
        let target = id.to_owned();
        // Parsed commit bodies awaiting their parents' generations.
        let mut parsed: HashMap<ObjectId, (ObjectId, Vec<ObjectId>)> = HashMap::new();
        // Generations resolved during this backfill (from the store or
        // freshly computed); doubles as the done-set for duplicate stack
        // entries in diamond-shaped ancestries.
        let mut generations: HashMap<ObjectId, u32> = HashMap::new();
        let mut result = None;

        let mut stack = vec![target];
        while let Some(&top) = stack.last() {
            if generations.contains_key(&top) {
                stack.pop();
                continue;
            }
            if let Some(record) = self.store.get_commit_graph(self.repo, &top).await? {
                generations.insert(top, record.generation);
                stack.pop();
                continue;
            }
            let (root_tree, parents) = match parsed.get(&top) {
                Some(entry) => entry.clone(),
                None => {
                    // This is where the commit object itself is read from the
                    // object database (and parsed into its root tree and
                    // parents); everything else in the backfill consumes
                    // commit-graph records.
                    let entry = self.parse_commit(&top).await?;
                    parsed.insert(top, entry.clone());
                    entry
                }
            };
            let missing: Vec<ObjectId> = parents
                .iter()
                .filter(|parent| !generations.contains_key(*parent))
                .copied()
                .collect();
            if missing.is_empty() {
                let generation = 1 + parents
                    .iter()
                    .filter_map(|parent| generations.get(parent))
                    .max()
                    .copied()
                    .unwrap_or(0);
                let record = CommitGraphRecord {
                    generation,
                    root_tree,
                    parents,
                };
                self.store
                    .put_commit_graph(self.repo, &top, &record, Durability::Relaxed)
                    .await?;
                generations.insert(top, generation);
                if top == target {
                    result = Some(record);
                }
                stack.pop();
            } else {
                stack.extend(missing);
            }
        }

        result.ok_or(WalkError::MissingObject(target))
    }

    /// Parse a commit object into its root tree and parents.
    async fn parse_commit(&self, id: &oid) -> Result<(ObjectId, Vec<ObjectId>), WalkError> {
        let Some((kind, body)) = self.objects.get(self.repo, id).await? else {
            return Err(WalkError::MissingObject(id.to_owned()));
        };
        if kind != Kind::Commit {
            return Err(WalkError::UnexpectedKind {
                oid: id.to_owned(),
                expected: Kind::Commit,
                actual: kind,
            });
        }
        let commit =
            CommitRef::from_bytes(&body, id.kind()).map_err(|source| WalkError::Decode {
                oid: id.to_owned(),
                source,
            })?;
        Ok((commit.tree(), commit.parents().collect()))
    }

    /// Insert every tree and blob reachable from `roots` into `have_objects`,
    /// walking all roots together one tree depth at a time: each level's trees
    /// are read as one concurrent batch, then their child subtrees form the
    /// next level. The set doubles as the seen-set, so a subtree shared between
    /// several haves (or several roots) is visited once and the total cost is
    /// bounded by the number of distinct objects reachable from the haves.
    /// `FilterSpec::BlobNone` keeps blobs out of the have-set too — a filtered
    /// have-side already has no blobs, so its have-set should not claim any.
    async fn expand_haves(
        &self,
        roots: &[ObjectId],
        have_objects: &mut HashSet<ObjectId>,
        filter: &FilterSpec,
    ) -> Result<(), WalkError> {
        let mut level = Vec::new();
        for root in roots {
            if have_objects.insert(*root) {
                level.push(*root);
            }
        }
        while !level.is_empty() {
            let mut next = Vec::new();
            for entries in self.read_trees(&level).await? {
                for entry in entries {
                    match entry {
                        TreeEntry::Subtree(child) => {
                            // Marking on discovery (rather than when the child
                            // is read) dedups the next level up front; the set
                            // ends up identical either way.
                            if have_objects.insert(child) {
                                next.push(child);
                            }
                        }
                        TreeEntry::Leaf(child) => {
                            if !matches!(filter, FilterSpec::BlobNone) {
                                have_objects.insert(child);
                            }
                        }
                    }
                }
            }
            level = next;
        }
        Ok(())
    }

    /// Collect the trees and blobs under `root_tree` into `objects`, skipping
    /// anything the client already has. The tree is walked one depth at a
    /// time: each level's trees are read as one concurrent batch and, for a
    /// size filter, that level's candidate blobs are size-checked as one batch
    /// too. Within a level the newly reached subtrees are collected first (in
    /// parent order, then entry order), then the admitted blobs in the same
    /// order — so a commit's objects come out in breadth-first order.
    ///
    /// Content addressing means an identical tree id implies identical content
    /// all the way down, so the walk prunes a whole subtree at the first tree
    /// found in `have_objects` (or already collected — `objects` memoizes trees
    /// shared between the commits being sent). Trees are always collected in
    /// full; `filter` decides which of the remaining blobs are admitted.
    async fn walk_tree(
        &self,
        root_tree: ObjectId,
        objects: &mut OrderedOidSet,
        have_objects: &HashSet<ObjectId>,
        filter: &FilterSpec,
    ) -> Result<(), WalkError> {
        let mut level = Vec::new();
        if !objects.contains(&root_tree) && !have_objects.contains(&root_tree) {
            objects.insert(root_tree);
            level.push(root_tree);
        }
        while !level.is_empty() {
            let mut next = Vec::new();
            let mut blob_candidates = Vec::new();
            let mut candidate_seen = HashSet::new();
            for entries in self.read_trees(&level).await? {
                for entry in entries {
                    match entry {
                        TreeEntry::Subtree(child) => {
                            if !objects.contains(&child) && !have_objects.contains(&child) {
                                objects.insert(child);
                                next.push(child);
                            }
                        }
                        // A blob is a candidate only when the client's
                        // have-frontier lacks it and it is not already
                        // collected; the filter has the final say below.
                        TreeEntry::Leaf(child) => {
                            if !have_objects.contains(&child)
                                && !objects.contains(&child)
                                && candidate_seen.insert(child)
                            {
                                blob_candidates.push(child);
                            }
                        }
                    }
                }
            }
            self.admit_blobs(blob_candidates, objects, filter).await?;
            level = next;
        }
        Ok(())
    }

    /// Add the blobs `filter` admits from `candidates` to `objects`, in
    /// candidate order. `BlobNone` admits none and `None` admits all without a
    /// lookup; `BlobLimit` decides from [`ObjectDb::size`] alone — the object
    /// record's inline header or pointer payload — read as one concurrent
    /// batch and never fetching any blob's content.
    async fn admit_blobs(
        &self,
        candidates: Vec<ObjectId>,
        objects: &mut OrderedOidSet,
        filter: &FilterSpec,
    ) -> Result<(), WalkError> {
        match *filter {
            FilterSpec::None => {
                for child in candidates {
                    objects.insert(child);
                }
            }
            FilterSpec::BlobNone => {}
            FilterSpec::BlobLimit(limit) => {
                for (child, size) in self.object_sizes(&candidates).await {
                    let (_, size) = size?.ok_or(WalkError::MissingObject(child))?;
                    if size <= limit {
                        objects.insert(child);
                    }
                }
            }
        }
        Ok(())
    }

    /// Read and parse a tree object into walk-relevant entries, dropping
    /// gitlink (submodule) entries, whose targets live in other
    /// repositories.
    async fn read_tree(&self, id: &oid) -> Result<Vec<TreeEntry>, WalkError> {
        let Some((kind, body)) = self.objects.get(self.repo, id).await? else {
            return Err(WalkError::MissingObject(id.to_owned()));
        };
        if kind != Kind::Tree {
            return Err(WalkError::UnexpectedKind {
                oid: id.to_owned(),
                expected: Kind::Tree,
                actual: kind,
            });
        }
        let mut entries = Vec::new();
        for entry in TreeRefIter::from_bytes(&body, id.kind()) {
            let entry = entry.map_err(|source| WalkError::Decode {
                oid: id.to_owned(),
                source,
            })?;
            if entry.mode.is_tree() {
                entries.push(TreeEntry::Subtree(entry.oid.to_owned()));
            } else if !entry.mode.is_commit() {
                entries.push(TreeEntry::Leaf(entry.oid.to_owned()));
            }
        }
        Ok(entries)
    }

    /// Read and parse every tree in `ids` as one bounded-width concurrent
    /// batch (see [`Walker::read_tree`]), returning their entries aligned to
    /// `ids`. Lookups run concurrently but results are yielded in input order,
    /// so the caller consumes them deterministically and the first failing
    /// tree (in input order) surfaces its error.
    async fn read_trees(&self, ids: &[ObjectId]) -> Result<Vec<Vec<TreeEntry>>, WalkError> {
        stream::iter(ids.iter().copied())
            .map(|id| async move { self.read_tree(&id).await })
            .buffered(WALK_LOOKAHEAD)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
    }

    /// Read the kind and content size of every id in `ids` as one bounded-width
    /// concurrent batch, pairing each id with its [`ObjectDb::size`] result in
    /// input order. Never touches blob content; each caller maps an absent
    /// record to the error it wants (an unknown want vs. a missing object).
    async fn object_sizes(
        &self,
        ids: &[ObjectId],
    ) -> Vec<(ObjectId, Result<Option<(Kind, u64)>, ObjectDbError>)> {
        stream::iter(ids.iter().copied())
            .map(|id| async move { (id, self.objects.size(self.repo, &id).await) })
            .buffered(WALK_LOOKAHEAD)
            .collect()
            .await
    }

    /// Load the commit-graph records for `ids` that are not already in `cache`,
    /// as one bounded-width concurrent batch, inserting the successes into
    /// `cache`. Returns each freshly fetched id paired with its result in
    /// first-seen input order (ids already cached are omitted — the cache
    /// already holds their record). Concurrent lazy backfills of overlapping
    /// ancestries are safe: the records they write are idempotent derived data.
    async fn prefetch_records(
        &self,
        cache: &mut HashMap<ObjectId, CommitGraphRecord>,
        ids: &[ObjectId],
    ) -> Vec<(ObjectId, Result<CommitGraphRecord, WalkError>)> {
        let mut misses = Vec::new();
        let mut seen = HashSet::new();
        for id in ids {
            if !cache.contains_key(id) && seen.insert(*id) {
                misses.push(*id);
            }
        }
        let fetched: Vec<(ObjectId, Result<CommitGraphRecord, WalkError>)> = stream::iter(misses)
            .map(|id| async move { (id, self.get_commit_info(&id).await) })
            .buffered(WALK_LOOKAHEAD)
            .collect()
            .await;
        for (id, record) in &fetched {
            if let Ok(record) = record {
                cache.insert(*id, record.clone());
            }
        }
        fetched
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use gix_object::WriteTo;
    use gix_object::tree::{Entry, EntryKind};
    use slatedb::object_store::{self, ObjectStore};
    use std::sync::Arc;

    use crate::storage::BlobStore;
    use crate::storage::values::ObjectRecord;

    /// A walker over a fresh `memory://` store with one repository.
    async fn walker() -> Walker {
        let store = Store::open("memory://").await.expect("open store");
        let repo = store.create_repo("walk").await.expect("create repo").id;
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let blobs = BlobStore::new(backing);
        let objects = ObjectDb::new(store.clone(), blobs, 65536, 6);
        Walker::new(store, objects, repo)
    }

    /// Hash `body` as a real git object of `kind` and store it.
    async fn put_object(w: &Walker, kind: Kind, body: &[u8]) -> ObjectId {
        let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, body).expect("hash");
        w.objects
            .put(
                w.repo,
                &id,
                kind,
                Bytes::copy_from_slice(body),
                Durability::Durable,
            )
            .await
            .expect("put object");
        id
    }

    async fn blob(w: &Walker, content: &[u8]) -> ObjectId {
        put_object(w, Kind::Blob, content).await
    }

    /// Encode a tree via gix-object (entries are sorted per git rules).
    async fn tree(w: &Walker, entries: &[(EntryKind, &str, ObjectId)]) -> ObjectId {
        let mut tree = gix_object::Tree {
            entries: entries
                .iter()
                .map(|(kind, name, id)| Entry {
                    mode: (*kind).into(),
                    filename: (*name).into(),
                    oid: *id,
                })
                .collect(),
        };
        tree.entries.sort();
        let mut body = Vec::new();
        tree.write_to(&mut body).expect("encode tree");
        put_object(w, Kind::Tree, &body).await
    }

    /// Store a commit with the canonical body encoding. No commit-graph
    /// record is written: the walk backfills records lazily, so every test
    /// exercises the backfill path unless it seeds records itself.
    async fn commit(w: &Walker, tree: ObjectId, parents: &[ObjectId], message: &str) -> ObjectId {
        let mut body = format!("tree {tree}\n");
        for parent in parents {
            body.push_str(&format!("parent {parent}\n"));
        }
        body.push_str("author A U Thor <author@example.com> 946684800 +0000\n");
        body.push_str("committer A U Thor <author@example.com> 946684800 +0000\n");
        body.push('\n');
        body.push_str(message);
        body.push('\n');
        put_object(w, Kind::Commit, body.as_bytes()).await
    }

    /// Store an annotated tag object pointing at `target`.
    async fn tag(w: &Walker, target: ObjectId, target_kind: Kind, name: &str) -> ObjectId {
        let body = format!(
            "object {target}\ntype {target_kind}\ntag {name}\n\
             tagger A U Thor <author@example.com> 946684800 +0000\n\nrelease {name}\n"
        );
        put_object(w, Kind::Tag, body.as_bytes()).await
    }

    /// A linear chain of `n` commits, each snapshotting one distinct file
    /// content. Returns the commit ids, oldest first.
    async fn chain(w: &Walker, n: usize) -> Vec<ObjectId> {
        let mut commits = Vec::new();
        let mut parent: Option<ObjectId> = None;
        for i in 0..n {
            let content = format!("content {i}");
            let file = blob(w, content.as_bytes()).await;
            let root = tree(w, &[(EntryKind::Blob, "file.txt", file)]).await;
            let parents: Vec<ObjectId> = parent.into_iter().collect();
            let id = commit(w, root, &parents, &format!("commit {i}")).await;
            commits.push(id);
            parent = Some(id);
        }
        commits
    }

    /// A deterministic oid that no fixture object hashes to.
    fn bogus_oid() -> ObjectId {
        ObjectId::from_hex(&[b'f'; 40]).expect("valid hex")
    }

    fn as_set(ids: &[ObjectId]) -> HashSet<ObjectId> {
        ids.iter().copied().collect()
    }

    #[tokio::test]
    async fn should_return_existing_graph_record_without_reading_objects() {
        // given: a commit-graph record for an oid with no stored object,
        // so any backfill attempt would fail with MissingObject.
        let w = walker().await;
        let id = ObjectId::from_hex(&[b'a'; 40]).expect("valid hex");
        let record = CommitGraphRecord {
            generation: 7,
            root_tree: ObjectId::from_hex(&[b'b'; 40]).expect("valid hex"),
            parents: vec![ObjectId::from_hex(&[b'c'; 40]).expect("valid hex")],
        };
        w.store
            .put_commit_graph(w.repo, &id, &record, Durability::Durable)
            .await
            .expect("seed record");

        // when
        let info = w.get_commit_info(&id).await.expect("get info");

        // then
        assert_eq!(info, record);
    }

    #[tokio::test]
    async fn should_backfill_generations_across_a_merge() {
        // given: a merge topology with no commit-graph records at all.
        //   c1 <- c2a <- m
        //     \-- c2b --/
        let w = walker().await;
        let base = chain(&w, 1).await;
        let c1 = base[0];
        let blob_a = blob(&w, b"side a").await;
        let tree_a = tree(&w, &[(EntryKind::Blob, "file.txt", blob_a)]).await;
        let c2a = commit(&w, tree_a, &[c1], "side a").await;
        let blob_b = blob(&w, b"side b").await;
        let tree_b = tree(&w, &[(EntryKind::Blob, "file.txt", blob_b)]).await;
        let c2b = commit(&w, tree_b, &[c1], "side b").await;
        let merge_tree = tree(
            &w,
            &[
                (EntryKind::Blob, "a.txt", blob_a),
                (EntryKind::Blob, "b.txt", blob_b),
            ],
        )
        .await;
        let m = commit(&w, merge_tree, &[c2a, c2b], "merge").await;

        // when: asking for the merge backfills its whole ancestry.
        let info = w.get_commit_info(&m).await.expect("merge info");

        // then: generation is 1 + max(parent generations), roots are 1.
        assert_eq!(info.generation, 3);
        assert_eq!(info.root_tree, merge_tree);
        assert_eq!(info.parents, vec![c2a, c2b]);
        assert_eq!(w.get_commit_info(&c2a).await.expect("c2a").generation, 2);
        assert_eq!(w.get_commit_info(&c2b).await.expect("c2b").generation, 2);
        assert_eq!(w.get_commit_info(&c1).await.expect("c1").generation, 1);
        // The backfilled records were persisted, not just computed.
        assert_eq!(
            w.store
                .get_commit_graph(w.repo, &c1)
                .await
                .expect("get record")
                .map(|r| r.generation),
            Some(1)
        );
    }

    #[tokio::test]
    async fn should_send_the_whole_chain_for_a_clone_without_haves() {
        // given
        let w = walker().await;
        let commits = chain(&w, 3).await;

        // when
        let selection = w.select_commits(&[commits[2]], &[]).await.expect("select");

        // then: every commit is sent, newest (highest generation) first.
        assert_eq!(selection.to_send, vec![commits[2], commits[1], commits[0]]);
        assert!(selection.common.is_empty());
    }

    #[tokio::test]
    async fn should_send_only_commits_newer_than_the_have() {
        // given
        let w = walker().await;
        let commits = chain(&w, 4).await;

        // when: the client has the second commit and wants the tip.
        let selection = w
            .select_commits(&[commits[3]], &[commits[1]])
            .await
            .expect("select");

        // then: the have's entire ancestry is excluded.
        assert_eq!(selection.to_send, vec![commits[3], commits[2]]);
        assert_eq!(selection.common, vec![commits[1]]);
    }

    #[tokio::test]
    async fn should_send_nothing_when_have_dominates_want() {
        // given
        let w = walker().await;
        let commits = chain(&w, 3).await;

        // when: the want is an ancestor of the have.
        let dominated = w
            .select_commits(&[commits[0]], &[commits[2]])
            .await
            .expect("select");

        // then
        assert!(dominated.to_send.is_empty());
        assert_eq!(dominated.common, vec![commits[2]]);

        // when: the want IS the have.
        let identical = w
            .select_commits(&[commits[2]], &[commits[2]])
            .await
            .expect("select");

        // then
        assert!(identical.to_send.is_empty());
        assert_eq!(identical.common, vec![commits[2]]);
    }

    #[tokio::test]
    async fn should_stop_discovery_at_have_dominated_merge_parent() {
        // given: c1 <- c2a <- m, c1 <- c2b <- m; the client has side a.
        let w = walker().await;
        let base = chain(&w, 1).await;
        let c1 = base[0];
        let blob_a = blob(&w, b"side a").await;
        let tree_a = tree(&w, &[(EntryKind::Blob, "a.txt", blob_a)]).await;
        let c2a = commit(&w, tree_a, &[c1], "side a").await;
        let blob_b = blob(&w, b"side b").await;
        let tree_b = tree(&w, &[(EntryKind::Blob, "b.txt", blob_b)]).await;
        let c2b = commit(&w, tree_b, &[c1], "side b").await;
        let merge_tree = tree(
            &w,
            &[
                (EntryKind::Blob, "a.txt", blob_a),
                (EntryKind::Blob, "b.txt", blob_b),
            ],
        )
        .await;
        let m = commit(&w, merge_tree, &[c2a, c2b], "merge").await;

        // when
        let selection = w.select_commits(&[m], &[c2a]).await.expect("select");

        // then: only the merge and the un-had side are sent; the shared
        // root is dominated through the have and discovery terminates.
        assert_eq!(selection.to_send, vec![m, c2b]);
        assert_eq!(selection.common, vec![c2a]);
    }

    #[tokio::test]
    async fn should_ignore_unknown_have_and_report_known_have_as_common() {
        // given
        let w = walker().await;
        let commits = chain(&w, 2).await;

        // when: one have is entirely unknown to the server.
        let selection = w
            .select_commits(&[commits[1]], &[bogus_oid(), commits[0]])
            .await
            .expect("select");

        // then: the unknown have is dropped, the known one is common.
        assert_eq!(selection.common, vec![commits[0]]);
        assert_eq!(selection.to_send, vec![commits[1]]);
    }

    #[tokio::test]
    async fn should_flag_bounded_by_haves_only_when_a_have_prunes_the_walk() {
        // given: a linear chain
        let w = walker().await;
        let commits = chain(&w, 3).await;

        // when: a clone with no haves — the walk drains to the root
        let clone = w.select_commits(&[commits[2]], &[]).await.expect("select");
        // then: the frontier emptied, so it is not bounded by haves
        assert!(!clone.bounded_by_haves);

        // when: an incremental fetch whose have dominates the tail
        let incremental = w
            .select_commits(&[commits[2]], &[commits[0]])
            .await
            .expect("select");
        // then: the have pruned the ancestry, leaving the frontier non-empty
        assert!(incremental.bounded_by_haves);

        // when: the only offered have is unknown to the server
        let unknown = w
            .select_commits(&[commits[2]], &[bogus_oid()])
            .await
            .expect("select");
        // then: nothing was common, so the walk ran to the root — not bounded
        assert!(!unknown.bounded_by_haves);
    }

    #[tokio::test]
    async fn should_error_on_unknown_want() {
        // given
        let w = walker().await;
        chain(&w, 1).await;

        // when
        let result = w.select_commits(&[bogus_oid()], &[]).await;

        // then
        assert!(matches!(result, Err(WalkError::UnknownWant(id)) if id == bogus_oid()));
    }

    #[tokio::test]
    async fn should_partition_wants_into_history_and_direct() {
        // given: one object of each servable kind
        let w = walker().await;
        let b = blob(&w, b"content").await;
        let t = tree(&w, &[(EntryKind::Blob, "file.txt", b)]).await;
        let c = commit(&w, t, &[], "one").await;
        let tg = tag(&w, c, Kind::Commit, "v1").await;

        // when
        let partition = w.partition_wants(&[c, b, t, tg]).await.expect("partition");

        // then: commits and tags drive discovery; blobs and trees are direct
        assert_eq!(partition.history, vec![c, tg]);
        assert_eq!(as_set(&partition.direct), as_set(&[b, t]));
    }

    #[tokio::test]
    async fn should_dedup_repeated_direct_wants() {
        // given: one blob wanted twice
        let w = walker().await;
        let b = blob(&w, b"content").await;

        // when
        let partition = w.partition_wants(&[b, b]).await.expect("partition");

        // then: the direct set holds it exactly once, with no history wants
        assert_eq!(partition.direct, vec![b]);
        assert!(partition.history.is_empty());
    }

    #[tokio::test]
    async fn should_error_when_partitioning_an_unknown_want() {
        // given
        let w = walker().await;
        chain(&w, 1).await;

        // when
        let result = w.partition_wants(&[bogus_oid()]).await;

        // then
        assert!(matches!(result, Err(WalkError::UnknownWant(id)) if id == bogus_oid()));
    }

    #[tokio::test]
    async fn should_peel_annotated_tag_chain_and_send_tag_objects() {
        // given: tag2 -> tag1 -> c2 (a tag-of-tag chain onto the tip).
        let w = walker().await;
        let commits = chain(&w, 2).await;
        let tag1 = tag(&w, commits[1], Kind::Commit, "v1").await;
        let tag2 = tag(&w, tag1, Kind::Tag, "v1-signed").await;

        // when
        let selection = w.select_commits(&[tag2], &[]).await.expect("select");

        // then: the want peeled to the commit and both tag objects ride
        // along after the commits.
        assert_eq!(selection.to_send, vec![commits[1], commits[0], tag2, tag1]);

        // when: collecting the selection.
        let collected = w
            .collect(&selection.to_send, &[], &FilterSpec::None)
            .await
            .expect("collect");

        // then: both tag objects are in the packed set exactly once, next
        // to the full commit closure (2 commits + 2 trees + 2 blobs).
        assert!(collected.contains(&tag1));
        assert!(collected.contains(&tag2));
        assert_eq!(collected.len(), 8);
        assert_eq!(as_set(&collected).len(), 8);
    }

    #[tokio::test]
    async fn should_collect_the_full_closure_for_a_clone() {
        // given: two commits with distinct trees and blobs.
        let w = walker().await;
        let b1 = blob(&w, b"one").await;
        let t1 = tree(&w, &[(EntryKind::Blob, "file.txt", b1)]).await;
        let c1 = commit(&w, t1, &[], "one").await;
        let b2 = blob(&w, b"two").await;
        let t2 = tree(&w, &[(EntryKind::Blob, "file.txt", b2)]).await;
        let c2 = commit(&w, t2, &[c1], "two").await;

        // when: collecting with no haves (a clone).
        let collected = w
            .collect(&[c2, c1], &[], &FilterSpec::None)
            .await
            .expect("collect");

        // then: exactly the six objects, commits leading their snapshots.
        assert_eq!(collected, vec![c2, t2, b2, c1, t1, b1]);
    }

    #[tokio::test]
    async fn should_prune_shared_subtrees_and_blobs_with_a_have() {
        // given: c2 changes one file under `a/` and keeps `a/keep.txt` and
        // the whole `b/` subtree identical to c1.
        let w = walker().await;
        let blob_a1 = blob(&w, b"a v1").await;
        let blob_keep = blob(&w, b"keep").await;
        let blob_b = blob(&w, b"b").await;
        let tree_a1 = tree(
            &w,
            &[
                (EntryKind::Blob, "a.txt", blob_a1),
                (EntryKind::Blob, "keep.txt", blob_keep),
            ],
        )
        .await;
        let tree_b = tree(&w, &[(EntryKind::Blob, "b.txt", blob_b)]).await;
        let root1 = tree(
            &w,
            &[
                (EntryKind::Tree, "a", tree_a1),
                (EntryKind::Tree, "b", tree_b),
            ],
        )
        .await;
        let c1 = commit(&w, root1, &[], "one").await;
        let blob_a2 = blob(&w, b"a v2").await;
        let tree_a2 = tree(
            &w,
            &[
                (EntryKind::Blob, "a.txt", blob_a2),
                (EntryKind::Blob, "keep.txt", blob_keep),
            ],
        )
        .await;
        let root2 = tree(
            &w,
            &[
                (EntryKind::Tree, "a", tree_a2),
                (EntryKind::Tree, "b", tree_b),
            ],
        )
        .await;
        let c2 = commit(&w, root2, &[c1], "two").await;

        // when: an incremental fetch of c2 with c1 as the have.
        let selection = w.select_commits(&[c2], &[c1]).await.expect("select");
        let collected = w
            .collect(&selection.to_send, &selection.common, &FilterSpec::None)
            .await
            .expect("collect");

        // then: exactly the diff closure — the shared `b/` subtree is
        // pruned at its root and the unchanged blob inside the changed
        // `a/` subtree is skipped via the have-frontier.
        assert_eq!(selection.to_send, vec![c2]);
        assert_eq!(as_set(&collected), as_set(&[c2, root2, tree_a2, blob_a2]));
    }

    #[tokio::test]
    async fn should_keep_collection_proportional_to_the_diff_when_root_changes() {
        // given: a three-level nesting where only one deeply nested blob
        // changes; siblings at every level stay shared.
        let w = walker().await;
        let blob_mid = blob(&w, b"mid").await;
        let blob_s = blob(&w, b"sibling").await;
        let blob_top = blob(&w, b"top").await;
        let tree_s = tree(&w, &[(EntryKind::Blob, "s.txt", blob_s)]).await;

        let leaf1 = blob(&w, b"deep v1").await;
        let d2_1 = tree(&w, &[(EntryKind::Blob, "deep.txt", leaf1)]).await;
        let d1_1 = tree(
            &w,
            &[
                (EntryKind::Tree, "d2", d2_1),
                (EntryKind::Blob, "mid.txt", blob_mid),
            ],
        )
        .await;
        let root1 = tree(
            &w,
            &[
                (EntryKind::Tree, "d1", d1_1),
                (EntryKind::Tree, "s", tree_s),
                (EntryKind::Blob, "top.txt", blob_top),
            ],
        )
        .await;
        let c1 = commit(&w, root1, &[], "one").await;

        let leaf2 = blob(&w, b"deep v2").await;
        let d2_2 = tree(&w, &[(EntryKind::Blob, "deep.txt", leaf2)]).await;
        let d1_2 = tree(
            &w,
            &[
                (EntryKind::Tree, "d2", d2_2),
                (EntryKind::Blob, "mid.txt", blob_mid),
            ],
        )
        .await;
        let root2 = tree(
            &w,
            &[
                (EntryKind::Tree, "d1", d1_2),
                (EntryKind::Tree, "s", tree_s),
                (EntryKind::Blob, "top.txt", blob_top),
            ],
        )
        .await;
        let c2 = commit(&w, root2, &[c1], "two").await;

        // when
        let collected = w
            .collect(&[c2], &[c1], &FilterSpec::None)
            .await
            .expect("collect");

        // then: even though the root tree changed, only the changed spine
        // (commit, three rewritten trees, one new blob) is collected.
        assert_eq!(as_set(&collected), as_set(&[c2, root2, d1_2, d2_2, leaf2]));
    }

    #[tokio::test]
    async fn should_backfill_a_missing_middle_graph_record() {
        // given: a three-commit chain whose middle commit-graph record is
        // absent while its neighbors' records exist (as after a partial
        // graph wipe; the store keeps records only for c1 and c3).
        let w = walker().await;
        let commits = chain(&w, 3).await;
        let (c1, c2, c3) = (commits[0], commits[1], commits[2]);
        let t1 = w.parse_commit(&c1).await.expect("parse c1").0;
        let t2 = w.parse_commit(&c2).await.expect("parse c2").0;
        let t3 = w.parse_commit(&c3).await.expect("parse c3").0;
        w.store
            .put_commit_graph(
                w.repo,
                &c1,
                &CommitGraphRecord {
                    generation: 1,
                    root_tree: t1,
                    parents: vec![],
                },
                Durability::Durable,
            )
            .await
            .expect("seed c1");
        w.store
            .put_commit_graph(
                w.repo,
                &c3,
                &CommitGraphRecord {
                    generation: 3,
                    root_tree: t3,
                    parents: vec![c2],
                },
                Durability::Durable,
            )
            .await
            .expect("seed c3");
        assert_eq!(
            w.store.get_commit_graph(w.repo, &c2).await.expect("get"),
            None
        );

        // when: a full walk through the gap.
        let selection = w.select_commits(&[c3], &[]).await.expect("select");

        // then: the walk succeeded and rewrote the missing record.
        assert_eq!(selection.to_send, vec![c3, c2, c1]);
        assert_eq!(
            w.store.get_commit_graph(w.repo, &c2).await.expect("get"),
            Some(CommitGraphRecord {
                generation: 2,
                root_tree: t2,
                parents: vec![c1],
            })
        );
    }

    #[tokio::test]
    async fn should_over_serve_rather_than_drop_a_want_under_corrupt_generations() {
        // given: a chain c1 <- c2 <- c3 plus an unrelated root commit d1,
        // with every commit-graph record seeded by hand (root trees and
        // parents accurate; only the generations lie): gen(c2)=10 outranks
        // the have c3's gen(c3)=5, so c2 is popped and emitted before c3's
        // have-propagation reaches it.
        let w = walker().await;
        let commits = chain(&w, 3).await;
        let (c1, c2, c3) = (commits[0], commits[1], commits[2]);
        let t1 = w.parse_commit(&c1).await.expect("parse c1").0;
        let t2 = w.parse_commit(&c2).await.expect("parse c2").0;
        let t3 = w.parse_commit(&c3).await.expect("parse c3").0;
        let d1_blob = blob(&w, b"independent root").await;
        let d1_tree = tree(&w, &[(EntryKind::Blob, "d.txt", d1_blob)]).await;
        let d1 = commit(&w, d1_tree, &[], "independent root").await;

        for (id, generation, root_tree, parents) in [
            (c1, 1, t1, vec![]),
            (c2, 10, t2, vec![c1]),
            (c3, 5, t3, vec![c2]),
            (d1, 1, d1_tree, vec![]),
        ] {
            w.store
                .put_commit_graph(
                    w.repo,
                    &id,
                    &CommitGraphRecord {
                        generation,
                        root_tree,
                        parents,
                    },
                    Durability::Durable,
                )
                .await
                .expect("seed corrupt record");
        }

        // when: c2 sorts ahead of the have c3 in the generation-keyed heap,
        // so it is emitted before the have-propagation that should have
        // downgraded it arrives; d1 is an unrelated want resolved later.
        let selection = w.select_commits(&[c2, d1], &[c3]).await.expect("select");

        // then: the walk completes and every want is served. The late
        // downgrade of the already-emitted c2 must be a no-op: if it
        // counted as a second resolution, discovery would stop after
        // resolving only one of {c1, d1} and silently serve an incomplete
        // pack. Over-serving c2 is acceptable; dropping a want is not.
        assert_eq!(as_set(&selection.to_send), as_set(&[c2, c1, d1]));
        assert_eq!(selection.common, vec![c3]);
    }

    #[test]
    fn should_default_to_no_filter_when_absent() {
        // given/when/then
        assert_eq!(FilterSpec::default(), FilterSpec::None);
    }

    #[test]
    fn should_parse_blob_none() {
        // given/when
        let parsed = FilterSpec::parse("blob:none").expect("parse");

        // then
        assert_eq!(parsed, FilterSpec::BlobNone);
    }

    #[test]
    fn should_parse_a_plain_blob_limit() {
        // given/when
        let parsed = FilterSpec::parse("blob:limit=1024").expect("parse");

        // then
        assert_eq!(parsed, FilterSpec::BlobLimit(1024));
    }

    #[test]
    fn should_parse_blob_limit_unit_suffixes() {
        // given/when/then: git accepts k/m/g (either case), base 1024.
        assert_eq!(
            FilterSpec::parse("blob:limit=1k").expect("k"),
            FilterSpec::BlobLimit(1024)
        );
        assert_eq!(
            FilterSpec::parse("blob:limit=1K").expect("K"),
            FilterSpec::BlobLimit(1024)
        );
        assert_eq!(
            FilterSpec::parse("blob:limit=2m").expect("m"),
            FilterSpec::BlobLimit(2 * 1024 * 1024)
        );
        assert_eq!(
            FilterSpec::parse("blob:limit=1g").expect("g"),
            FilterSpec::BlobLimit(1024 * 1024 * 1024)
        );
    }

    #[test]
    fn should_reject_unsupported_filter_specs() {
        // given/when/then: filter kinds the walk does not implement.
        for spec in ["tree:0", "sparse:oid=abc123", "combine:blob:none+tree:1"] {
            assert_eq!(FilterSpec::parse(spec), None, "spec: {spec}");
        }
    }

    #[test]
    fn should_reject_a_blob_limit_with_no_digits() {
        // given/when/then
        assert_eq!(FilterSpec::parse("blob:limit=k"), None);
        assert_eq!(FilterSpec::parse("blob:limit="), None);
    }

    #[tokio::test]
    async fn should_collect_zero_blobs_but_all_trees_and_commits_with_blob_none() {
        // given: two commits with distinct trees and blobs.
        let w = walker().await;
        let b1 = blob(&w, b"one").await;
        let t1 = tree(&w, &[(EntryKind::Blob, "file.txt", b1)]).await;
        let c1 = commit(&w, t1, &[], "one").await;
        let b2 = blob(&w, b"two").await;
        let t2 = tree(&w, &[(EntryKind::Blob, "file.txt", b2)]).await;
        let c2 = commit(&w, t2, &[c1], "two").await;

        // when
        let collected = w
            .collect(&[c2, c1], &[], &FilterSpec::BlobNone)
            .await
            .expect("collect");

        // then: every commit and tree rides along; neither blob does.
        assert_eq!(as_set(&collected), as_set(&[c2, t2, c1, t1]));
        assert!(!collected.contains(&b1));
        assert!(!collected.contains(&b2));
    }

    #[tokio::test]
    async fn should_include_only_blobs_at_or_under_the_limit() {
        // given: one commit whose tree has a small blob and a larger one.
        let w = walker().await;
        let small = blob(&w, b"tiny").await; // 4 bytes
        let big = blob(&w, b"this content is over the limit").await; // 31 bytes
        let root = tree(
            &w,
            &[
                (EntryKind::Blob, "small.txt", small),
                (EntryKind::Blob, "big.txt", big),
            ],
        )
        .await;
        let c = commit(&w, root, &[], "one").await;

        // when: a limit that admits the small blob but not the big one.
        let collected = w
            .collect(&[c], &[], &FilterSpec::BlobLimit(10))
            .await
            .expect("collect");

        // then
        assert_eq!(as_set(&collected), as_set(&[c, root, small]));
    }

    #[tokio::test]
    async fn should_check_offloaded_blob_size_without_reading_blob_content() {
        // given: a tree with a real small blob and a "phantom" blob whose
        // object record is a bare BlobPointer with no matching content ever
        // written to the blob store (as if only its size were durable).
        // Fetching the phantom's content would fail; a size-based filter
        // must never attempt it.
        let w = walker().await;
        let small = blob(&w, b"tiny").await;
        let phantom = ObjectId::from_hex(&[b'5'; 40]).expect("valid hex");
        w.store
            .put_object(
                w.repo,
                &phantom,
                &ObjectRecord::BlobPointer { size: 1_000_000 },
                Durability::Durable,
            )
            .await
            .expect("seed phantom pointer record");
        let root = tree(
            &w,
            &[
                (EntryKind::Blob, "small.txt", small),
                (EntryKind::Blob, "phantom.bin", phantom),
            ],
        )
        .await;
        let c = commit(&w, root, &[], "one").await;

        // when: a limit that admits the small blob but not the phantom's
        // declared size.
        let collected = w
            .collect(&[c], &[], &FilterSpec::BlobLimit(10))
            .await
            .expect("collect");

        // then: the phantom is excluded by its declared size alone — had the
        // walk instead fetched its content, this would have failed with a
        // blob-store lookup error rather than returning successfully.
        assert_eq!(as_set(&collected), as_set(&[c, root, small]));
    }

    #[tokio::test]
    async fn should_return_zero_records_when_every_ref_dangles() {
        // given: a fresh repo, whose only ref (HEAD) symrefs to a branch
        // that was never created.
        let w = walker().await;

        // when
        let count = w.rebuild_commit_graph().await.expect("rebuild");

        // then
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn should_rebuild_records_for_every_commit_reachable_from_refs() {
        // given: a linear chain named by refs/heads/main (HEAD symrefs to it).
        let w = walker().await;
        let commits = chain(&w, 3).await;
        w.store
            .put_ref(w.repo, "refs/heads/main", &RefTarget::Direct(commits[2]))
            .await
            .expect("put main");

        // when
        let count = w.rebuild_commit_graph().await.expect("rebuild");

        // then: exactly the three-commit chain got fresh records, with the
        // expected generations.
        assert_eq!(count, 3);
        for (i, id) in commits.iter().enumerate() {
            let record = w
                .store
                .get_commit_graph(w.repo, id)
                .await
                .expect("get graph")
                .expect("record written");
            assert_eq!(record.generation, (i + 1) as u32);
        }
    }

    #[tokio::test]
    async fn should_root_an_annotated_tag_ref_at_its_target_commit() {
        // given: a tag ref pointing at an annotated tag object over a commit.
        let w = walker().await;
        let commits = chain(&w, 1).await;
        let t = tag(&w, commits[0], Kind::Commit, "v1").await;
        w.store
            .put_ref(w.repo, "refs/tags/v1", &RefTarget::Direct(t))
            .await
            .expect("put tag ref");

        // when
        let count = w.rebuild_commit_graph().await.expect("rebuild");

        // then: the tag object itself is not a commit-graph root; only the
        // commit it points at gets a record.
        assert_eq!(count, 1);
        assert!(
            w.store
                .get_commit_graph(w.repo, &commits[0])
                .await
                .expect("get graph")
                .is_some()
        );
    }

    #[tokio::test]
    async fn should_skip_a_ref_whose_target_is_not_a_commit() {
        // given: a ref pointing directly at a blob.
        let w = walker().await;
        let b = blob(&w, b"not a commit").await;
        w.store
            .put_ref(w.repo, "refs/heads/broken", &RefTarget::Direct(b))
            .await
            .expect("put broken ref");

        // when
        let count = w.rebuild_commit_graph().await.expect("rebuild");

        // then: no commit-graph record is produced for the non-commit ref.
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn should_recompute_a_record_even_when_a_different_value_is_already_stored() {
        // given: a root commit whose stored generation (42) disagrees with
        // the value implied by its actual (empty) parent list. Lazy backfill
        // via `get_commit_info` would trust this record and never touch it;
        // a rebuild must not.
        let w = walker().await;
        let commits = chain(&w, 2).await;
        w.store
            .put_ref(w.repo, "refs/heads/main", &RefTarget::Direct(commits[1]))
            .await
            .expect("put main");
        let root_tree = w.parse_commit(&commits[0]).await.expect("parse").0;
        w.store
            .put_commit_graph(
                w.repo,
                &commits[0],
                &CommitGraphRecord {
                    generation: 42,
                    root_tree,
                    parents: vec![],
                },
                Durability::Durable,
            )
            .await
            .expect("seed stale record");

        // when
        w.rebuild_commit_graph().await.expect("rebuild");

        // then: the rebuild overwrote the stale generation with the value
        // recomputed from the commit object (a root has generation 1).
        let record = w
            .store
            .get_commit_graph(w.repo, &commits[0])
            .await
            .expect("get graph")
            .expect("record written");
        assert_eq!(record.generation, 1);
    }

    #[tokio::test]
    async fn should_select_a_wide_octopus_merge_in_descending_generation_order() {
        // given: a base commit with many sibling children joined by one
        // octopus merge — a frontier that only ever fans out wide, never deep.
        let w = walker().await;
        let c0 = chain(&w, 1).await[0];
        let mut sides = Vec::new();
        for i in 0..8 {
            let b = blob(&w, format!("side {i}").as_bytes()).await;
            let t = tree(&w, &[(EntryKind::Blob, "s.txt", b)]).await;
            sides.push(commit(&w, t, &[c0], &format!("side {i}")).await);
        }
        let merged = blob(&w, b"merged").await;
        let merge_tree = tree(&w, &[(EntryKind::Blob, "s.txt", merged)]).await;
        let m = commit(&w, merge_tree, &sides, "octopus").await;

        // when: cloning the merge tip.
        let clone = w.select_commits(&[m], &[]).await.expect("select");

        // then: every commit is sent once, the merge (highest generation)
        // first and the base (lowest) last, in non-increasing generation order.
        let mut all = vec![m];
        all.extend(sides.iter().copied());
        all.push(c0);
        assert_eq!(as_set(&clone.to_send), as_set(&all));
        assert_eq!(clone.to_send.first(), Some(&m));
        assert_eq!(clone.to_send.last(), Some(&c0));
        let mut generations = Vec::new();
        for id in &clone.to_send {
            generations.push(w.get_commit_info(id).await.expect("info").generation);
        }
        assert!(
            generations.windows(2).all(|pair| pair[0] >= pair[1]),
            "commits must be in descending generation order"
        );
        assert!(clone.common.is_empty());

        // when: the client already has one side of the merge.
        let incremental = w.select_commits(&[m], &[sides[0]]).await.expect("select");

        // then: that side and the shared base drop out; the merge and the
        // other sides are all that remain.
        let mut expected = vec![m];
        expected.extend(sides.iter().skip(1).copied());
        assert_eq!(as_set(&incremental.to_send), as_set(&expected));
        assert_eq!(incremental.common, vec![sides[0]]);
    }

    #[tokio::test]
    async fn should_collect_a_wide_tree_in_one_level_order_traversal() {
        // given: a commit whose root tree fans out into many blobs plus a
        // couple of subtrees — width at each level, only a shallow depth.
        let w = walker().await;
        let names: Vec<String> = (0..40).map(|i| format!("f{i:02}.txt")).collect();
        let mut expected = Vec::new();
        let mut root_entries = Vec::new();
        for name in &names {
            let b = blob(&w, name.as_bytes()).await;
            expected.push(b);
            root_entries.push((EntryKind::Blob, name.as_str(), b));
        }
        let sub_a_leaf = blob(&w, b"a leaf").await;
        let sub_a = tree(&w, &[(EntryKind::Blob, "leaf.txt", sub_a_leaf)]).await;
        let sub_b_leaf = blob(&w, b"b leaf").await;
        let sub_b = tree(&w, &[(EntryKind::Blob, "leaf.txt", sub_b_leaf)]).await;
        expected.extend([sub_a, sub_a_leaf, sub_b, sub_b_leaf]);
        root_entries.push((EntryKind::Tree, "sub_a", sub_a));
        root_entries.push((EntryKind::Tree, "sub_b", sub_b));
        let root = tree(&w, &root_entries).await;
        let c = commit(&w, root, &[], "wide").await;
        expected.push(root);
        expected.push(c);

        // when: cloning the commit.
        let collected = w
            .collect(&[c], &[], &FilterSpec::None)
            .await
            .expect("collect");

        // then: the whole closure is collected exactly once and the commit
        // leads its objects.
        assert_eq!(collected.first(), Some(&c));
        assert_eq!(as_set(&collected), as_set(&expected));
        assert_eq!(collected.len(), expected.len());
    }
}
