//! Fetch object discovery: decide which commits to send (Phase I) and which
//! trees/blobs/tags those commits require (Phase II).
//!
//! See `docs/0001-init.md` §command=fetch. The walk preserves that section's
//! invariants: a HAVE always dominates a WANT, a have's entire ancestry is
//! HAVE, traversal pops the highest generation first, and discovery stops as
//! soon as every want has been resolved (emitted or downgraded to HAVE).
//! Commit metadata (generation, root tree, parents) comes from the
//! commit-graph segment, which is rebuilt lazily from the objects themselves
//! whenever records are missing.

use std::collections::{BinaryHeap, HashMap, HashSet};

use gix_hash::{ObjectId, oid};
use gix_object::{CommitRef, Kind, TagRef, TreeRefIter};

use crate::storage::keys::RepoId;
use crate::storage::values::CommitGraphRecord;
use crate::storage::{Durability, ObjectDb, ObjectDbError, Store, StoreError};

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
        self.backfill_commit_info(id).await
    }

    /// Phase I: decide which commits to send. `wants` are ref tips requested
    /// by the client (commits, or annotated tags which are peeled to their
    /// target commit while the tag objects themselves are queued for
    /// sending). `haves` are commits the client claims to possess: unknown
    /// haves are ignored (standard git behavior), known ones are reported in
    /// [`Selection::common`], and every known have dominates its entire
    /// ancestry. An unknown want is an error.
    pub async fn select_commits(
        &self,
        wants: &[ObjectId],
        haves: &[ObjectId],
    ) -> Result<Selection, WalkError> {
        let mut discovery = Discovery::default();
        let mut common = Vec::new();

        for have in haves {
            if discovery.state.contains_key(have) {
                continue; // duplicate have; already counted as common
            }
            match self.get_commit_info(have).await {
                Ok(_) => {}
                // The client claimed an oid this server has never seen (or
                // that is not a commit): ignore it rather than failing the
                // fetch. Only the have itself is excused — a missing object
                // deeper in the ancestry is corruption and still errors.
                Err(WalkError::MissingObject(missing)) if missing == *have => continue,
                Err(WalkError::UnexpectedKind { oid, .. }) if oid == *have => continue,
                Err(err) => return Err(err),
            }
            common.push(*have);
            self.mark(&mut discovery, *have, Flag::Have).await?;
        }

        let (commit_wants, tags) = self.peel_wants(wants).await?;
        for want in commit_wants {
            self.mark(&mut discovery, want, Flag::Want).await?;
        }

        // Pop highest generation first; stop once no wants are pending.
        // A commit can sit in the heap more than once (re-pushed when a WANT
        // was downgraded to HAVE), so `done` guards against reprocessing.
        let mut to_send = Vec::new();
        let mut done: HashSet<ObjectId> = HashSet::new();
        while !discovery.pending.is_empty() {
            let Some(QueueEntry { oid: id, .. }) = discovery.frontier.pop() else {
                // Every id in `pending` was pushed onto the frontier when it
                // was marked WANT (see `mark`) and can only leave `pending`
                // by being popped from here — either emitted below or
                // downgraded to HAVE by a parent's `mark` call reached from
                // some other pop. So the frontier cannot run dry while
                // `pending` is non-empty, on healthy *or* corrupt
                // commit-graph data: this is believed unreachable. Treat it
                // as a hard error instead of silently truncating the pack,
                // in case that invariant is ever wrong.
                return Err(WalkError::DiscoveryStalled {
                    unresolved: discovery.pending.iter().copied().collect(),
                });
            };
            if !done.insert(id) {
                continue;
            }
            let Some(flag) = discovery.state.get(&id).copied() else {
                continue; // unreachable: everything in the heap was marked
            };
            if flag == Flag::Want {
                to_send.push(id);
                discovery.pending.remove(&id);
            }
            let info = self.get_commit_info(&id).await?;
            for parent in info.parents {
                self.mark(&mut discovery, parent, flag).await?;
            }
        }

        to_send.extend(tags);
        Ok(Selection { to_send, common })
    }

    /// Phase II: expand the commits chosen by [`Walker::select_commits`]
    /// into the full set of objects to pack, skipping everything already
    /// reachable from the client's haves. Returns a deduplicated list in
    /// insertion order: each commit followed by its new trees and blobs,
    /// with wanted tag objects wherever they appear in `to_send`.
    ///
    /// The have-side expansion visits every tree/blob under each have's root
    /// tree once (the have-set doubles as its own seen-set); the want-side
    /// walk prunes an entire subtree at the first tree the client already
    /// has and adds blobs only when they are absent from the have-set, so an
    /// incremental fetch is proportional to the diff, not the snapshot.
    pub async fn collect(
        &self,
        to_send: &[ObjectId],
        haves: &[ObjectId],
    ) -> Result<Vec<ObjectId>, WalkError> {
        let mut have_objects: HashSet<ObjectId> = HashSet::new();
        for have in haves {
            let info = match self.get_commit_info(have).await {
                Ok(info) => info,
                // Mirror discovery: an unknown (or non-commit) have is
                // ignored; deeper misses are corruption and still error.
                Err(WalkError::MissingObject(missing)) if missing == *have => continue,
                Err(WalkError::UnexpectedKind { oid, .. }) if oid == *have => continue,
                Err(err) => return Err(err),
            };
            self.expand_haves(info.root_tree, &mut have_objects).await?;
        }

        let mut objects = OrderedOidSet::default();
        for id in to_send {
            let (kind, _) = self
                .objects
                .size(self.repo, id)
                .await?
                .ok_or(WalkError::MissingObject(*id))?;
            match kind {
                Kind::Commit => {
                    objects.insert(*id);
                    let info = self.get_commit_info(id).await?;
                    self.walk_tree(info.root_tree, &mut objects, &have_objects)
                        .await?;
                }
                Kind::Tag => objects.insert(*id),
                actual => {
                    return Err(WalkError::UnexpectedKind {
                        oid: *id,
                        expected: Kind::Commit,
                        actual,
                    });
                }
            }
        }
        Ok(objects.into_vec())
    }

    /// Mark `id` with `flag`, updating `pending` and pushing the commit onto
    /// the frontier when its state changed: a new WANT joins `pending`, a
    /// WANT downgraded by a HAVE leaves it, and an established HAVE absorbs
    /// everything. Both `pending` operations are idempotent, so re-marking
    /// an id that already left (or never entered) the relevant state is
    /// always safe.
    async fn mark(
        &self,
        discovery: &mut Discovery,
        id: ObjectId,
        flag: Flag,
    ) -> Result<(), WalkError> {
        let resolved = match (discovery.state.get(&id), flag) {
            (Some(Flag::Have), _) | (Some(Flag::Want), Flag::Want) => return Ok(()),
            (Some(Flag::Want), Flag::Have) => {
                discovery.pending.remove(&id);
                Flag::Have
            }
            (None, Flag::Have) => Flag::Have,
            (None, Flag::Want) => {
                discovery.pending.insert(id);
                Flag::Want
            }
        };
        discovery.state.insert(id, resolved);
        let generation = self.get_commit_info(&id).await?.generation;
        discovery.frontier.push(QueueEntry {
            generation,
            oid: id,
        });
        Ok(())
    }

    /// Resolve raw wants into commit wants plus the annotated tag objects to
    /// send. Tag chains are chased tag-by-tag (each tag object in the chain
    /// is sent) until a commit is reached; any other target kind is
    /// unsupported.
    async fn peel_wants(
        &self,
        wants: &[ObjectId],
    ) -> Result<(Vec<ObjectId>, Vec<ObjectId>), WalkError> {
        let mut commit_wants = Vec::new();
        let mut tags = OrderedOidSet::default();
        for want in wants {
            let mut cursor = *want;
            loop {
                let Some((kind, body)) = self.objects.get(self.repo, &cursor).await? else {
                    return Err(if cursor == *want {
                        WalkError::UnknownWant(*want)
                    } else {
                        // A tag in the chain references a missing object:
                        // server-side corruption, not a client mistake.
                        WalkError::MissingObject(cursor)
                    });
                };
                match kind {
                    Kind::Commit => {
                        commit_wants.push(cursor);
                        break;
                    }
                    Kind::Tag => {
                        tags.insert(cursor);
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
        Ok((commit_wants, tags.into_vec()))
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

    /// Insert every tree and blob reachable from `root_tree` into
    /// `have_objects`. The set doubles as the seen-set, so a subtree shared
    /// between several haves is visited once and the total cost is bounded
    /// by the number of distinct objects reachable from the haves.
    async fn expand_haves(
        &self,
        root_tree: ObjectId,
        have_objects: &mut HashSet<ObjectId>,
    ) -> Result<(), WalkError> {
        let mut stack = vec![root_tree];
        while let Some(tree_id) = stack.pop() {
            if !have_objects.insert(tree_id) {
                continue;
            }
            for entry in self.read_tree(&tree_id).await? {
                match entry {
                    TreeEntry::Subtree(child) => {
                        if !have_objects.contains(&child) {
                            stack.push(child);
                        }
                    }
                    TreeEntry::Leaf(child) => {
                        have_objects.insert(child);
                    }
                }
            }
        }
        Ok(())
    }

    /// Collect the trees and blobs under `root_tree` into `objects`,
    /// skipping anything the client already has. Content addressing means an
    /// identical tree id implies identical content all the way down, so the
    /// walk prunes a whole subtree at the first tree found in `have_objects`
    /// (or already collected — `objects` memoizes trees shared between the
    /// commits being sent).
    async fn walk_tree(
        &self,
        root_tree: ObjectId,
        objects: &mut OrderedOidSet,
        have_objects: &HashSet<ObjectId>,
    ) -> Result<(), WalkError> {
        let mut stack = vec![root_tree];
        while let Some(tree_id) = stack.pop() {
            if objects.contains(&tree_id) || have_objects.contains(&tree_id) {
                continue;
            }
            objects.insert(tree_id);
            let mut subtrees = Vec::new();
            for entry in self.read_tree(&tree_id).await? {
                match entry {
                    TreeEntry::Subtree(child) => subtrees.push(child),
                    // The sole blob admission point: a blob is sent only
                    // when the client's have-frontier lacks it.
                    TreeEntry::Leaf(child) => {
                        if !have_objects.contains(&child) {
                            objects.insert(child);
                        }
                    }
                }
            }
            // Depth-first in entry order: push in reverse so the first
            // subtree is processed first.
            for child in subtrees.into_iter().rev() {
                stack.push(child);
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

    /// A walker over a fresh `memory://` store with one repository.
    async fn walker() -> Walker {
        let store = Store::open("memory://").await.expect("open store");
        let repo = store.create_repo("walk").await.expect("create repo").id;
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let blobs = BlobStore::new(backing);
        let objects = ObjectDb::new(store.clone(), blobs, 65536);
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
        let collected = w.collect(&selection.to_send, &[]).await.expect("collect");

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
        let collected = w.collect(&[c2, c1], &[]).await.expect("collect");

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
            .collect(&selection.to_send, &selection.common)
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
        let collected = w.collect(&[c2], &[c1]).await.expect("collect");

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
}
