//! Connectivity validation and crash-safe promotion of a staged pack into
//! committed storage.
//!
//! See `docs/0001-init.md` §Receive API (validation + promotion paragraphs)
//! and §Commit Graph Storage. A push is only promoted once every object
//! reachable from its ref tips resolves against either the received pack or
//! committed storage; promotion then writes objects children-before-parents
//! so committed storage only ever holds objects whose full closure is already
//! present, even if the server dies mid-promotion.

use std::collections::HashSet;

use bytes::Bytes;
use gix_hash::{ObjectId, oid};
use gix_object::{CommitRef, Kind, TagRef, TreeRefIter};

use crate::git::ingest::{IngestError, StagedPack};
use crate::git::walk::{WalkError, Walker};
use crate::storage::keys::RepoId;
use crate::storage::values::CommitGraphRecord;
use crate::storage::{Durability, ObjectDb, ObjectDbError, Store, StoreError};

/// Errors returned by [`validate_and_promote`].
#[derive(Debug, thiserror::Error)]
pub enum PromoteError {
    /// A SlateDB-backed store operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// An object database operation failed.
    #[error(transparent)]
    Objects(#[from] ObjectDbError),
    /// Reading an object out of the staged pack failed.
    #[error(transparent)]
    Staged(#[from] IngestError),
    /// Backfilling a commit-graph record during commit-graph extension failed.
    #[error(transparent)]
    Walk(#[from] WalkError),
    /// A staged object body could not be parsed while enumerating its
    /// children (indicates a malformed object that nonetheless verified its
    /// SHA — i.e. storage/pack corruption).
    #[error("object {oid} could not be decoded")]
    Decode {
        /// The object whose body failed to parse.
        oid: ObjectId,
        /// The underlying parse failure.
        #[source]
        source: gix_object::decode::Error,
    },
    /// An object reachable from a push tip resolves against neither the
    /// received pack nor committed storage: the push is not self-contained
    /// and is rejected before anything is written.
    #[error(
        "object {missing} is referenced but absent from the received pack and committed storage (referenced by {referenced_by:?})"
    )]
    Connectivity {
        /// The object that could not be found.
        missing: ObjectId,
        /// The object that referenced it, or `None` when the missing object
        /// is itself a push tip.
        referenced_by: Option<ObjectId>,
    },
}

/// Summary of what a push added to committed storage, for the receive-pack
/// endpoint to report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Promotion {
    /// Objects newly written to committed storage, in the order they were
    /// written: blobs, then trees bottom-up, then commits ancestor-first,
    /// then tags. Objects already present were pruned and never appear here.
    pub promoted: Vec<ObjectId>,
    /// The commits among `promoted`, ancestor-before-descendant; each received
    /// a freshly written commit-graph record.
    pub commits: Vec<ObjectId>,
}

/// One frame of the explicit-stack traversal that both validates connectivity
/// and derives the promotion order.
enum Step {
    /// First visit of `oid`, reached along an edge from `referenced_by`
    /// (`None` for a push tip). Prunes objects already committed, otherwise
    /// reads the staged body and schedules its children followed by an
    /// [`Step::Emit`].
    Enter {
        oid: ObjectId,
        referenced_by: Option<ObjectId>,
    },
    /// Every child of `oid` has already been emitted; append it to the
    /// post-order (children-before-parents) list.
    Emit {
        oid: ObjectId,
        kind: Kind,
        body: Bytes,
    },
}

/// Validate that the full object closure reachable from `tips` (the non-delete
/// `new-oid`s of a push) is present in `staged` or already committed, then
/// promote the new objects into committed storage and extend the commit graph.
///
/// Traversal is an iterative DFS from each tip over an explicit stack: an
/// object already in committed storage is pruned (its closure is guaranteed
/// present by this very invariant), otherwise it must come from `staged` or
/// the push is rejected with [`PromoteError::Connectivity`] before any write.
/// The DFS records objects in post-order, so children are always promoted
/// before parents; grouping that order by kind yields the crash-safe write
/// sequence blobs → trees → commits → tags.
pub async fn validate_and_promote(
    staged: &StagedPack,
    tips: &[ObjectId],
    objectdb: &ObjectDb,
    store: &Store,
    repo: RepoId,
) -> Result<Promotion, PromoteError> {
    // Phase 1: validate connectivity and derive a children-before-parents
    // ordering. Nothing is written here, so a rejection leaves storage
    // untouched.
    let post_order = validate(staged, tips, objectdb, repo).await?;

    // Group the post-order by kind. Each group keeps its post-order relative
    // ordering, so trees stay bottom-up, commits stay ancestor-first, and
    // tags stay target-first.
    let mut blobs = Vec::new();
    let mut trees = Vec::new();
    let mut commits = Vec::new();
    let mut tags = Vec::new();
    for (oid, kind, body) in post_order {
        match kind {
            Kind::Blob => blobs.push((oid, body)),
            Kind::Tree => trees.push((oid, body)),
            Kind::Commit => commits.push((oid, body)),
            Kind::Tag => tags.push((oid, body)),
        }
    }
    let (blob_count, tree_count, tag_count) = (blobs.len(), trees.len(), tags.len());

    // Phase 2: promote objects children-before-parents. Blobs depend on
    // nothing, trees on blobs and subtrees, commits on their root tree and
    // parent commits, and tags on their (already-written) target. Tags come
    // last because a tag may point at a commit promoted in this same batch;
    // writing it earlier could leave committed storage holding a tag whose
    // target is absent if the server died mid-promotion.
    //
    // Writes are relaxed-durability: each `put` returns once the record
    // lands in SlateDB's in-memory WAL buffer, without waiting for the WAL
    // flush that makes it durable. The flush barrier at the end of this
    // function (see its comment) is what makes the batch as a whole safe.
    let mut promotion = Promotion::default();
    for (oid, body) in blobs {
        objectdb
            .put(repo, &oid, Kind::Blob, body, Durability::Relaxed)
            .await?;
        promotion.promoted.push(oid);
    }
    for (oid, body) in trees {
        objectdb
            .put(repo, &oid, Kind::Tree, body, Durability::Relaxed)
            .await?;
        promotion.promoted.push(oid);
    }
    for (oid, body) in &commits {
        objectdb
            .put(repo, oid, Kind::Commit, body.clone(), Durability::Relaxed)
            .await?;
        promotion.promoted.push(*oid);
    }
    for (oid, body) in tags {
        objectdb
            .put(repo, &oid, Kind::Tag, body, Durability::Relaxed)
            .await?;
        promotion.promoted.push(oid);
    }

    // Phase 3: extend the commit graph. Commits are ancestor-first, so every
    // parent that is itself new already has a record by the time its child is
    // processed; parents from prior pushes are resolved (and backfilled if
    // needed) through the shared walker.
    let walker = Walker::new(store.clone(), objectdb.clone(), repo);
    for (oid, body) in &commits {
        let commit =
            CommitRef::from_bytes(body, oid.kind()).map_err(|source| decode_error(oid, source))?;
        let root_tree = commit.tree();
        let parents: Vec<ObjectId> = commit.parents().collect();

        let mut max_parent_generation = 0;
        for parent in &parents {
            let generation = walker.get_commit_info(parent).await?.generation;
            max_parent_generation = max_parent_generation.max(generation);
        }

        // `1 + max(parent generations)`, which is `1` for a root (no parents,
        // so the max stays 0).
        let record = CommitGraphRecord {
            generation: max_parent_generation + 1,
            root_tree,
            parents,
        };
        store
            .put_commit_graph(repo, oid, &record, Durability::Relaxed)
            .await?;
        promotion.commits.push(*oid);
    }

    // Barrier: flush the WAL so every relaxed write issued above is durable
    // before this function returns, and any write failure surfaces in this
    // push request rather than a later, unrelated one.
    //
    // This is safe because SlateDB writes become durable in submission
    // order: the WAL is flushed by a single sequential task and replayed
    // strictly in contiguous wal-id order, each WAL SST landing via one
    // atomic object-store PUT. This function submits children before
    // parents (blobs, then trees, then commits, then tags — see Phase 2
    // above), so any durable prefix of the WAL is a set of full closures;
    // and the ref CAS that follows promotion (a separate, awaited-durable
    // transaction) is the last write in the sequence, so a recovered state
    // that contains a ref necessarily contains that ref's entire closure.
    store.flush().await?;

    tracing::debug!(
        blobs = blob_count,
        trees = tree_count,
        commits = commits.len(),
        tags = tag_count,
        graph_records = promotion.commits.len(),
        "promotion complete"
    );

    Ok(promotion)
}

/// Traverse the closure of `tips`, verifying every object is either committed
/// or staged and returning the new (uncommitted) objects in post-order —
/// children before parents.
async fn validate(
    staged: &StagedPack,
    tips: &[ObjectId],
    objectdb: &ObjectDb,
    repo: RepoId,
) -> Result<Vec<(ObjectId, Kind, Bytes)>, PromoteError> {
    // Tips are pushed in reverse so the first tip is explored first.
    let mut stack: Vec<Step> = tips
        .iter()
        .rev()
        .map(|tip| Step::Enter {
            oid: *tip,
            referenced_by: None,
        })
        .collect();
    // Objects whose visit is settled: either pruned (already committed) or
    // scheduled for emission. Guards against re-descending shared subgraphs.
    let mut done: HashSet<ObjectId> = HashSet::new();
    let mut post_order: Vec<(ObjectId, Kind, Bytes)> = Vec::new();

    while let Some(step) = stack.pop() {
        match step {
            Step::Enter { oid, referenced_by } => {
                if !done.insert(oid) {
                    continue;
                }
                // An object already in committed storage anchors the closure:
                // by the promotion invariant its own closure is present, so we
                // stop here without descending.
                if objectdb.exists(repo, &oid).await? {
                    continue;
                }
                let Some((kind, body)) = staged.read(&oid)? else {
                    return Err(PromoteError::Connectivity {
                        missing: oid,
                        referenced_by,
                    });
                };
                let children = object_children(&oid, kind, &body)?;
                // Emit sits below the children on the stack, so it runs only
                // after every child has been emitted.
                stack.push(Step::Emit { oid, kind, body });
                for child in children.into_iter().rev() {
                    stack.push(Step::Enter {
                        oid: child,
                        referenced_by: Some(oid),
                    });
                }
            }
            Step::Emit { oid, kind, body } => post_order.push((oid, kind, body)),
        }
    }

    Ok(post_order)
}

/// The objects directly referenced by `body` (an object of `kind` hashing to
/// `id`), which must be present for the closure to be complete: a commit's
/// root tree and parents, a tree's non-submodule entries, or a tag's target.
/// Blobs reference nothing. Submodule (gitlink) tree entries are skipped —
/// their targets live in another repository.
fn object_children(id: &oid, kind: Kind, body: &Bytes) -> Result<Vec<ObjectId>, PromoteError> {
    match kind {
        Kind::Blob => Ok(Vec::new()),
        Kind::Tree => {
            let mut children = Vec::new();
            for entry in TreeRefIter::from_bytes(body, id.kind()) {
                let entry = entry.map_err(|source| decode_error(id, source))?;
                if entry.mode.is_commit() {
                    continue;
                }
                children.push(entry.oid.to_owned());
            }
            Ok(children)
        }
        Kind::Commit => {
            let commit = CommitRef::from_bytes(body, id.kind())
                .map_err(|source| decode_error(id, source))?;
            let mut children = vec![commit.tree()];
            children.extend(commit.parents());
            Ok(children)
        }
        Kind::Tag => {
            let tag =
                TagRef::from_bytes(body, id.kind()).map_err(|source| decode_error(id, source))?;
            Ok(vec![tag.target()])
        }
    }
}

/// Build a [`PromoteError::Decode`] for `id` from a gix parse failure.
fn decode_error(id: &oid, source: gix_object::decode::Error) -> PromoteError {
    PromoteError::Decode {
        oid: id.to_owned(),
        source,
    }
}
