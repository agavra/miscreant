# Issues / follow-ups

Running list of deferred work and known gaps discovered while building
Miscreant. See `docs/0001-init.md` for the design.

## Storage

- **SHA-256 not wired end to end.** Key/value codecs are width-generic, but
  `Store::create_repo` always stamps `object-format=sha1`, so the server only
  ever creates and serves SHA-1 repositories. Creating/advertising SHA-256
  repos is future work.

## Pack ingestion

- **In-pack REF_DELTA bases are not resolved within the received pack.**
  `gix_pack::Bundle::write_to_directory`'s thin-pack lookup consults only the
  provided object database for every REF_DELTA base, so a pack that deltas by
  object id against another object *in the same pack* is rejected unless that
  base also already exists in committed storage. Harmless for modern clients —
  we advertise `ofs-delta`, which git uses for all in-pack deltas, leaving
  REF_DELTA for genuine thin-pack (out-of-pack) bases — but a client that does
  not support `ofs-delta` could produce packs we cannot ingest.

## Receive / push

- **Ref updates enforce compare-and-swap only, not a fast-forward/force
  policy.** `git-receive-pack` applies each command as a CAS on the claimed
  old-oid (per the design), so it rejects a command only when the ref no
  longer holds the value the client sent (`ng <ref> non-fast-forward`). It
  performs no ancestry check of its own: a client that sends the current tip
  as old-oid with a new-oid that is not its descendant overwrites the ref, the
  same as a force push. The git CLI never does this unintentionally (it
  reconciles against the advertisement and refuses non-fast-forwards unless
  forced), but server-side force control belongs with the deferred
  authentication/authorization work.

## Promotion

- **Tag objects are promoted last, after commits (not between trees and
  commits).** The design's crash-safety invariant is that committed storage
  only ever holds objects whose full closure is already present. A tag can
  point at a commit newly promoted in the same push, so a tag must be written
  only after every object it references (its target commit/tree/blob and their
  closures). The promotion order is therefore blobs → trees (bottom-up) →
  commits (ancestor-first) → tags (target-first), all derived from a single
  post-order traversal. Not a gap — recorded because the ordering places tags
  later than one might expect from the design's "blobs, then trees, then
  commits" summary, which does not mention tags.

## Fetch / upload

- **`include-tag` is accepted and ignored.** The fetch command parses the
  argument but never auto-includes annotated tag objects whose targets enter
  the pack; tag objects are sent only when a tag ref itself is wanted. A
  `git clone` wants every advertised ref (tags included), so clones are
  unaffected; a bare `git fetch` that relies on tag following may not pick
  up a new tag until it asks for the tag ref explicitly.

## Performance / scale

The nonfunctional targets that matter here are set by agent workloads, not
human ones: ephemeral cold clients, high per-repo concurrency, tiny frequent
commits, branch-per-agent, and sparse access. Both read and write paths
reduce to object point-lookups against SlateDB/S3, where per-request latency
is ~10–50ms but parallelism is effectively free — so most of the work below
is about trading serial depth for width and avoiding repeated identical work.

- **Read-path traversal should batch lookups per level, not recurse
  depth-first.** The read latency floor is (serial dependency depth × RTT).
  The commit walk is bounded by generation depth and the tree walk by tree
  depth (~5–15); both should issue wide concurrent batches of SlateDB lookups
  per level rather than the recursive DFS the design pseudocode is written as.
  The concurrency lives in fan-out width, not depth.

- **Packfile / result cache keyed by `(wants, haves, filter)`.** The flagship
  agent pattern is fan-out exploration: N agents clone the same commit at
  once. v1 unpacks and re-packs identical bytes per request (packfile storage
  is deferred), so the herd pays the full pack CPU cost N times. Caching the
  packed result (or at least the collected object set) collapses this.
  Candidate to promote from deferred if concurrent-clone throughput is a
  target.

- **Have-set expansion cache keyed by commit SHA.** Already flagged as
  deferred in the design. Incremental fetch's dominant cost is expanding the
  have-frontier, recomputed identically every request; haves are almost
  always recent tips, so the cache is small and hot. Likely not actually
  deferrable for agents that sync frequently.

- **Promote-before-CAS generates garbage under push contention.** Objects are
  promoted to SlateDB before the ref CAS, so every push that loses a
  many-writers-one-branch race leaves dangling promoted objects. With GC
  deferred, sustained contention grows storage without bound. The upside is
  that a loser's objects are already present, making its retry cheap — but the
  many-writers-one-branch workload cannot be supported without a GC story.

- **Ref advertisement scales with total ref count.** Branch-per-agent
  explodes the ref table, and every discovery / `ls-refs` without a prefix
  advertises all of it. Establish a namespacing convention for agent branches
  and lean on `ref-prefix` scans so a fleet does not pay O(total refs) per
  operation.

- **Stateless server loses SlateDB's block cache on scale-up.** An autoscaled
  or redeployed server starts cold, so its first requests pay full S3 latency
  for blocks a warm instance would have cached. If server cold-start latency
  matters for the fleet, consider a shared warm cache tier or an on-disk block
  cache.

- **Optimize for time-to-first-useful-file, not just time-to-full-clone.**
  The winning agent pattern is partial (`blob:none` / `blob:limit`) plus
  sparse access, not repeated full clones. Treat filter/sparse support as a
  first-class latency path rather than a nicety.

- **"Conflicts" is a client concern; the server only does ref CAS.** Actual
  merge-conflict resolution lives in the client/agent. As a server NFR,
  "conflicts" means CAS-contention throughput and retry-loop convergence, not
  server-side merging.

## Benchmarking

- **Bench harness with agent-shaped workloads.** The SlateDB dogfood repo
  (489 blobs, 26MB) is a correctness fixture, not a load fixture. To find
  limits, drive: (a) linux/cpython-scale repos for read-path depth, (b)
  N-clone-same-commit for the thundering herd, (c) M-agents-push-main for CAS
  contention, (d) branch-explosion for ref scaling. Headline metrics: p99
  cold clone / incremental-sync latency, concurrent-clone throughput, push p99
  and CAS-retry convergence, and S3 request count per clone/push (request
  count, not bytes, is usually the bill).
