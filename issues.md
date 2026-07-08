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
