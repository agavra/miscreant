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
