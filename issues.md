# Issues / follow-ups

Running list of deferred work and known gaps discovered while building
Miscreant. See `docs/0001-init.md` for the design.

## Storage

- **Logical segments only.** SlateDB has no public physical-segment API yet
  (segment-oriented compaction, RFC 0024). The design's four "segments"
  (metadata, object, ref, commit-graph) are realized logically via the leading
  segment byte in every key, sharing one LSM tree. Adopt physical segments
  once SlateDB exposes them.
- **SHA-256 not wired end to end.** Key/value codecs are width-generic, but
  `Store::create_repo` always stamps `object-format=sha1`, so the server only
  ever creates and serves SHA-1 repositories. Creating/advertising SHA-256
  repos is future work.
- **`file://./…` storage URLs do not parse.** `object_store::parse_url`
  only accepts the `file` scheme with no host, so the current default
  (`file://./miscreant-data`, host `"."`) is rejected. Tests use `memory://`
  (host is empty, so it parses). Before the store is wired into the running
  server, normalize relative `file` URLs (or switch the default to an absolute
  `file:///…` path).
