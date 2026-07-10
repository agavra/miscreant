# 0002 - Oracle Testing

- **Status:** Draft
- **Author(s):** Almog Gavra
- **Created:** 2026-07-10
- **Updated:** 2026-07-10

## Summary

This document covers the core correctness testing mechanism for miscreant's
git server: invariant checks run with the `git` CLI's plumbing, plus a
differential oracle that compares against `git http-backend`, which is assumed
correct.

## Motivation / Goals

Miscreant reimplements a large slice of the git wire protocol against object
storage rather than shelling out to canonical git. Every deviation from what
the real `git` client expects is a bug that surfaces as a failed clone, a
corrupt checkout, or a rejected push.

We want a testing strategy that:

- exercises the full protocol against a real `git` client
- catches divergence from canonical git without re-deriving the protocol spec
- makes certain our non-supported features fail gracefully

## Non-Goals

- byte-for-byte wire equality with canonical git (test only *client-observable*
  behavior)
- porting git's `t/t55xx` shell suite wholesale (see [Mining git's own
  suite](#mining-gits-own-test-suite))
- testing `git://` or SSH protocols

## Background

The authoritative definition of correct server behavior is (a) the git protocol
documents and (b) git's own test suite. The suite's HTTP tests
(`t5551-http-fetch-smart`, `t5541-http-push-smart`, `t5702-protocol-v2`, …) are
the reference, but `t/lib-httpd.sh` always starts its own Apache instance
running `git-http-backend`. Since there is no knob to aim those tests at an
external server we instead reimplement a similar test suite using the `git
http-backend` as the reference oracle.

## Design

The existing harness in `tests/common/mod.rs` is a good foundation to develop
from since it already runs a real world git workload. The test suite is broken
down into two components: the invariant tests and differential oracle tests.

1. The invariant oracle uses the `git` CLI's internal plumbing to confirm that
   invariants are always well formed.
2. The differential oracle spins up a real `git` server as an oracle and
   compares the result of a workload (modeled after the `t/t55xx` tests)
   between the oracle and the miscreant server.


### The invariant oracles

The test will confirm that every repository on every operation meets the
following invariants (confirmed with `git <command>`):

| Invariant | Check |
|---|---|
| Clone is a well-formed object graph | `git fsck --full --strict` on the clone exits 0 with no dangling/missing |
| Emitted pack is valid | pipe the fetch pack through `git index-pack --strict --stdin -o <idx>` |
| Clone reproduces the source tip | `git rev-parse HEAD` on the clone == the pushed commit id |
| Ref advertisement is exactly the refs that exist | `git ls-remote` output == the known ref set |
| Object set is reachability-closed | `git rev-list --objects --all` on the clone succeeds (no missing referents) |
| Working tree matches | checkout and compare file contents / `git status` clean |

> For partial clones (`blob:none`/`blob:limit`) the reachability and
> working-tree checks run with `--missing=allow-promisor`

### The differential oracle

For this test we will spin up the oracle with `git http-backend`. Commands
run against it and miscreant are identical, asserting only client-observable
behavior outcomes match:

- exit status
- `ls-remote` output
- `fsck`-clean clone's object set
- `rev-list --count --objects --all` match
- resulting refs are identical

Order-sensitive outputs (`ls-remote`, `rev-list --objects`) are compared as
sorted sets since ordering is not guarnateed across implementations.

```ascii-art
                         ┌──────────────────────┐
                         │      miscreant       │█
                    ┌───▶│     (TestServer)     │█
                    │    └──────────────────────┘█
                    │     ███████████▲████████████
┌───────────────┐   │                │
│               │   │                │
│ git <command> │───┤             compare
│               │   │             results
└───────────────┘   │                │
                    │                ▼
                    │    ┌──────────────────────┐
                    │    │   git http-backend   │█
                    └───▶│  (reference oracle)  │█
                         └──────────────────────┘█
                          ████████████████████████
```

Any divergence from git http-backend is a Miscreant bug. For implementation,
since `git http-backend` is a Common Gateway Interface (CGI) we can just spin
it up for each request and write via stdin through a minor rust shim to drive
the shell via env variables. The shim must forward the `Git-Protocol` header
as `GIT_PROTOCOL` (otherwise v2 silently degrades to v0), set
`GIT_HTTP_EXPORT_ALL=1`, and enable `http.receivepack=true` for pushes.

### Fetch matrix (differential + invariant)

Each row runs under protocol v0 and v2, against both servers, with the
invariant checks applied to every clone (the harness must set
`protocol.version` per row — `git_command` currently pins it to v2):

| Scenario | Notes |
|---|---|
| Full clone, single branch | baseline |
| Full clone, many refs + annotated/lightweight tags | tag peeling, ref advertisement |
| Incremental fetch (fast-forward) | have/want negotiation, thin diff |
| Fetch after force-update (non-ff) | negotiation with divergent tips |
| Clone of empty repo (unborn HEAD) | `ls-refs` unborn, empty pack |
| Partial clone `--filter=blob:none` + later blob fault-in | supported filter |
| Partial clone `--filter=blob:limit=<n>` | supported filter |
| Clone with `ofs-delta` negotiated vs not | send-side delta planner |
| Clone git's own repository (gated) | see [Dogfood](#dogfood-the-git-repository) |

### Push matrix (differential + invariant)

Push is the `receive-pack` v0 protocol only. We check the following:

| Scenario | Notes |
|---|---|
| Push new repo | receive-pack create path |
| Incremental push (fast-forward) | thin pack against server-held bases |
| Delta-heavy push (amend/rebase of a large blob) | receive-pack must resolve a thin pack (client `REF_DELTA` against server bases); verify with `index-pack --strict` |
| Non-fast-forward rejected, then `--force` | CAS ref update, `ng`/`ok` report-status |
| Branch delete (push empty source) | ref deletion |
| Concurrent push to the same ref | invariant-only (not differential): exactly one wins, loser sees a clean `ng`; use a barrier to force the race deterministically |
| Push then clone round-trip | full-cycle integrity: clone fscks clean and reproduces the pushed tree |

### Verify unsupported behavior

For unsupported behaviors, we just want to ensure that the failure modes are
clean and don't fail either the client or the server with unexpected failure
modes:

| Client action | Expected client-observable outcome |
|---|---|
| `git clone --depth=1` | fails cleanly |
| `git clone --filter=tree:0` | rejected |
| `git clone --filter=sparse:oid=<oid>` | rejected |
| fetch naming an unknown object-format (SHA-256) | rejected (explicit policy — storage is width-aware, so confirm the path rejects rather than half-accepts) |
| `want-ref` request | not advertised |
| unknown v2 command | `ERR` pkt-line |

### Dogfood: the git repository

In addition to creating targeted tests for edge cases and standard behavior,
we will dogfood miscreant against an established repository with a complicated
history, `git` itself (https://github.com/git/git). This should help root out
any subtle pack/delta/negotiation bugs that are lurking about.

A gated (non-default, opt-in) test clones a pinned commit of `git/git` from a
local mirror, pushes it into Miscreant, clones it back, and asserts an
`fsck`-clean, tree-identical result under both protocol versions. It is gated
because at ~300 MB it runs in tens of seconds which is suitable for gating
commits but would slow down local development sufficiently to make it worth
disabling.

## Mining git's own test suite

Rather than porting, we read `t/t5551-http-fetch-smart.sh`,
`t/t5541-http-push-smart.sh`, `t/t5601-clone.sh`, `t/t5616-partial-clone.sh`,
and `t/t5702-protocol-v2.sh` for cases worth reproducing in our harness,
especially their edge cases around empty repos, symref advertisement, tag
peeling, and report-status formatting. Their negative tests
(`t/t5703-upload-pack-ref-in-want.sh`, shallow cases) map onto our
[Verify unsupported behavior](#verify-unsupported-behavior) matrix as
rejection assertions.

## References

- [0001 - Miscreant](0001-init.md)
- [gitprotocol-http(5)](https://www.kernel.org/pub/software/scm/git/docs/gitprotocol-http.html)
- [git protocol-v2](https://git-scm.com/docs/protocol-v2)
- [git-http-backend](https://git-scm.com/docs/git-http-backend)
- [git t/t5551-http-fetch-smart.sh](https://github.com/git/git/blob/master/t/t5551-http-fetch-smart.sh)
- [git t/lib-httpd.sh](https://github.com/git/git/blob/master/t/lib-httpd.sh)
