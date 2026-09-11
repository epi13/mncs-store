# P2-004 — Append-only host_write cannot express store file semantics

Narrows P1-001 (partially resolved 2026-09-10): the write half that
`host_write` does NOT cover, with Phase-2 evidence for each missing
shape.

Storage feature being implemented: chunk persistence (create-exclusive),
generation staging and publication, snapshot pins, reclamation deletes,
serial-counter durability (RFC 0005 commit protocol, RFC 0015 recovery).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13): `host_write(view)` appends at most 64 bytes per
call to the granted path, creating it when absent, returning the count.
Verified working on research-bytecode (returns 4+4, file holds 8 bytes,
effect event carries grant path + digest). There is no:

- create-exclusive (chunk files need same-bytes-idempotent /
  different-bytes-fatal; append cannot refuse a second writer);
- overwrite/truncate (generation `current` pointer moves need exact
  8-byte replacement; append-only would grow it unboundedly);
- read-back (staging must be re-read for verification);
- per-call sizes above 64 bytes (a 1020-byte manifest needs 16 appends
  with no atomicity across them).

Minimal reproducer: express "create this chunk file, fail if different
bytes exist" or "replace these 8 current-pointer bytes atomically" with
`host_write` — the first silently unions writers, the second is
inexpressible (append has no position).

Required semantics: a bounded file effect family with positioned and
exclusive variants (`write_at`, `create_exclusive`, `truncate`,
`delete`) behind a namespace capability, each returning a rich status
(written/denied/exists/mismatch/unsupported) rather than trapping.

Why the current behavior/API is insufficient: the store's two strongest
file-level invariants — committed-chunk immutability and atomic current
publication — are enforced by host `O_EXCL` + `os.replace`, unaudited by
the compiler. `host_write` staging logs would still need an atomic
link step to become authoritative (invariant 10).

Safety/correctness implications: HIGH (same as P1-001: the commit verbs
live outside the language).

Performance implications: 64 B/call forces 16+ invocations per large
manifest with no batching across the append boundary.

Workaround used: host `_write_create_exclusive` + `_write_sync`
(tmp + fsync + replace) in the driver, each labeled with the missing
capability. Semantically a bootstrap substitute (authority outside MNCS);
byte-identical to what the language effects must produce when they land
(file formats stay MNCS-owned).

What the language/stdlib/runtime should ideally provide: positioned and
exclusive file effects composed with the fs_* grant model (root-scoped
capabilities, bounded path arguments), plus the sync barrier (P1-002)
and atomic rename (P2-005) in the same tranche — one without the others
still strands the commit protocol.

Affected backend(s): all for the missing shapes; `host_write` itself is
bytecode-only (P1-B02).

Severity: major (blocker for in-language lifecycle).

Status: workaround (host file mechanics; semantics in-language)

Acceptance test: an MNCS program create-exclusively writes two chunks
(same bytes -> idempotent count, different bytes -> mismatch status),
replaces an 8-byte pointer, and lists the results via fs_* — with the
driver asserting byte-identical files to today's host path.
