# RFC 0017 — Multi-chunk objects, generations commit protocol, and recovery decisions (Phase 2)

**Status:** Accepted for implementation (Phase 2 scope)
**Amends:** concretizes RFC 0004 (chunking strategy), RFC 0005 (commit
protocol states), RFC 0015 (recovery classification); extends RFC 0016
(v1 encodings frozen, v2 added alongside)
**Version bounds:** manifest v2, descriptor v2, single-chunk v1 frozen

## Problem

RFC 0016 deferred multi-chunk objects (`chunk_count != 1` rejected),
generation mechanics beyond full-snapshot files, and recovery decisions.
Source Profile 0.13 raises sequences/views and iterations to 1024 but
executors still realize single-shot digests over at most 64 bytes, so an
arbitrary-length hash remains inexpressible. Phase 2 needs a bounded but
genuine multi-chunk path, an explicit commit protocol, and deterministic
recovery — all implementable under the 64-byte digest realization bound.

## Decision

### Descriptor v2

Identical 16-byte layout to v1 with version byte 2. The sole semantic
change is the BYTE_SEQ row: dim 0..992 (31 chunks x 32 bytes), len ==
dim. Scalar and pair rows are unchanged. v1 validation still rejects dim
> 32; v2 validation still rejects version != 2. Neither generation admits
the other's descriptors (invariant 8).

### Chunk partition and canonical tail padding

A blob of true length L (0..992) partitions into floor(L/32) full 32-byte
body chunks plus one tail of R = L % 32 bytes (no tail chunk when R ==
0). The tail is zero-extended to the next covered frame width W in
{4, 8, 16, 32} so every frame reuses the frozen v1 widths and their
single-shot identities. The extension is canonical: `tail_okW` predicates
require every byte at index >= R to be zero (checked on put AND on every
read), and the manifest total_len binds the true length; readers strip
exactly to it. An all-zero tail of length 0 commits one empty-frame
chunk so the chain below is never vacuous.

`tail_width_for` selects the least covered width (0 outside 1..32: no
silent choice). The host mirror of this table is orchestration only; the
MNCS predicate is the authority and tests pin both to the same table.

### Manifest v2 and the hash-chained root

A v2 manifest file is 28 + 32*N bytes (N = 1..31):

```text
0   2  magic "MN" (0x4D 0x4E)
2   1  version (2)
3   1  type_tag (3 = BYTE_SEQ; v2 is blob-only)
4   2  chunk_count u16 BE
6   16 descriptor v2 bytes
22  4  total_len u32 BE
26  2  reserved (0,0)
28  N*32 chunk ContentIds in commit order
```

Root identity is a domain-separated Merkle chain with every step hashing
at most 64 bytes:

```text
H0 = SHA-256(header[0..28])
H_{i+1} = SHA-256(H_i || chunk_digest_i)
RootId = H_N
```

The chain binds the header (count, total_len, type, descriptor) and the
complete ordered chunk sequence. Reordered, missing, or substituted
chunks change the root; chunk presence alone never implies commitment
(invariant 10). The chain needs exactly one bulk constructor
(`concat32_32`); per-step batching across items keeps the invocation
count at 1 + maxdepth per put plan.

`validate_v2` codes: 0 OK, 1 BAD_VIEW_LEN, 2 BAD_MAGIC, 3 BAD_VERSION,
4 BAD_TYPE, 5 BAD_COUNT, 6 BAD_RESERVED, 7 BAD_DESCRIPTOR,
8 COUNT_MISMATCH, 9 DESC_LEN_MISMATCH, 10 TOTAL_OVER_CEILING. Counts
above 31 are unrepresentable through the 1024-byte view bound (noted,
not silently accepted).

### Generations, CAS, snapshots

Generation files keep the Phase-1a layout (8-byte `MG|1|0|0|count` header
+ 44-byte records); header assembly moves in-language
(`store.generation.header`, records via `store.manifest.gen_record`).
Compare-and-transition: `cas_decide(current, expected)` returns 0 COMMIT
iff equal, else 1 CONFLICT; stale writers receive a `conflict_token`
(observed + attempted generations) and the store never retries silently.
Snapshot tokens (`SN|1|flags|gen|reserved`) bind readers to one
generation; `snap_permits` is the single binding authority, so concurrent
publication cannot mix generations into a bound read (invariant 9).

Commit states: 0 PREPARING, 1 CONTENT_READY, 2 CANDIDATE,
3 DURABLE_PREREQ, 4 PUBLISHED (terminal), 5 ABORTED (terminal).
`commit_next(state, step_ok)` is the single transition authority: legal
steps advance one state, failures abort, terminal states stick.

### Recovery

`classify_store` triages open (healthy / no-store / no-current /
dangling-current). `classify_generation` triages one generation file
(committed-valid / incomplete / missing-chunk / corrupt) from MNCS
validator outputs. `recover_decide(prev_valid, cand_class)` selects
STAY / PROMOTE / REFUSE: only a fully valid candidate promotes, and only
over a checked previous generation (invariant 12). `prune_decide`
gates staged-byte reclamation on retained references.

### Reclamation

`table_contains(table1024, entries, target)` decides retained-digest
membership in-language over a host-assembled digest table (zero-padded
transport, masked by the entries bound). Callers union the chunk digests
named by freshly MNCS-verified retained manifests (live generations,
live snapshot pins, retention policy) before asking; window OR-ing
across 32-entry windows is boolean transport over MNCS verdicts. Shared
chunks survive while any retained manifest names them; unpinned
generation files and unreferenced staged bytes are pruned.

## Bounds (explicit, not silent)

- Objects: 0..992 bytes (31 x 32). Longer values are rejected, never
  truncated (P2 pressure: digest realization bound, view bound).
- Manifest views: <= 1024 bytes; generation files validate in-language
  to 23 records (wider files keep host length-gate parity + full
  per-object MNCS verification).
- Reclamation tables: 32 roots per scan call (windowed by the caller).
- Reclamation scans measure ~34k steps (explicit scan budget).

## Invariants (restated for v2)

- v1 bytes are frozen; v2 readers fail closed on v1-or-unknown versions
  and vice versa.
- Canonical bytes and ALL commit/recovery/reclamation decisions come
  from `src/store/*.mncs` executions; the host transports and performs
  only the filesystem mutations the language cannot name.
- JSON remains harness transport, never canonical state (invariant 23).

## Rejected alternatives

- Truncated-digest multi-chunk roots (weakens content addressing).
- Ad-hoc `sha256(sha256(p1) || p2)` without domain separation (the v2
  chain separates header-binding from step-binding by construction).
- Non-zero or unvalidated tail padding (smuggling vector; refused).
- Host-decided generation winner (makes Python the recovery authority;
  refused — see pressure P2-007).
- Retroactively widening v1 rows (breaks the Phase-1a freeze; versioned
  instead).

## Implementation pressure

P2-001 (64-byte digest realization bound shapes the chain), P2-002
(view invariance forces widest-view signatures), P2-003 (host cannot
invoke generics), P2-004..P2-006 (append-only writes; no rename/sync/
mkdir/delete), P2-007 (host/MNCS decision boundary discipline), P2-008
(step-cost and batching pressure). See `pressure/PHASE2-PRESSURE-
SUMMARY.md`.
