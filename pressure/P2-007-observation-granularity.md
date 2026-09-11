# P2-007 — Observation granularity: 64 B reads, one root per capability

New Phase-2 pressure (discovered 2026-09-10 building lifecycle
verification and reclamation over fs_*/host_read).

Storage feature being implemented: bulk verification (N chunk files per
read path) and retained-root assembly for reclamation (RFC 0015
transitive checking; RFC 0004 reachability).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13): `host_read` grants bind one capability to one
whole file (<= 64 B, loaded once at realization); `fs_read_bytes_at`
reads <= 64 B per call from one granted root; `host_write` appends have
no read-back. Consequences measured this run:

- Verifying a 92-byte v2 manifest in-language needs 2+ chunked reads
  plus host-side reassembly decisions (or one arg-passed view, which
  costs JSON transport instead).
- A 1020-byte manifest needs 16 sequential `fs_read_bytes_at` calls to
  observe fully; each call is a separate effect invocation with its own
  steps.
- Assembling a retained-roots table (N x 32 B) in-language is possible
  only through repeated chunked reads plus host concatenation; the
  membership verdict (`table_contains`) is in-language but its INPUT
  assembly is host-side byte shuffling.

Minimal reproducer: observe a 1020-byte manifest file and its 31 chunk
digests in ONE MNCS call — impossible; the widest single observation is
64 bytes per effect call.

Required semantics: parameterized multi-granularity observation —
bounded scatter-gather reads (one call, K disjoint ranges), table
assembly primitives (bounded concat of observed chunks into a view), and
read-back of own-grant appends — so bulk verification is one function
over many files instead of N calls plus host reassembly.

Why the current behavior/API is insufficient: the SHAPE is proven
(in-language file -> digest verification works; fs_* chunking works),
but granularity forces the driver to choose between per-file
capabilities that do not scale (P1-019 pattern) and arg-passed bytes
that cost 25x JSON expansion per byte (P1-016 compounding). The
lifecycle driver verifies through arg-passed bytes in one batched
invocation per N files; the decision stays MNCS, the batching geometry
stays host.

Safety/correctness implications: LOW (transport geometry, not
semantics). One real hazard: host-side reassembly order must match
manifest order — enforced because MNCS chain verification fails closed
on any reorder, so a host shuffle cannot pass silently.

Performance implications: MEDIUM (multiplies invocations and bytes;
see P2-008 measurements).

Workaround used: arg-passed bytes for bulk verification (batched, one
invocation per N files); fs_* chunked reads kept for recovery scans and
single-file proofs. Semantically exact (MNCS decides every verdict);
bootstrap substitute for observation geometry.

What the language/stdlib/runtime should ideally provide: root-scoped
read capabilities with bounded multi-range reads and in-language table
assembly (bounded concat/materialize), with documented oversize and
truncation semantics.

Affected backend(s): all for geometry; bytecode-only for realization
(P1-B02).

Severity: moderate (scale/ergonomics with measured costs).

Status: workaround (arg-passed batch verification + fs_* proofs)

Acceptance test: one MNCS call observes a 1020-byte manifest plus all
31 referenced chunk headers through granted reads alone (no byte
arguments), returning the same chain verdict as the arg-passed path.
