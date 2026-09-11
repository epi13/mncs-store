# Language and system pressure

This directory records cases where implementing `mncs-store` reveals missing or awkward capabilities in `mncs-language`, its runtime, compiler backends, Fabric, or adjacent MNCS primitives.

A pressure report should include:

- the storage feature being implemented;
- minimal MNCS-language reproducer or pseudo-code;
- what cannot be expressed or cannot meet the required semantics;
- safety/correctness requirement;
- performance requirement and measurements when available;
- workaround used, if any;
- preferred language/runtime capability;
- affected targets/backends.

Pressure is useful evidence, not a reason to hide the core implementation in another language. Storage is expected to stress lifetimes/borrowing, mmap and raw views, atomics, async I/O, checked arithmetic, crash-consistency barriers, typed reflection, SIMD, heterogeneous memory, CUDA interaction, capability types, concurrency, and distributed execution.

## Phase 1 campaign index

- [PHASE1-PRESSURE-SUMMARY.md](PHASE1-PRESSURE-SUMMARY.md) — consolidated
  findings, severity table, and recommended language implementation order.
- `P1-001`–`P1-003` — persistence blockers (file write, sync barrier,
  atomic rename / directory ops).
- `P1-004` — no streaming hash; `P1-005` — 64-element sequence bound;
  `P1-006` — no scatter-write into buffers.
- `P1-007`–`P1-015` — type-system, boundary, and capability pressure
  (single results, iteration ceiling, view subsumption, host-called
  generics, record identities, issuance).
- `P1-016` — no in-process call boundary (measured subprocess/JSON cost).
- `P1-017`–`P1-020` — paths, zero-copy views, grant shape, error codes.
- `P1-021`, `P1-B01` — overflow-obligation debt and the C11
  checked-arithmetic divergence (with in-suite canary).
- `P1-B02` — host effects realized only by research-bytecode; compiled
  backends refuse with a silent shape-shift (pinned by effects probe).
- Every `P1-*` report carries a `Re-baseline 2026-09-10` appendix
  re-verdicting it against Source Profile 0.13 (RESOLVED /
  PARTIALLY_RESOLVED / STILL_REPRODUCES / REFRAMED). Historical sections
  are preserved; appendices are current truth.

## Phase 2 campaign index

- [PHASE2-PRESSURE-SUMMARY.md](PHASE2-PRESSURE-SUMMARY.md) — current
  findings, re-baseline table, and the repair order for the next
  mncs-language run.
- `P2-001` — 64-byte digest realization bound with no incremental API
  (reframes P1-004; answered to 992 B by chained v2 roots).
- `P2-002` — no view-width subsumption forces widest-view boundary
  signatures (Phase-2 cost of P1-012; new).
- `P2-003` — no generic boundary functions forces per-width tail
  families (Phase-2 instance of P1-013; new).
- `P2-004` — append-only `host_write` limits (narrows P1-001).
- `P2-005` — no atomic publication / namespace mutation / sync barrier
  (narrows P1-002/P1-003 with commit-protocol evidence).
- `P2-006` — in-language generation scans bound at 23 records (new).
- `P2-007` — observation granularity: 64 B reads, one root per
  capability (new).
- `P2-008` — measured step-cost and boundary-crossing pressure (new).
