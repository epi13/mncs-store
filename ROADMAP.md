# mncs-store roadmap

The roadmap is ordered to prove semantics before scale. Distributed features should not mask weaknesses in the single-node object model.

## Phase 0 — architecture bootstrap

- [x] project charter and repository boundaries
- [x] normative design invariants
- [x] initial RFC suite
- [x] terminology and architecture docs
- [x] language-pressure recording policy

## Phase 1 — single-node persistent object core

- [x] canonical object and content identity types (Phase 1a: width +
  nominal types, negative elaboration test; issuance host-owned, P1-015)
- [x] deterministic representation descriptors (Phase 1a: 16-byte v1,
  4 value rows, 172 corpus cases incl. malformed rejection)
- [x] content-addressed chunk writer/reader (Phase 1a: exact widths
  0/4/8/16/32, SHA-256 domain-separated frames; arbitrary sizes deferred,
  P1-004–P1-006)
- [x] manifest/root format with versioning (Phase 1a: 64-byte v1,
  single-chunk; multi-chunk reserved, P1-004)
- [x] local store layout that is an implementation detail, not the public model
  (Phase 1a: host-owned `chunks/objects/generations/temp`, P1-001–P1-003)
- [x] typed put/get round-trip tests (u32/u64/pair/blob8/16/32/empty)
- [x] reopen and integrity verification tests (corruption/torn/missing
  suite: bit-flips, truncation, missing files, immutability planting;
  integrity vs not-found kept distinct)

**Exit proof:** an MNCS native value can be persisted, closed, reopened, verified, and read with the same type/layout semantics.
Phase 1a meets this for the covered value classes with host-owned
lifecycle mechanics; full in-language lifecycle, multi-chunk values, and
arbitrary sizes remain open and are tracked as language pressure
(`pressure/PHASE1-PRESSURE-SUMMARY.md`).

**Phase 1b modernization (2026-09-10, Profile 0.13):** sources bumped to
`mncs 0.13`; P1-009/P1-010 workarounds removed (boolean ordering logic);
P1-008 resolved (1024-wide iteration); single-chunk v1 classes frozen
and still tested alongside the Phase-2 multi-chunk path (0..992 B
blobs via manifest v2). See RFC 0017 and the P1 re-baseline appendices.

## Phase 2 — generations and recovery

- [x] atomic generation commit protocol (Phase 2: explicit 5-state
  machine, `commit_next` transitions in-language, publication host-owned
  pending file effects P2-004/P2-005)
- [x] stable reader snapshots (snapshot tokens + `snap_permits`
  binding; reads resolve through exactly one generation mapping)
- [x] compare-and-swap / conflict primitives (`cas_decide` +
  `conflict_token` with observed/attempted generations; no silent retry)
- [x] crash/torn-write recovery (`classify_store`/`classify_generation`/
  `recover_decide`: STAY/PROMOTE/REFUSE; PROMOTE moves the pointer only
  for fully valid candidates)
- [x] unreachable chunk reclamation policy (`table_contains`
  membership over verified retained manifests; shared chunks survive;
  snapshot pins respected; staged/temp pruning gated by `prune_decide`)
- [x] deterministic recovery tests under injected faults (7 commit
  boundaries × old-or-new assertions in `tests/test_phase2.py`)

**Exit proof:** after any injected interruption, readers observe either the previous committed generation or the new committed generation, never a fabricated mixture.
Phase 2 meets this for the bounded domain (objects 0..992 B,
generations to 23 records in-language) with host-owned file mechanics;
full in-language lifecycle, arbitrary sizes, and wide generation scans
remain open and are tracked as language pressure
(`pressure/PHASE2-PRESSURE-SUMMARY.md`).

## Phase 3 — native views and representation-aware I/O

- [ ] mmap-compatible representations
- [ ] typed slices/struct arrays/tensors
- [ ] alignment and endianness rules
- [ ] explicit transformations for incompatible layouts
- [ ] copy/allocation instrumentation

**Exit proof:** eligible values can be reopened as safe typed views without document deserialization.

## Phase 4 — relationships, provenance, index and query contracts

- [ ] first-class references and relationship metadata
- [ ] provenance/lineage objects
- [ ] `mncs-index` provider integration
- [ ] typed query execution boundary
- [ ] stale/rebuildable index semantics

**Exit proof:** an index can be destroyed and rebuilt from canonical store state without information loss.

## Phase 5 — placement, replication, and Fabric integration

- [ ] durability/placement policy model
- [ ] replica verification and anti-rollback semantics
- [ ] transfer protocol based on immutable chunks
- [ ] `mncs-fabric` placement coordination
- [ ] remote and heterogeneous worker testing
- [ ] accelerator-aware placement experiments

**Exit proof:** replicated objects preserve identity and integrity across heterogeneous workers while placement remains independent of logical identity.

## Phase 6 — ecosystem adoption and pressure campaign

Migrate representative state from JSON/ad-hoc files in selected MNCS projects. Prioritize workloads that pressure different dimensions: compiler IR/cache state, ingest artifacts, micro-model state, training checkpoints, engine assets, agent evidence, and large relationship graphs.

Every migration should measure correctness, copy/serialization reduction, storage amplification, recovery behavior, and new `mncs-language` pressure.
