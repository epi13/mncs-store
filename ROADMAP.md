# mncs-store roadmap

The roadmap is ordered to prove semantics before scale. Distributed features should not mask weaknesses in the single-node object model.

## Phase 0 — architecture bootstrap

- [x] project charter and repository boundaries
- [x] normative design invariants
- [x] initial RFC suite
- [x] terminology and architecture docs
- [x] language-pressure recording policy

## Phase 1 — single-node persistent object core

- [ ] canonical object and content identity types
- [ ] deterministic representation descriptors
- [ ] content-addressed chunk writer/reader
- [ ] manifest/root format with versioning
- [ ] local store layout that is an implementation detail, not the public model
- [ ] typed put/get round-trip tests
- [ ] reopen and integrity verification tests

**Exit proof:** an MNCS native value can be persisted, closed, reopened, verified, and read with the same type/layout semantics.

## Phase 2 — generations and recovery

- [ ] atomic generation commit protocol
- [ ] stable reader snapshots
- [ ] compare-and-swap / conflict primitives
- [ ] crash/torn-write recovery
- [ ] unreachable chunk reclamation policy
- [ ] deterministic recovery tests under injected faults

**Exit proof:** after any injected interruption, readers observe either the previous committed generation or the new committed generation, never a fabricated mixture.

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
