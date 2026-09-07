# AGENTS.md — mncs-store

## Mission

Build a machine-native persistence substrate for the MNCS ecosystem. Preserve typed machine state, representation metadata, relationships, provenance, generations, integrity, and placement without making JSON, SQL, or another human-oriented interchange format the canonical model.

## Current phase

This repository is in an architecture-first bootstrap. The RFCs and invariants define the intended system; they do **not** imply those features are implemented. Do not turn roadmap language into claims of completion.

## Implementation policy

- Core implementation should be written in **mncs-language**.
- Do not introduce Rust, C++, Python, or another language as the hidden canonical implementation simply because MNCS language is missing a feature.
- When the language blocks a sound implementation, document the gap in `pressure/` with a minimal reproducer, desired semantics, safety constraints, and performance implications.
- Host-language scripts may be used only for clearly bounded development/bootstrap tooling when unavoidable; they must not define storage semantics.

## Non-negotiable invariants

Read `docs/invariants.md` before changing storage semantics. In particular:

- committed content is immutable;
- logical identity is distinct from content identity;
- indices are derived and disposable;
- boundary formats are not canonical storage;
- provenance and relationships must remain representable without application-specific blob decoding;
- placement must not change logical identity;
- durability claims must correspond to completed persistence barriers;
- zero-copy access must never weaken type, lifetime, bounds, or capability safety.

## RFC discipline

Changes to identity, representation descriptors, chunking, commit semantics, references, provenance, placement, zero-copy, indexing contracts, query semantics, replication, security, interchange, or recovery require an RFC amendment or a new RFC.

Each RFC should state:

1. problem and motivation;
2. decision;
3. invariants;
4. failure behavior;
5. security/integrity implications;
6. rejected alternatives;
7. implementation pressure where known.

## Ecosystem boundaries

- `mncs-store`: canonical durable state, representation, generations, integrity, placement metadata.
- `mncs-index`: derived discovery/ranking/traversal structures.
- `mncs-memory`: memory policy, learned/experiential semantics.
- `mncs-ingest`: external-to-native normalization and source capture.
- `mncs-learn`: training/checkpoint/dataset behavior using stored objects.
- `mncs-fabric`: compute/resource scheduling and coordinated data movement.

Avoid absorbing another repository's responsibility merely because the integration is convenient.

## Testing expectations

When implementation begins, tests must cover deterministic encoding, reopen round trips, torn/crash commits, corruption detection, concurrent readers/writers, capability enforcement, stale index behavior, and compatibility across supported targets. Tests should prefer executable semantic proofs over fixture-heavy golden JSON.

## Performance expectations

Do not optimize by throwing away semantics. Measure:

- bytes copied;
- allocations;
- serialization/deserialization work;
- mmap/view eligibility;
- read/write amplification;
- chunk reuse;
- index rebuild cost;
- network movement;
- accelerator transfer behavior.

Record surprising language/runtime pressure even when a workaround exists.
