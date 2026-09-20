# mncs-store

Machine-native persistent storage for MNCS: typed objects, graphs, tensors, model state, provenance, versioned data, and zero-copy structures without reducing machine state to documents or tables.

> **Status:** Phase 1a object-core proof complete (2026-09-08); Phase 2
> generations/recovery complete (2026-09-10, Source Profile 0.13).
> General blobs (0..992 bytes) persist → close → reopen → verify →
> typed-read through multi-chunk manifests with hash-chained roots;
> generations commit with compare-and-transition conflicts, stable
> snapshots, an explicit commit state machine, deterministic recovery,
> and sharing-aware reclamation — all semantic bytes AND all semantic
> decisions produced by mncs-language executions. File mechanics
> (files, fsync, rename, deletes) stay host-driven pending language
> file/sync effects; arbitrary sizes past 992 bytes are explicitly
> deferred. See [ROADMAP.md](ROADMAP.md),
> [RFC 0017](rfcs/0017-phase2-multichunk-generations-recovery.md), and
> [pressure/PHASE2-PRESSURE-SUMMARY.md](pressure/PHASE2-PRESSURE-SUMMARY.md).

The current substrate tranche adds a bounded typed-state slice without
changing those frozen bytes. `store.relationship.v1` (80 bytes),
`store.provenance.v1` (112 bytes), and `store.commit_feed.v1` (56 bytes) are
versioned native records stored separately from object payload chunks.
`store.semantic_state.v1` adds a 164-byte identity-bound payload for
producer-supplied lifecycle, severity, evidence-set, supersession-set,
generation, and completeness fields without making Store the authority for
their meanings. A retained `mncs-embed` session now serves repeated semantic batches in the
Store driver; the filesystem lifecycle remains an explicit host boundary
until the larger generation publication protocol can use granted effects
without weakening atomicity or no-follow safety. See
[docs/machine-native-substrate.md](docs/machine-native-substrate.md).

## Why this exists

MNCS components increasingly need durable state, but repeatedly reducing machine state to JSON, rows, documents, or application-specific blobs throws away information the machine already knows: type, layout, alignment, shape, provenance, relations, generation, placement, integrity, and hardware affinity.

`mncs-store` starts from a different question:

> What is the smallest useful unit of machine state, and how can MNCS persist it without translating it into a human-oriented document model first?

JSON, SQL, CSV, Parquet, protobuf, filesystems, and object stores remain useful interoperability boundaries. They are not the canonical internal model.

## Core model

A persistent MNCS object has a stable logical identity and one or more immutable representations. A representation is described by native type/layout metadata and is rooted in content-addressed chunks. Objects can reference other objects, carry provenance, participate in committed generations, and exist in multiple placements or durability classes.

Conceptually:

```text
logical object
  ├─ identity
  ├─ native type + representation descriptor
  ├─ generation-visible root
  │    └─ content-addressed chunks
  ├─ relationships
  ├─ provenance / lineage
  ├─ capabilities
  ├─ integrity evidence
  └─ placements: RAM / NVMe / GPU / remote / archive
```

The model is designed so useful representations can eventually be memory-mapped or viewed directly instead of deserialized through an intermediate document graph.

## Architectural boundaries

```text
external data
     │
     ▼
 mncs-ingest
     │
     ▼
 mncs-store ◀────▶ mncs-index
     │               │
     │               └─ discovery / ranking / traversal / learned indices
     │
     ├─ mncs-memory   learned / experiential semantics
     ├─ mncs-learn    datasets / checkpoints / training lineage
     ├─ mncs-fabric   placement / movement / distributed execution
     ├─ mncs-language typed access and query expressions
     └─ compiler / engine / agents / applications
```

`mncs-store` owns durable representation, identity, generations, integrity, and placement metadata. `mncs-index` owns finding, ranking, relating, and traversing stored state. `mncs-memory` owns higher-level memory behavior. `mncs-ingest` turns external information into machine-native structures suitable for persistence.

## Design invariants

1. **Machine state is not canonically a document.** Boundary formats must not dictate internal representation.
2. **Types and representations are explicit.** Shape, scalar type, layout, alignment, encoding, and version are preserved when known.
3. **Committed content is immutable.** Mutation creates new content and a new committed generation rather than rewriting history in place.
4. **Identity and content identity are distinct.** A logical object can evolve while immutable chunks remain content-addressed.
5. **Relationships are first-class.** References are not hidden inside opaque application blobs.
6. **Provenance is part of state.** Producer, inputs, transformations, verification, and ancestry can travel with an object.
7. **Placement is observable but not identity.** The same object may exist on NVMe, in RAM, on an accelerator, or remotely.
8. **Integrity is verifiable.** Stored roots and chunks have deterministic integrity evidence.
9. **Indices are replaceable derivatives.** Losing an index must not destroy canonical data.
10. **Zero-copy is a design target, not a promise.** Compatible representations should permit direct views; incompatible ones require explicit transformation.

See [docs/invariants.md](docs/invariants.md) for the normative form.

## Repository layout

```text
.
├── AGENTS.md
├── CONTRIBUTING.md
├── ROADMAP.md
├── SECURITY.md
├── docs/
│   ├── architecture.md
│   ├── ecosystem-boundaries.md
│   ├── invariants.md
│   └── terminology.md
├── rfcs/
│   ├── README.md
│   └── 0001 ... 0015
├── pressure/
│   └── README.md
├── src/
│   └── README.md
└── tests/
    └── README.md
```

## RFC map

| RFC | Topic |
|---|---|
| 0001 | Machine-native storage model and project charter |
| 0002 | Persistent object identity |
| 0003 | Native type and representation descriptors |
| 0004 | Content-addressed chunks and manifests |
| 0005 | Generations, snapshots, and atomic commits |
| 0006 | Object relationships and references |
| 0007 | Provenance and lineage |
| 0008 | Placement, durability, and storage classes |
| 0009 | Zero-copy and memory-mapped access |
| 0010 | Index provider interface |
| 0011 | Query execution interface |
| 0012 | Replication and distributed stores |
| 0013 | Capability and security model |
| 0014 | Import/export and compatibility boundaries |
| 0015 | Recovery, corruption detection, and verification |
| 0016 | Phase-1 canonical encodings v1 (frozen) |
| 0017 | Multi-chunk objects, generation commits, recovery (implemented) |

The RFCs are initial architectural decisions, not declarations that implementation is complete.

## Implementation (Phases 1a + 2)

| Area | Where | Notes |
|---|---|---|
| Identity, descriptors, chunks, manifests, generations, recovery | `src/store/*.mncs` | mncs-language, Source Profile 0.13, no stdlib imports |
| Canonical encodings v1 (frozen) | `rfcs/0016-phase1-canonical-encodings.md` | single-chunk classes unchanged, still tested |
| Multi-chunk + generations + recovery | `rfcs/0017-phase2-multichunk-generations-recovery.md` | manifest/descriptor v2, chained roots, CAS, snapshots, commit states |
| Lifecycle driver (host transport) | `tests/store_phase1a.py` | files/fsync/rename only; semantics always MNCS |
| Phase-2 driver (transport + decisions) | `tests/store_phase2.py` | multi-chunk put/get, CAS, snapshots, recovery, reclaim, fault injection |
| Semantic corpora (300+ cases) | `tests/corpora/*.json` | independent oracles; pure suites all backends, hash suites bytecode (P1-B02) |
| Lifecycle + corruption tests | `tests/test_lifecycle.py` | close/reopen, torn/corrupt rejection |
| Generation/recovery/fault tests | `tests/test_phase2.py` | boundaries, CAS, snapshots, 7-point fault matrix, reclamation |
| Typed relations/provenance/feed | `src/store/relationship.mncs`, `src/store/provenance.mncs`, `src/store/commit_feed.mncs` | fixed versioned records, separate persistence, generation-bound validation |
| Retained semantic boundary | `tests/retained_session.py` | one admitted artifact across Store batches; C ABI is transport only |
| Language pressure (23 P1 + 8 P2) | `pressure/` | re-baselined 2026-09-10; see PHASE2-PRESSURE-SUMMARY.md |

Run the suite: `cd tests && python3 -m pytest . -q`
(`MNCS_BACKENDS` narrows the backend matrix; `MNCS_BIN` overrides the
compiler path.)

## Initial proof target

The first executable milestone should remain deliberately small:

```text
MNCS native value
    ↓
representation descriptor
    ↓
content-addressed chunks
    ↓
logical object + generation commit
    ↓
close / reopen
    ↓
typed read or mmap-compatible view
```

That proof should establish deterministic identity, crash-safe commits, integrity validation, and a real typed round trip before distributed replication or sophisticated query planning is attempted.

## Language policy

The intended implementation is **MNCS language first**. If `mncs-language` cannot express a required storage primitive safely or efficiently, record the pressure explicitly under `pressure/` rather than silently moving the core design into another implementation language.

## License

Apache-2.0. See [LICENSE](LICENSE).
