# Machine-native substrate slice

This tranche preserves the Phase-1 encodings and adds only versioned
representations that are not reinterpretations of the old object bytes.

| Representation | Size | Purpose | Identity domains kept separate |
|---|---:|---|---|
| `store.identity.v1` | 12/32-byte fields | logical objects and content/chunk/root roles | logical vs content/chunk/root |
| `store.descriptor.v1` | 16 bytes | type tag, scalar/layout, shape, alignment, length | representation metadata vs payload |
| `store.relationship.v1` | 80 bytes | typed edge kind, endpoints, generation, provenance identity, ordinal | logical endpoint vs relation generation |
| `store.relationship.v2` | 172 bytes | generic relation type identity, endpoints, generation, provenance, ordinal, optional typed metadata identities | relation type identity vs endpoint/generation/metadata identities |
| `store.provenance.v1` | 112 bytes | source, producer, transformation, generation, evidence, ancestry | source/producer/transformation/evidence |
| `store.commit_feed.v1` | 56 bytes | deterministic Store-to-Index change metadata | Store generation vs Index-through generation |
| `store.semantic_state.v1` | 164 bytes | producer-supplied lifecycle/severity, evidence-set and supersession-set identities, generation, completeness | Store persists codes and set identities; Commons owns their meanings |

`store.relationship.v1` is frozen and remains available for existing Commons
state and compatibility fixtures. `store.relationship.v2` is the reusable
consumer contract for new family domains: Store carries the relation type
identity and optional typed metadata identities, while the producer/owning
authority defines their meaning. The relationship and provenance records are
persisted under separate Store areas and validated as their own native values.
They are not JSON documents embedded in an object blob. The first Commons path
carries the Ingest handoff's semantic type and source/transformation identities
as typed bytes; Commons remains the authority for pressure lifecycle and
meaning.

## Frozen bytes

The following historical encodings remain unchanged and continue to be
tested as frozen compatibility surfaces:

- object logical IDs and 32-byte content/chunk/root widths;
- Phase-1 descriptors, frames, chunk digests, and manifests;
- Phase-1 generation headers and records;
- Phase-2 manifest-v2 headers, chained roots, recovery decisions, and
  reclamation vocabulary.

New relation, provenance, feed, and publication records use explicit version
and magic fields. No historical bytes are mutated in place.

## Publication boundary

`publication_admit` is now the native semantic gate for candidate content,
root, durability, and recovery observations. The Profile-0.16 `publish`
entrypoint has a focused granted-filesystem proof for bounded files. The
ordinary Phase-2 path still uses Python for platform mechanics: directory
creation/listing, file reads, create-exclusive placement, fsync, atomic
rename, recovery scans, and directory barriers. Those operations are
observable as a host boundary in the Store trace; Python does not choose the
semantic admission verdict.

## Identity architecture

The Store does not use one hash for every concept:

```text
logical object ID       stable namespace + serial
content identity        immutable canonical bytes/chunk/root evidence
representation identity descriptor/version/layout
generation identity     committed Store head
semantic subject        Commons/Ingest supplied identity
source/provenance       producer and transformation ancestry
index identity          derived projection/result identity
query identity          external request identity
```

JSON may carry a declaration, migration input, debug view, or exported
query result. It is not the canonical identity surface of these records.
