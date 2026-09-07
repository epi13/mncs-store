# MNCS ecosystem boundaries

`mncs-store` is intentionally narrow enough to be shared by many MNCS projects without swallowing their domain logic.

## mncs-store

Owns:

- logical persistent identity;
- versioned representation descriptors;
- immutable content/chunk identity;
- generation/snapshot/commit semantics;
- canonical references and provenance representation;
- integrity verification and recovery;
- placement/durability metadata and replication primitives;
- typed storage access contracts.

Does not own semantic ranking, memory policy, data ingestion semantics, model training policy, or compute scheduling.

## mncs-index

Owns derived structures for discovery, lookup, relationship traversal, ranking, similarity, temporal/spatial search, and eventually learned indices. Index roots may be persisted through `mncs-store`, but index contents remain rebuildable derivatives unless an RFC explicitly says otherwise.

Contract direction:

```text
store commit → changed object/reference set → index provider
query → index candidates → store verification/materialization
```

## mncs-memory

Owns memory behavior: what becomes memory, retention/reinforcement, associations, micro-model relationships, retrieval policy, and learned/experiential semantics. It should persist durable state through `mncs-store` and use `mncs-index` when useful.

## mncs-ingest

Owns turning external data into normalized machine-native structures, including source capture and semantic extraction. `mncs-store` persists the resulting objects and source/provenance evidence; it does not decide how a PDF, image, packet stream, source tree, or language corpus should be interpreted.

## mncs-learn

Owns training processes, dataset construction policy, checkpoints, evaluation, and model evolution. Store supplies durable typed objects and lineage primitives; Learn supplies training meaning.

## mncs-fabric

Owns execution/resource topology, worker capabilities, scheduling, and coordinated movement. Store exposes where verified representations/chunks are placed and what movement/durability is required. Fabric can use that information to schedule compute near data.

## mncs-language

Should eventually expose first-class typed store APIs and query expressions. Store must pressure the language where persistence needs lifetimes, mmap views, async I/O, atomics, crash consistency, typed reflection, heterogeneous memory, or capability semantics not yet expressible.

## Consumers

Compiler, engine, Atlas/agents, actions, and applications should use storage semantics without each inventing `state.json`, `cache.json`, custom SQLite schemas, or private binary formats for information that belongs in the shared persistence substrate.
