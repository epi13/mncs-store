# RFC 0006 — Object relationships and references

**Status:** Accepted for architecture

## Problem

Machine state is relational and graph-shaped, but conventional blob stores force relationships into application payloads while relational systems often require domain-specific join schemas.

## Decision

`mncs-store` provides a first-class canonical reference representation. A reference identifies a target logical object/namespace and may include relationship type, version/generation constraints, optional edge metadata, and integrity/provenance binding as later schemas require.

References are inspectable without decoding the target application's payload. This makes generic traversal, provenance, garbage collection, authorization, and indexing possible.

## Boundary with mncs-index

Store preserves canonical edges. `mncs-index` may build reverse-edge, adjacency, semantic, similarity, temporal, spatial, or learned structures for efficient discovery. Those indices remain derived unless separately declared canonical.

## Requirements

- dangling references must have explicit semantics rather than silently retargeting;
- historical snapshots retain their historical reference targets/constraints;
- deletion/reclamation must account for retained references and snapshots;
- cyclic graphs are valid and traversal must be bounded by callers/index policy;
- capabilities may restrict following or revealing a reference.

## Rejected alternative

Embedding raw target IDs inside arbitrary payloads prevents the storage system from safely understanding reachability or generic relationships.
