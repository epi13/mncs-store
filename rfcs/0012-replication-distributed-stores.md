# RFC 0012 — Replication and distributed stores

**Status:** Accepted for architecture

## Problem

MNCS runs across heterogeneous workers. Replication should exploit immutable content identity rather than inventing a second distributed data model.

## Decision

Distributed transfer operates primarily on immutable content/manifests plus generation/object metadata. Peers exchange identities, discover missing chunks, transfer content, verify it locally, and only then advertise verified placement.

Logical object identity and content identity remain stable across workers. Node-specific paths/device addresses are never canonical identifiers.

## Consistency

This bootstrap RFC does not choose a global consensus or multi-writer consistency protocol. It requires that any later protocol preserve atomic committed generations within its declared scope and make authority/freshness explicit.

## Anti-rollback / equivocation

A peer must not silently replace a newer authoritative generation with older state when the configured consistency policy forbids rollback. Generation ancestry/checkpoint evidence must be sufficient for the selected replication mode to detect invalid rollback or conflicting authority.

## Heterogeneity

Peers may support different direct-view representations. Canonical portable representations and target-specific optimized representations can coexist, with explicit transformation/provenance. Replication must not label an incompatible native layout as directly usable on another architecture.

## Fabric integration

Fabric can coordinate transfer and computation using Store's placement and integrity facts; Store does not become the cluster scheduler.
