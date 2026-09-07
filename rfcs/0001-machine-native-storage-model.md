# RFC 0001 — Machine-native storage model

**Status:** Accepted for architecture  
**Scope:** project charter and canonical model

## Problem

MNCS components need durable state but conventional persistence APIs encourage them to serialize rich machine state into documents, rows, key/value blobs, or application-specific files. That translation discards or hides type, layout, relationships, provenance, generations, integrity, and physical placement, then forces downstream systems to reconstruct them.

## Decision

`mncs-store` will use a typed persistent-object model. A logical object has stable identity. Each committed generation resolves that identity to an immutable representation root plus metadata. Representation roots are composed from content-addressed immutable content and are interpreted through versioned native representation descriptors.

Relationships, provenance, capabilities, integrity evidence, and placement/durability metadata are first-class concepts rather than conventions hidden inside payloads.

External document/database formats are adapters at system boundaries. They do not define canonical internal storage.

## Project boundary

Store owns persistence, not all data behavior. Search/ranking/traversal belongs to `mncs-index`; learned memory behavior to `mncs-memory`; ingestion semantics to `mncs-ingest`; training behavior to `mncs-learn`; compute/resource scheduling to `mncs-fabric`.

## Invariants

- committed content is immutable;
- logical object identity differs from content identity;
- representations are explicitly described and versioned;
- committed generations are atomically visible;
- indices/caches are not canonical by default;
- placement changes do not change logical identity;
- canonical integrity can be verified independently.

## Non-goals

This RFC does not select a hash function, chunk size, query language syntax, consensus algorithm, compression format, or physical directory layout. Those decisions require measurement and later RFCs.

## Consequence

The first implementation should prove a typed persist/reopen/verify cycle on one node before expanding into a distributed database-shaped product.
