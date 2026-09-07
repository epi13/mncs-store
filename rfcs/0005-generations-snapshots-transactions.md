# RFC 0005 — Generations, snapshots, and atomic commits

**Status:** Accepted for architecture

## Problem

Readers need coherent state while writers update multiple objects, references, and provenance records. Durable chunks arriving independently cannot define visibility.

## Decision

The unit of authoritative visibility is a **generation commit**. Writers stage immutable content and a set of logical-state changes, then atomically publish a generation. Readers bind to a committed generation/snapshot and observe a stable mapping for its lifetime.

A generation can include object-root updates, object creation, reference/provenance changes, and tombstone/reachability changes. Exact transaction syntax is deferred.

## Required semantics

- staged chunks may exist before commit without being visible as the new object value;
- after recovery, a generation is either verifiably committed or not authoritative;
- readers never observe an invented mixture of two generations;
- historical retained generations remain immutable;
- conflict primitives such as compare-and-swap may reject a commit based on expected prior generation/root.

## Durability

Commit visibility and requested durability are related but distinct. An API must state whether it acknowledges local visibility, durable local persistence, or a replication policy. It must not claim a stronger barrier than completed.

## Non-goal

This RFC does not require SQL-style serializable transactions or a specific MVCC algorithm. It establishes stable snapshots and atomic publication as the baseline on which stronger isolation can be specified if needed.
