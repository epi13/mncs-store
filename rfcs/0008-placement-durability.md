# RFC 0008 — Placement, durability, and storage classes

**Status:** Accepted for architecture

## Problem

Machine-native workloads may have the same representation on local NVMe, RAM, GPU memory, another worker, or archive media. Conventional APIs often hide this until expensive copies are already required.

## Decision

Placement is explicit operational metadata attached to verified content/representations, separate from logical identity. A representation may have multiple placements simultaneously.

Durability is expressed as policy/requirements rather than a hard-coded single class. Initial vocabulary may include ephemeral, cache, recoverable, durable, replicated, and archival behavior, but implementation should prefer composable properties where practical.

Examples of policy properties include required durable copies, failure domains, locality constraints, retention/pinning, latency preferences, and whether accelerator residency is merely a cache.

## Fabric boundary

`mncs-store` knows content identity, verified placements, movement requirements, and durability state. `mncs-fabric` knows worker/resource topology and schedules execution. Together they can prefer compute near existing data rather than repeatedly moving large state.

## Requirements

- placement changes do not change Object ID or Content ID;
- unverified replicas are not authoritative;
- cache residency is never confused with durability;
- an acknowledgment states which durability barrier actually completed;
- accelerator memory should normally be treated as volatile placement unless a future backend provides stronger semantics.
