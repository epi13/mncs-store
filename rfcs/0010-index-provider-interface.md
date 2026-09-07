# RFC 0010 — Index provider interface

**Status:** Accepted for architecture

## Problem

A machine-native store needs efficient lookup and traversal but should not embed every indexing strategy into the persistence core.

## Decision

Define an index-provider boundary where `mncs-index` (or another conforming provider) consumes committed changes and returns candidate Object IDs/content roots for queries. The store remains authoritative and verifies/materializes candidates against the reader's generation and capabilities.

## Commit feed

A successful generation can expose a deterministic change description containing relevant created/updated/tombstoned objects, relationship changes, type/representation metadata, and provenance references subject to capability rules.

Providers must be able to rebuild from canonical store state. Checkpoints/index roots may themselves be persisted as derived objects.

## Consistency

Queries must state or expose index freshness. A stale index may produce incomplete candidates if the query allows it; APIs requiring complete results must either use an index known complete through the target generation or fall back to another complete plan.

## Failure model

Index corruption/loss is a performance/availability problem, not canonical data loss. Store recovery must not depend on an index that cannot be rebuilt.

## Non-goal

This RFC does not choose B-tree, hash, vector, graph, learned, full-text, or temporal algorithms. Those are provider concerns.
