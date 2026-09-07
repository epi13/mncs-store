# RFC 0004 — Content-addressed chunks and manifests

**Status:** Accepted for architecture

## Problem

Large persistent values should not require full rewrites, duplicate storage, or opaque monolithic blobs when only a portion changes.

## Decision

Committed representation content is immutable and content-addressed. Large representations may be decomposed into chunks referenced by immutable manifests/trees. A representation root identifies the complete structure required to reconstruct or view the value.

The content ID must cover the canonical bytes and any structure necessary to prevent ambiguity. The specific cryptographic algorithm and chunking strategy are intentionally deferred behind versioned identifiers.

## Properties

This model enables:

- structural sharing across object generations;
- deduplication across independent objects;
- integrity checking per chunk and root;
- resumable replication by missing content ID;
- cache promotion without changing identity;
- garbage collection based on reachability from retained roots/generations.

## Requirements

A store must verify fetched/untrusted chunks before treating them as canonical. Chunk presence alone does not mean an object update is committed. Garbage collection must respect historical snapshots, pinned roots, replication/recovery requirements, and in-flight readers.

## Rejected alternative

A single mutable file per logical object couples identity, update granularity, recovery, and placement, making structural sharing and verified replication significantly harder.
