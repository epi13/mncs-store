# RFC 0014 — Import/export and compatibility boundaries

**Status:** Accepted for architecture

## Problem

MNCS must interoperate with JSON, SQL databases, filesystems, protobuf, Parquet, model formats, and external object stores without allowing any one of those formats to dictate canonical machine state.

## Decision

External formats are handled through explicit import/export adapters. Import transforms external representations into typed store objects with source provenance. Export transforms store objects into a requested boundary format and reports any loss of semantics.

## Requirements

- importers treat external data and metadata as untrusted;
- import records enough provenance to identify the external source/version when available;
- export does not silently drop type/layout/provenance/relationship information when the target cannot express it; loss must be declared or caller-approved;
- adapter versioning is independent from canonical representation versioning;
- a filesystem backend may store chunks in files internally, but filesystem paths do not become canonical Object IDs;
- a SQL adapter may project objects into tables, but table shape does not become the universal object model.

## Migration

Canonical representation changes require versioned migration/transformation semantics. A migration should be capable of retaining old roots for rollback/history according to retention policy and should emit provenance linking old and new representations.

## Principle

Interoperability belongs at boundaries. The persistence core should optimize for faithful machine semantics first.
