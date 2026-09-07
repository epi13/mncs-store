# RFC 0002 — Persistent object identity

**Status:** Accepted for architecture

## Problem

Content hashes are excellent identities for immutable bytes but poor identities for evolving concepts. Conversely, mutable application IDs do not prove content integrity. A machine-native store needs both meanings.

## Decision

Define two separate identity domains:

1. **Object ID:** stable identity of a logical persistent object.
2. **Content ID:** deterministic identity of immutable canonical content/chunks/manifests.

A generation maps an Object ID to a representation root identified by content plus committed metadata. Updating an object creates a new mapping in a later generation; it does not mutate the prior representation.

Object IDs must be globally unambiguous within their declared namespace. The concrete bit layout/issuance mechanism is deferred, but callers must never infer object equality from current content equality.

## Required behavior

- Two logical objects may reference identical content without becoming the same object.
- One logical object may reference different content across generations.
- Historical snapshots preserve the historical mapping.
- Content-addressed deduplication may occur beneath logical identity.
- Import adapters must define whether external IDs become aliases, provenance, or explicit Object IDs; they must not guess silently.

## Failure behavior

Unknown Object IDs return an explicit absence/error appropriate to the API. A content hash mismatch is an integrity failure, not "object not found."

## Rejected alternative

Using only content hashes makes ordinary evolution awkward and pushes mutable identity into ad-hoc side tables. Using only mutable IDs loses intrinsic verification and structural sharing.
