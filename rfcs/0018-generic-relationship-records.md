# RFC 0018 — Generic relationship records v2

**Status:** Accepted for implementation

## Problem

`relationship.v1` encodes a small closed set of Commons-oriented numeric
relation kinds. It is sufficient for the first Store vertical, but reusing it
for Fabric placement, Forge evidence lineage, RAVEL lifecycle history, or
other family domains would force Store to own each application's vocabulary.

## Decision

Add `store.relationship.v2` as a new representation. The v1 bytes and meaning
remain frozen. A v2 record contains:

- an identity for the relation type supplied by the owning domain;
- source and target logical object identities;
- the Store generation and provenance identity;
- an ordinal for deterministic ordering within a commit; and
- an optional pair of typed metadata-object identities (schema/type and root).

The relation type identity is data, not an enum interpreted by Store. The
metadata pair references an immutable typed Store object; Store does not decode
or assign its application meaning.

## Invariants

1. v1 readers continue to accept only v1 bytes and v2 readers never reinterpret
   v1 bytes.
2. A relation with a zero relation-type identity is invalid.
3. Metadata is either absent (both identities zero) or present (both nonzero).
4. Endpoints remain logical identities; relation type, provenance, and metadata
   remain separate identity domains.
5. Index may query relation type identity and endpoints without decoding an
   application payload.

## Failure behavior

The native validator rejects wrong length, magic, version, reserved flags,
zero relation type, and a half-present metadata pair with distinct codes.
Unknown future versions fail closed and require another explicit
representation.

## Security and integrity

Relation and metadata identities are untrusted input until their referenced
objects are verified under the Store publication/recovery policy. A relation
does not grant access to either endpoint or metadata object.

## Rejected alternatives

- Extending the v1 numeric enum would mutate frozen bytes and hard-code future
  application ontology into Store.
- Storing a JSON edge envelope would violate the canonical representation
  boundary and prevent generic indexing without reparsing.
- Embedding arbitrary metadata bytes would make the edge an opaque application
  blob and duplicate object storage.
