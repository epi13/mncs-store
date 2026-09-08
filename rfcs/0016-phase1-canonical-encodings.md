# RFC 0016 — Phase-1 canonical encodings v1 (single-node object core)

**Status:** Accepted for implementation (Phase 1a proof scope)
**Amends:** none (concretizes RFC 0002/0003/0004/0015 where they defer
choices; defers nothing it cannot implement soundly)

## Problem

RFCs 0001–0005 intentionally defer the hash function, byte order,
framing, manifest layout, and commit mechanics "behind versioned
identifiers" pending implementation pressure. Phase 1a now selects them
— minimally, reversibly, and only as far as the current language allows
sound implementation.

## Decision

### Byte order and integers

All multi-byte integers in canonical encodings are unsigned big-endian.
This matches `mncs.std.encoding.v1` and removes host-endianness folklore
from every decoder. Decoders use wrapping operators where the true value
provably fits the target width (exact, not approximate); see pressure
P1-B01 for why plain checked operators were avoided.

### Identity (RFC 0002 concretized)

- Logical ObjectId: 12 bytes, `[namespace u32 BE][serial u64 BE]`.
  Phase-1a namespace is `1`. Serials are store-issued u64 counters;
  issuance is host-owned in Phase 1a (pressure P1-015).
- Content identity: 32-byte SHA-256 over canonical framed bytes
  (verify-only host primitive; no home-grown hash — RFC 0004's deferred
  algorithm is resolved to SHA-256 for v1 frames and roots).
- ChunkId, ContentId, RootId are nominally distinct record types
  in-language; on the wire all are 32-byte digests distinguished by
  POSITION (manifest offsets), never by guesswork.

### Representation descriptors v1 (RFC 0003 concretized)

16 bytes: `MD | ver=1 | type | scalar | rank | dim0 u16 | len u32 |
align | layout=0 | endian=1 | reserved=0`. Valid rows:

| type | scalar | rank | dim0 | len | align |
|---|---|---|---|---|---|
| 1 U32 | 3 U32 | 0 | 1 | 4 | 4 |
| 2 U64 | 4 U64 | 0 | 1 | 8 | 8 |
| 3 BYTE_SEQ | 1 BYTE | 1 | 0..32 | == dim0 | 1 |
| 4 U32_PAIR | 3 U32 | 0 | 2 | 8 | 4 |

Anything else fails `descriptor.validate` with an explicit code (1–13).
`layout=0` means tightly packed big-endian bytes; `endian=1` is big-endian
canonical. Unknown representation versions fail, never guess (invariant 8).

### Chunks v1 (RFC 0004 concretized)

Frame: `0x43 ('C') | ver=1 | payload-len u16 BE | payload`. Supported
payload widths: 0, 4, 8, 16, 32. ChunkId = SHA-256(frame). Chunk FILES
store frames. `frame_valid` codes: 0 OK, 1 TRUNCATED, 2 BAD_DOMAIN,
3 BAD_VERSION, 4 LENGTH_MISMATCH, 5 UNSUPPORTED_WIDTH. Committed chunks
are immutable; same-name/different-bytes is a fatal integrity violation;
same-name/same-bytes is idempotent.

### Manifests/roots v1 (RFC 0004 concretized)

64 bytes: `MR | ver=1 | type | count=1 | res=0 | descriptor[16] |
total u32 | chunk u32 | res2[2] | chunk-digest[32]`. RootId =
SHA-256(all 64 bytes). `manifest.validate` codes: 0 OK, 1 BAD_VIEW_LEN,
2 BAD_MAGIC, 3 BAD_VERSION, 4 BAD_TYPE, 5 BAD_COUNT, 6 BAD_RESERVED,
7 BAD_DESCRIPTOR, 8 LEN_MISMATCH, 9 DESC_LEN_MISMATCH. Multi-chunk roots
are RESERVED, not implemented: `chunk_count != 1` is rejected (pressure
P1-004 explains why: no streaming hash over >64-byte identities).

### Generations and layout (RFC 0005/0015 bootstrapped, host-owned)

Physical layout (implementation detail, NOT the public model):

```text
store/
  meta                  16 B: MS|1|0|namespace u32|next_serial u64
  current               8 B:  MC|1|0|0|gen u32
  objects/<obj12hex>    64 B canonical root
  chunks/<digesthex>    framed chunk bytes
  generations/<genhex>  8 B header (MG|1|0|0|count u32) + N×44 B records
  temp/                 staging area
```

Generation records are 44 bytes: `[object 12][root-id 32]`, encoded and
split by `manifest.gen_record*` (one in-language authority for the
record layout). Publication is temp-write + fsync + atomic rename +
current-pointer rename (host-executed; pressures P1-001–P1-003). Each put
commits a full-snapshot generation (simple, crash-explainable; deltas
deferred to Phase 2). `objects/` writes are create-exclusive: same bytes
→ idempotent; different bytes → fatal (immutability enforcement).

### Corruption behavior (RFC 0015 concretized)

- Hash mismatch ⇒ IntegrityError (never "not found").
- Unknown object ⇒ NotFoundError (distinct type).
- Truncated/torn files ⇒ length-gate codes (frame 4/1, manifest 1,
  generation header/count mismatch), never partial interpretation.
- Unknown store/manifest/descriptor/frame versions ⇒ explicit refusal.
- Type-tag mismatch on typed read ⇒ TypeMismatchError before decoding.

### Size and overflow semantics

Payloads ≤ 32 bytes; views ≤ 64 bytes; descriptors 16 bytes; roots
64 bytes; records 44 bytes. All length/offset arithmetic is checked or
provably-exact wrapping; impossible layouts are rejected, never
allocated from (invariant 6).

## Invariants (restated for v1)

- V1 bytes are frozen: any future change ships a new version number and
  readers that fail closed on unknown versions.
- Canonical bytes are produced ONLY by `src/store/*.mncs` executions;
  the host driver transports them opaquely.
- JSON is transport for the test harness only, never canonical state.

## Rejected alternatives

- Home-grown hash (rejected: unjustifiable crypto; used host SHA-256,
  filed P1-004 for streaming).
- JSON manifests (rejected: invariant 23).
- Multi-chunk-via-truncated-digests (rejected: weakens content addressing
  silently; deferred honestly instead).
- Collapsing ObjectId and ContentId (rejected: invariant 1; enforced by
  width + nominal types + negative elaboration test).

## Implementation pressure

P1-001–P1-003 (I/O effects), P1-004 (streaming hash), P1-005 (64-bound),
P1-006 (scatter), P1-015 (issuance), P1-017 (paths). Phase-1a scope is
shaped exactly by these six; the rest (P1-007–P1-014, P1-016, P1-018–
P1-021, P1-B01) were worked around or pinned in-suite.
