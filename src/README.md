# Source layout

MNCS-language modules whose boundaries follow the RFCs. All canonical
storage bytes are produced by executing these modules; the host test
driver (`tests/store_phase1a.py`) transports bytes opaquely and never
reimplements their semantics. Canonical layouts are frozen by
[RFC 0016](../rfcs/0016-phase1-canonical-encodings.md).

| Module | RFC | Owns |
|---|---|---|
| `store/identity.mncs` (`store.identity.v1`) | 0002 | ObjectId encoding/parse/equality; digest compare/order/match; nominal ChunkId/ContentId/RootId/LogicalId wrappers |
| `store/descriptor.mncs` (`store.descriptor.v1`) | 0003 | 16-byte descriptor encode/validate/accessors; put-path `describe_*` |
| `store/chunk.mncs` (`store.chunk.v1`) | 0004 | Scalar codecs; domain-separated framing; SHA-256 digests/verification; whole-frame verify; typed get projectors |
| `store/manifest.mncs` (`store.manifest.v1`) | 0004/0005 | 64-byte root encode/validate; root identity; generation-record encode/split |
| `store/read_verify.mncs` (`store.read_verify.v1`) | 0015 | In-language chunk-file read + digest verification (dual-effect proof) |

Profile: Source 0.10 (max backend coverage). No stdlib imports, by design:
canonical store bytes must be frozen against stdlib evolution. Every
module elaborates cleanly (only `integer-overflow` proof obligations with
safe fallbacks remain; see pressure P1-021).

Value classes in Phase 1a: u32, u64, u32-pair, fixed blobs (8/16/32),
empty. Multi-chunk objects are reserved, not implemented (pressure
P1-004).
