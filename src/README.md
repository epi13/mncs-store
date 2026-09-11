# Source layout

MNCS-language modules whose boundaries follow the RFCs. All canonical
storage bytes — and, since Phase 2, all commit/recovery/reclamation
decisions — are produced by executing these modules; the host test
drivers (`tests/store_phase1a.py`, `tests/store_phase2.py`) transport
bytes opaquely and never reimplement their semantics. Canonical layouts
are frozen by [RFC 0016](../rfcs/0016-phase1-canonical-encodings.md)
(v1) and extended by [RFC 0017](../rfcs/0017-phase2-multichunk-generations-recovery.md)
(v2, alongside — never by altering v1).

| Module | RFC | Owns |
|---|---|---|
| `store/identity.mncs` (`store.identity.v1`) | 0002 | ObjectId encoding/parse/equality; digest compare/order/match; nominal ChunkId/ContentId/RootId/LogicalId wrappers |
| `store/descriptor.mncs` (`store.descriptor.v1`) | 0003 | 16-byte descriptor encode/validate/accessors; put-path `describe_*`; descriptor v2 (BYTE_SEQ to 992 B) |
| `store/chunk.mncs` (`store.chunk.v1`) | 0004 | Scalar codecs; domain-separated framing; SHA-256 digests/verification; whole-frame verify; typed get projectors; `concat32_32`; canonical tail-padding checks; partition arithmetic |
| `store/manifest.mncs` (`store.manifest.v1`) | 0004/0005 | 64-byte root encode/validate; root identity; generation-record encode/split; manifest v2 (chained multi-chunk roots) |
| `store/generation.mncs` (`store.generation.v1`) | 0005 | Generation headers; CAS decisions + conflict tokens; snapshot tokens + binding; commit state machine; reclamation membership scans |
| `store/recovery.mncs` (`store.recovery.v1`) | 0015 | Store/generation triage; STAY/PROMOTE/REFUSE selection; prune decisions |
| `store/read_verify.mncs` (`store.read_verify.v1`) | 0015 | In-language chunk-file read + digest verification (dual-effect proof) |

Profile: Source 0.13. No stdlib imports, by design:
canonical store bytes must be frozen against stdlib evolution. Every
module elaborates cleanly (only CMP301 proof obligations with safe
fallbacks remain; see pressure P1-021).

Value classes: Phase-1a exact widths (u32, u64, u32-pair, fixed blobs
8/16/32, empty — frozen v1, still tested) plus general blobs 0..992 B
via manifest v2 (31 x 32-byte chunks, canonical zero-padded tails,
hash-chained roots). Arbitrary sizes past 992 B remain pressure
(P2-001).
