# mncs-store Phase 1 language pressure — campaign summary

Run: Spark 1.3 Extra High implementation + language-pressure run, 2026-09-08.
Scope: single-node persistent object core (ROADMAP Phase 1).
Toolchain: mncs-language CLI (`target/debug/mncs`), Source Profile 0.10,
five executable backends; store sources in `src/store/*.mncs`.

All pressure below was discovered through real implementation, not
inspection. Every item has a full report under `pressure/P1-*.md` with a
minimal reproducer; every workaround is labeled in code and tested.

## Blockers (storage capability inexpressible; host-owned in Phase 1a)

| ID | Pressure | Blocking? | Workaround? |
|---|---|---|---|
| [P1-001](P1-001-no-file-write-effect.md) | No file-write effect | YES (lifecycle mechanics) | host driver transports bytes |
| [P1-002](P1-002-no-durable-sync-barrier.md) | No fsync/durability barrier | YES (durability claims) | host `os.fsync` ordering |
| [P1-003](P1-003-no-atomic-rename-or-dir-ops.md) | No atomic rename / mkdir / readdir / stat | YES (Phase 2 commit protocol) | host `os.replace`, `O_EXCL` |
| [P1-004](P1-004-no-streaming-hash.md) | Single-shot SHA-256 over ≤64 B views only | YES (multi-chunk roots) | single-chunk Phase 1a; count!=1 rejected |

## Major pressure

| ID | Pressure | Severity | Blocking? | Workaround? | Language area |
|---|---|---|---|---|---|
| [P1-005](P1-005-sequence-bound-64.md) | MAX_SEQUENCE_BOUND = 64 | major | caps values at 32 B | exact-width classes | bounded-data substrate |
| [P1-006](P1-006-no-scatter-write.md) | No scatter-write / mutable buffers | major | generic framing | monomorphized widths | data construction |
| [P1-B01](P1-B01-c11-checked-arith-divergence.md) | C11 traps valid u32 checked products | major (backend) | wrapping discipline | canary + allowlist | backend lowering |
| [P1-B02](P1-B02-effects-bytecode-only.md) | Host effects realized only by research-bytecode; silent refusal shape-shift | major (backend) | bytecode-scoped crypto suites | probe + refusal allowlist | backend capabilities |
| [P1-021](P1-021-overflow-obligation-debt.md) | Overflow obligations undischargeable | moderate+ | proof debt + fallback divergence | wrapping + canary | verification/obligations |

## Moderate pressure

| ID | Pressure | Workaround? | Language area |
|---|---|---|---|
| [P1-007](P1-007-single-result-functions.md) | Single-result functions | projector families | surface syntax |
| [P1-008](P1-008-iteration-bound-ceiling.md) | Iteration bound 1..=32/level | indexing discipline | bounded iteration |
| [P1-012](P1-012-no-view-width-subsumption.md) | No view-width subsumption | inline checks / widest views | type system |
| [P1-013](P1-013-host-cannot-call-generics.md) | Host cannot name type args | non-generic boundary layer | execution requests |
| [P1-014](P1-014-record-identities-at-boundary.md) | Record identities brittle at boundary | scalar boundary + round-trip laws | ABI |
| [P1-015](P1-015-no-identity-issuance.md) | No unique-id issuance | host counter + O_EXCL | effects/capabilities |
| [P1-016](P1-016-no-in-process-call-boundary.md) | Subprocess+JSON per call (perf) | batching + counters | tooling/embedding |
| [P1-017](P1-017-no-path-type.md) | No path/string type | host layout naming | type system |
| [P1-018](P1-018-no-zero-copy-views.md) | No mmap/leased views (Phase 3) | none needed yet | runtime/views |
| [P1-019](P1-019-host-read-grant-shape.md) | Single-file ≤64 B read grants | arg-passed bytes | capabilities |
| [P1-020](P1-020-untyped-error-codes.md) | u64 codes, not typed errors | code tables + corpus pins | ABI/errors |

## Minor ergonomics

| ID | Pressure | Workaround? |
|---|---|---|
| [P1-009](P1-009-no-bool-equality.md) | No `==` over `bool` | i64 flag discipline |
| [P1-010](P1-010-byte-comparison-asymmetry.md) | `<` missing over `byte` (only `<=`) | compound idiom |
| [P1-011](P1-011-wrapping-literal-position.md) | Wrapping-op literals right-side only | coding discipline |

## Positive findings (investigated, NO pressure)

- Dual effects under one capability (`host_read` + `sha256_digest` in
  `store.read_verify`) elaborate and execute — effect composition works.
- `sha256_digest` accepts any byte-sequence width (no bound check at the
  intrinsic); results agree with hashlib on all backends tried.
- Cross-module imports (`use store.identity;` + qualified calls) resolve
  deterministically with nominal identity preservation.
- Generic view params (`[byte; up_to N]`) elaborate with explicit Nat
  args (host just cannot invoke them — P1-013).
- Full-range slices (`root[0..64]`) elaborate and hash correctly.
- Width mismatches at the corpus boundary fail closed (`invalid_request`,
  MNE133) — the type system already enforces identity separation.

## Workarounds currently used (all labeled in code)

1. Host driver owns files/fsync/rename/counters/paths (P1-001–003, 015, 017).
2. Single-chunk Phase 1a; multi-chunk rejected, not approximated (P1-004).
3. Exact-width value classes 0/4/8/16/32 (P1-005, P1-006).
4. Wrapping arithmetic where exactness is provable (P1-B01, P1-021).
5. Scalar/bytes harness boundary; records proven via round-trip laws (P1-014).
6. u64 status-code tables pinned by corpora (P1-020).
7. Maximal call batching + invocation counters (P1-016).

## Recommended mncs-language implementation order (dependency leverage)

1. **File effects + sync barrier + atomic rename** (P1-001–003) — unblocks
   the entire lifecycle in-language; everything else compounds on this.
   Realize effects on compiled backends with declared per-backend support
   (P1-B02) in the same tranche. Otherwise integrity proofs stay
   interpreter-only and cross-backend agreement for hashing is impossible.
2. **Streaming hash + larger views, together** (P1-004 + P1-005) — one
   without the other strands multi-chunk values; ship as a pair.
3. **Bulk data construction** (P1-006 scatter/copy + P1-012 subsumption +
   P1-013 host type args) — collapses ~40% of store source volume and all
   width-duplicated validators.
4. **Checked-arithmetic uniformity** (P1-B01 fix + P1-021 discharge) —
   removes the only backend divergence found and the permanent proof debt.
5. **Boundary values** (P1-014 stable records + P1-020 typed errors +
   P1-017 paths + P1-015 issuance) — makes the store API nominal
   end-to-end instead of scalar-at-the-edges.
6. **Tooling scale** (P1-016 in-process calls) — test-time cost only, but
   it dominates every suite run.
7. **Phase-3 views** (P1-018) — correctly sequenced after the core above.
8. **Ergonomics sweep** (P1-007–P1-011, P1-019) — small, independent,
   good first issues for the language run.
