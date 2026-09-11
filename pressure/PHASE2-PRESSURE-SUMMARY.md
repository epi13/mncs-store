# mncs-store language-pressure campaign — Phase 2 summary (current)

Run: Spark 1.3 Extra High implementation + pressure run, 2026-09-10.
Scope: Phase-1 modernization, multi-chunk objects, ROADMAP Phase 2
(generations, snapshots, CAS, commit protocol, recovery, reclamation).
Supersedes nothing: the Phase-1a ledger
(`pressure/PHASE1-PRESSURE-SUMMARY.md`, 2026-09-08, Profile 0.10) is
preserved as historical evidence; every P1 report carries a
`Re-baseline 2026-09-10` appendix with its current verdict.

## Toolchain (exact, reproducible)

- mncs-language HEAD `890a653308a7aa273a64e410b5500aeaa0734316`
  (2026-09-09, "docs(language): record post-merge validation in tranche
  evidence"), branch `feat/proof-transport-exhaustion-hardening`, WITH
  uncommitted changes (18 modified files incl. codegen backends, model,
  syntax, profile docs; 4 untracked evidence/example files).
- Source Profile 0.13 (additive over 0.1–0.12; store sources bumped
  `mncs 0.10` -> `mncs 0.13`).
- CLI `mncs 0.1.0` (debug build at
  `mncs-language/target/debug/mncs`).
- Backends tested: the five executable backends
  (research-bytecode, portable-wasm-mvp, c11, llvm-ir, cranelift) on
  x86_64 Linux. riscv32/ebpf/ptx64 exist in the matrix but were not
  executed (no emulator/driver on this host).

All pressure below was discovered through real implementation, not
inspection. Every item has a report under `pressure/` with a minimal
reproducer; every workaround is labeled in code and tested.

## Re-baseline table

| ID | Old status | Current status | Store feature | Current evidence | Workaround | Severity | Fix leverage |
|---|---|---|---|---|---|---|---|
| P1-001 | blocker (no write effect) | PARTIALLY_RESOLVED | lifecycle mechanics | `host_write` append-only executes (4+4 appends verified) | host files/fsync/rename | major | HIGH (with P1-002/P2-005) |
| P1-002 | blocker (no sync) | STILL_REPRODUCES | durability claims | source grep: zero sync primitives | host `os.fsync` ordering | blocker | HIGH |
| P1-003 | major (no ns ops) | PARTIALLY_RESOLVED | commit/recovery scans | fs_list/fs_read execute (listing, chunked reads, EOF, wild-index refusal) | host rename/mkdir/delete | major | HIGH |
| P1-004 | major (<=64 B hash) | REFRAMED → P2-001 | multi-chunk roots | elaboration widened, realization still 64-capped (InvalidRequest repro) | chained v2 roots | major | HIGH |
| P1-005 | major (bound 64) | PARTIALLY_RESOLVED | value sizes | ceiling 64→1024 (MNE105 past 1024) | 992 B bounded objects | moderate | MEDIUM |
| P1-006 | major (no scatter) | PARTIALLY_RESOLVED | framing/construction | `replace` + repeats + concat verified | 1 concat + per-width tails | moderate | MEDIUM |
| P1-007 | moderate (1 result) | STILL_REPRODUCES | parse/project | no tuples in 0.13 | projector families (grown) | moderate | LOW |
| P1-008 | moderate (iter ≤32) | RESOLVED | all scans | 64- and 1024-scans execute; envelope 1M | none | — | — |
| P1-009 | minor (no bool ==) | RESOLVED | digest ordering | `==`/`!=`/`!` execute | REMOVED (bool logic) | — | — |
| P1-010 | minor (no byte <) | RESOLVED | digest ordering | `byte <` executes | REMOVED (compound idiom) | — | — |
| P1-011 | minor (wrap side) | STILL_REPRODUCES | length arith | `28 +% count` live failure at manifest.mncs:309 | right-side discipline | minor | LOW |
| P1-012 | moderate (no subsumption) | STILL_REPRODUCES | validators | MNE133 live (header round trip removed) | widest-view signatures | moderate | MEDIUM |
| P1-013 | moderate (no generics) | PARTIALLY_RESOLVED | boundary layer | source inference works; host call still "SSA function does not exist" | non-generic boundary | moderate | MEDIUM |
| P1-014 | moderate (records) | STILL_REPRODUCES | nominal IDs | abi strings still versioned; OrderState change proved brittleness | scalar boundary + laws | moderate | MEDIUM |
| P1-015 | moderate (no issuance) | STILL_REPRODUCES | serials | no issuance primitive in 0.13 | host counter + O_EXCL | moderate | MEDIUM |
| P1-016 | moderate (subprocess) | STILL_REPRODUCES | orchestration | still subprocess+JSON; Phase-2 costs measured | batching + counters | moderate | MEDIUM (tooling) |
| P1-017 | moderate (no paths) | STILL_REPRODUCES | layout naming | no string/path type; fs uses indices | host hex paths | moderate | LOW-MEDIUM |
| P1-018 | moderate (no mmap) | STILL_REPRODUCES | zero-copy | no view/lease types | none (copies fit scale) | moderate | LOW (Phase 3) |
| P1-019 | moderate (grants) | PARTIALLY_RESOLVED | read path | fs enumeration + chunked reads verified | arg-passed bulk verify | minor-moderate | MEDIUM |
| P1-020 | moderate (errors) | STILL_REPRODUCES | failure taxonomy | Result still record-bound; code tables grown | code tables + pins | moderate | MEDIUM |
| P1-021 | moderate+ (obligations) | STILL_REPRODUCES | proofs | CMP301 on every new module | wrapping discipline | moderate+ | MEDIUM |
| P1-B01 | major (C11 diverge) | STILL_REPRODUCES | checked arith | canary still traps on C11 only | wrapping + allowlist | major (backend) | HIGH (correctness) |
| P1-B02 | major (fx bytecode) | REFRAMED (live core) | integrity on backends | matrix declares effects; CG?302-family refusals; still interpreter-only | bytecode-scoped crypto | major (backend) | HIGHEST |

## Resolved since Phase 1a (3)

- **P1-008** — iteration ceilings 32 → 1024/level, 1M envelope. No
  store workaround remains.
- **P1-009** — boolean equality and negation. Store workaround REMOVED
  (`content_less` is boolean logic now).
- **P1-010** — `<` over `byte`. Store workaround REMOVED (same
  function).

Regression evidence: identity corpus (33 cases, incl. ordering) green on
all five backends after the rewrite.

## Partially resolved (7)

P1-001 (append-only writes), P1-003 (fs observation), P1-005 (1024
bound), P1-006 (replace/repeat/concat), P1-013 (source inference only),
P1-019 (fs enumeration + chunked reads). Each appendix records which
half is fixed and which half the store still works around.

## Still live (11)

P1-002, P1-007, P1-011, P1-012, P1-014, P1-015, P1-016, P1-017, P1-018,
P1-020, P1-021, P1-B01 — each re-reproduced or re-confirmed against
890a653/0.13 with current evidence quoted in its appendix. (Count: 12
with P1-B01.)

## Reframed / superseded (2)

- **P1-004 → P2-001**: bound moved from elaboration to realization;
  store answers with chained roots.
- **P1-B02 (reframed, core live)**: from "silent refusal shape-shift" to
  "declared refusal (matrix + CGC301/302), interpreter-only
  realization". Harness guidance changed: key on the CG?302 family, not body shape.

## Newly discovered Phase 2 pressure (8)

| ID | Pressure | Severity | Workaround | Exact or substitute? |
|---|---|---|---|---|
| [P2-001](P2-001-digest-realization-bound-64.md) | 64 B digest realization bound, no streaming | major | chained v2 roots to 992 B | exact to 992 B; substitute past it |
| [P2-002](P2-002-view-invariance-widest-signatures.md) | no view-width subsumption at boundaries | moderate | widest-view signatures + manual length gates | exact verdicts, substitute precision |
| [P2-003](P2-003-no-generic-boundary-functions.md) | no type args on execution requests | moderate | 4 monomorphic tail families + pinned host mirror | exact, substitute surface |
| [P2-004](P2-004-append-only-write-limits.md) | append-only writes (no exclusive/overwrite) | major | host O_EXCL + tmp/replace | substitute (authority outside MNCS) |
| [P2-005](P2-005-no-atomic-publication.md) | no rename/mkdir/delete/sync | major | host publication mechanics | substitute (transitions/decisions exact) |
| [P2-006](P2-006-generation-scan-bound.md) | generation scans bound at 23 records | moderate | host length-gate parity past 23 | exact equality, substitute authority |
| [P2-007](P2-007-observation-granularity.md) | 64 B reads, one root per capability | moderate | arg-passed batch verification | exact verdicts, substitute geometry |
| [P2-008](P2-008-step-and-boundary-costs.md) | step constants + JSON boundary costs | moderate | batching, budgets, counters | measured (no semantic content) |

## Backend-specific pressure

- **P1-B01** (C11 checked-arithmetic trap): unchanged, allowlisted.
- **P1-B02 core**: hash/read/write/fs effects on all four compiled
  backends refused (now declared + coded CGC302). Crypto suites stay
  bytecode-scoped with stale-entry discipline.
- **P1-B02 gap 3 re-confirmed 2026-09-11** (current toolchain,
  d7cc953 + uncommitted): a PURE function (`frame32`) in effect-hosting
  `src/store/chunk.mncs` is refused whole-program on
  `mncs-portable-wasm-mvp` (exit 1, compilation-result shape, CGN302 ×
  20 + CGN301). chunk-v2/manifest-v2 corpora therefore stay
  bytecode-scoped; no store-side fix exists — per-function admission is
  the language change.
- Unmeasured: step constants on compiled backends (P2-008 is
  bytecode-measured); riscv32/ebpf/ptx64 never executed here.

## Performance / tooling pressure

P2-008 consolidates the measurements: 1024-scans ~9–34k steps by shape,
chain depth = invocations (~10 per 40 B put), 25x JSON expansion,
seconds-per-invocation compile dominance. No optimization was applied
that trades correctness; the numbers are reported, not tuned.

## What the store proved anyway (implementation headline)

- General blobs 0..992 B: partition → frames → digests → chained v2
  root → publish → close → reopen → verify → typed read, all semantic
  bytes and all commit/recovery/reclamation decisions MNCS-produced.
- Ordering, dedup/sharing, missing/reordered/corrupt/torn rejection all
  fail closed with distinct codes.
- Generations with CAS conflicts (typed tokens), stable snapshots,
  explicit commit state machine, deterministic recovery (STAY/PROMOTE/
  REFUSE), and sharing-aware reclamation — under 7-boundary fault
  injection with the old-or-new invariant holding throughout.

## Recommended next mncs-language repair order (leverage-ranked)

1. **Effect realization on compiled backends + declared matrix**
   (P1-B02 core): unblocks cross-backend integrity agreement — the
   mechanism that would catch the next P1-B01-class bug in hash paths.
   Single highest-leverage tranche; fold the CGC302 machine-readable
   refusal in as the contract.
2. **File effects + sync barrier + atomic rename as one tranche**
   (P1-001 remainder + P1-002 + P2-004 + P2-005): moves the entire
   lifecycle (staging, exclusive chunks, publication, durability) into
   the language; removes the largest host scaffolding block (~40% of
   the driver) and makes crash-consistency arguments statable.
3. **Streaming/incremental hash + wider views together** (P2-001 +
   P2-006 + P2-007): one without the other strands large values again;
   ship as a pair to unlock arbitrary sizes and wide generation scans.
4. **Boundary values** (P1-013 host type args + P1-014 stable records +
   P1-020 typed errors + P1-012 subsumption): collapses the boundary
   boilerplate (projector families, widest-view signatures, code
   tables) and makes the API nominal end-to-end.
5. **Checked-arithmetic uniformity** (P1-B01 fix + P1-021 discharge):
   removes the only backend divergence and the permanent proof debt.
6. **Tooling scale** (P1-016 in-process calls + P2-008 transport):
   test-time cost only, but it dominates every suite run and dictates
   batching architecture.
7. **Ergonomics sweep** (P1-007 multi-result, P1-011 literal sides,
   P1-015 issuance, P1-017 paths): small, independent, good first
   issues once the core lands.
8. **Phase-3 views** (P1-018): correctly sequenced last.
