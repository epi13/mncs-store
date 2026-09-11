# Test strategy (Phase 1a: executable)

Two layers, matching the host/MNCS responsibility split (RFC 0016):

## 1. MNCS semantic corpora (`tests/corpora/*.json`)

Static, reviewable vectors with independent oracle values (big-endian
assembly + hashlib in the authoring step, never copied from the `.mncs`
sources). Run via `tests/test_mncs_corpora.py` across all executable
backends (`MNCS_BACKENDS` narrows the matrix).

| Corpus | Module under test | Cases |
|---|---|---|
| `identity-corpus.json` | `src/store/identity.mncs` | 33 |
| `descriptor-corpus.json` | `src/store/descriptor.mncs` | 57 (+8 v2) |
| `chunk-corpus.json` (+ `--grant-crypto store_chunk`) | `src/store/chunk.mncs` | 58 |
| `chunk-v2-corpus.json` (+ `--grant-crypto store_chunk`) | `src/store/chunk.mncs` | 39 (concat, tails, widths, partitions) |
| `manifest-corpus.json` (+ `--grant-crypto store_manifest`) | `src/store/manifest.mncs` | 32 (+5 chain) |
| `manifest-v2-corpus.json` (+ `--grant-crypto store_manifest`) | `src/store/manifest.mncs` | 28 (header2, counts, validate_v2, projectors) |
| `generation-corpus.json` | `src/store/generation.mncs` | 39 (headers, CAS, tokens, snapshots, commit table, scans) |
| `recovery-corpus.json` | `src/store/recovery.mncs` | 17 (classify, recover, prune) |
| `canary-corpus.json` | `tests/fixtures/checked_arith_canary.mncs` | 2 |

Plus elaboration checks (all modules clean), the `type_confusion.mncs`
negative fixture (must fail with MNE133), cross-backend agreement over
the universally-executed suites, repeat-run determinism, and the
`effects-probe` suite pinning per-backend host-effect support (P1-B02).

Backend support matrix (re-measured 2026-09-10, x86_64 Linux,
compiler 890a653, Profile 0.13):

| Suite | research-bytecode | wasm / c11 / llvm / cranelift |
|---|---|---|
| identity, descriptor (+v2), generation, recovery, canary (pure) | pass | pass (c11 canary-max allowlisted, P1-B01) |
| chunk, manifest, chunk-v2, manifest-v2 (sha256 effects in-module) | pass | refused (P1-B02 whole-program refusal), suites scoped to bytecode |
| effects-probe (sha256 + host_read) | pass (3/3) | refused with CG?302-family diagnostics (P1-B02 reframe, pinned) |

`chunk-corpus.json`, `chunk-v2-corpus.json`, `manifest-corpus.json`,
and `manifest-v2-corpus.json` need `--grant-crypto` because their
modules host digest functions executing the `sha256_digest` effect —
and refusal is whole-program, so even the pure cases in those files are
bytecode-scoped. Digests are cross-checked against hashlib oracles.
Pure-only modules (identity, descriptor, generation, recovery) run on
all five backends, including the 1024-byte `table_contains` scans
(explicit 64k step budget). The 1020-byte v2 manifest vectors live in
the bytecode-scoped manifest-v2 suite (whole-program refusal, above).

## 2. Lifecycle tests (`tests/test_lifecycle.py`)

Real on-disk stores under `tmp_path` driven by `tests/store_phase1a.py`
(host transport + layout + atomicity; every semantic byte MNCS-produced):

- typed put → close → reopen → typed get (u32/u64/pair/blobs/empty);
- content dedup without identity merge; chunk immutability enforcement;
- corruption: payload-bit flips, length-field flips, magic flips,
  root-digest flips, truncation, missing files (integrity vs not-found
  kept distinct), manifest structural attacks;
- type-mismatch rejection; store version/magic gates;
- determinism across independent stores (byte-identical files);
- in-language chunk-file verification via `store.read_verify`
  (skips honestly if combined read+crypto grants are refused).

## 3. Phase-2 tests (`tests/test_phase2.py`)

Real on-disk stores driven by `tests/store_phase2.py` (host transport
only; every semantic byte AND every commit/recovery/reclamation
decision is MNCS-produced):

- general blobs 0..992 B (23 boundary sizes), close/reopen round trips,
  dedup/sharing without identity merge, v1+v2 coexistence;
- ordered-manifest binding: reorder/corrupt/missing/torn/padding attacks;
- generations with CAS conflicts (typed tokens), stable snapshots,
  commit-trace assertions;
- fault injection at 7 commit boundaries with old-or-new recovery
  assertions (STAY/PROMOTE/REFUSE decided by MNCS `recover_decide`);
- sharing-aware reclamation with snapshot-pin protection and staged-temp
  pruning.

## Toolchain

Set `MNCS_BIN` (default: the mncs-language debug CLI) and
`MNCS_BACKENDS` (default: all five executable backends) to adapt to an
environment. `MNCS_LIBRARY_PATH` is managed by the harness — do not
override it when running these tests.
