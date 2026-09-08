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
| `descriptor-corpus.json` | `src/store/descriptor.mncs` | 49 |
| `chunk-corpus.json` (+ `--grant-crypto store_chunk`) | `src/store/chunk.mncs` | 58 |
| `manifest-corpus.json` (+ `--grant-crypto store_manifest`) | `src/store/manifest.mncs` | 27 |
| `canary-corpus.json` | `tests/fixtures/checked_arith_canary.mncs` | 2 |

Plus elaboration checks (all modules clean), the `type_confusion.mncs`
negative fixture (must fail with MNE133), cross-backend agreement over
the universally-executed suites, repeat-run determinism, and the
`effects-probe` suite pinning per-backend host-effect support (P1-B02).

Backend support matrix (measured 2026-09-08, x86_64 Linux):

| Suite | research-bytecode | wasm / c11 / llvm / cranelift |
|---|---|---|
| identity, descriptor, canary (pure) | pass | pass (c11 canary-max allowlisted, P1-B01) |
| chunk, manifest (sha256 effects) | pass | refused (P1-B02), suites scoped to bytecode |
| effects-probe (sha256 + host_read) | pass (3/3) | refused in documented shape (P1-B02) |

`chunk-corpus.json` and `manifest-corpus.json` need `--grant-crypto`
because digest/root-identity functions execute the `sha256_digest`
effect; digests are cross-checked against hashlib oracles.

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

## Toolchain

Set `MNCS_BIN` (default: the mncs-language debug CLI) and
`MNCS_BACKENDS` (default: all five executable backends) to adapt to an
environment. `MNCS_LIBRARY_PATH` is managed by the harness — do not
override it when running these tests.
