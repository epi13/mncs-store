# Test strategy

Executable since Phase 1a. The suite prioritizes semantic invariants over
API snapshots.

Phase-1a coverage (all passing; see `corpora/README.md` for the semantic
layer) plus Phase-2 generations/recovery (2026-09-10):

- deterministic content/descriptor encoding (172 corpus cases + repeat-run
  determinism + cross-backend agreement);
- multi-chunk / generation / recovery semantics (136 new corpus cases:
  chunk-v2, manifest-v2 + chain, descriptor-v2 additions, generation,
  recovery; pure suites on all backends, hash suites bytecode-scoped);
- historical general-blob vectors 0..992 B across 23 boundary sizes
  (the frozen differential oracle, not the supported consumer path);
- supported bounded content-tree geometry, close/reopen round trips,
  dedup/sharing without identity merge, and a 4 MB admission geometry
  proof;
- ordered-manifest binding (reorder/corrupt/missing/torn/padding attacks
  all fail closed with distinct codes);
- generations with CAS conflicts (typed tokens), stable snapshots across
  publication, commit-trace assertions, v1+v2 coexistence;
- fault injection at 7 commit boundaries with old-or-new recovery
  assertions; torn/missing classification; staged-temp reclamation;
- logical identity versus content identity (width + nominal types,
  negative elaboration fixture, dedup-without-merge lifecycle test);
- typed put/get and process reopen round trips (u32/u64/pair/blobs/empty,
  close + fresh-instance reopen);
- chunk reuse and structural sharing (identical re-put shares one chunk
  file; distinct logical IDs);
- corrupted chunk/manifest rejection (15 bit-flip/truncation/missing
  cases; integrity vs not-found kept distinct);
- torn commit and restart fault injection (truncated chunk/manifest files,
  missing committed files, serial healing on open);
- store version/magic gates (unknown versions fail explicitly).

Deferred to later phases (tracked, not silently dropped):

- capability enforcement beyond test-grant boundaries (needs language
  capability maturation);
- direct-view eligibility and unsafe-layout rejection (Phase 3);
- stale/destroyed index behavior (Phase 4);
- replication integrity and heterogeneous representation compatibility
  (Phase 5).

Harness layers:

- `test_mncs_corpora.py` — checked-in semantic vectors across all five
  executable backends (plus elaboration, negative, agreement,
  determinism, and CG?302 refusal-shape checks).
- `test_lifecycle.py` — real on-disk stores via `store_phase1a.py`.
- `test_phase2.py` — multi-chunk/generation/snapshot/recovery/fault
  tests via `store_phase2.py`.
- `mncs_exec.py` — bounded transport + value parsing (no semantics).
- `store_phase1a.py` — host transport/layout/atomicity driver (every
  semantic byte MNCS-produced; see its scope contract).
- `store_phase2.py` — Phase-2 driver: transport + orchestration only;
  every semantic byte AND every commit/recovery/reclamation decision is
  MNCS-produced (see its scope contract).

Fixture formats such as JSON may be used to drive tests, but passing a JSON round trip is not proof of the machine-native storage model.
JSON here is harness transport only — never canonical state.

## Reference execution and throughput

The harness respects `MNCS_BIN`; otherwise it selects the built release
compiler before debug. Build the matching compiler and embedding library once
in the Language checkout with `cargo build --release -p mncs-cli -p mncs-embed`.
`MNCS_LANGUAGE_ROOT` and `MNCS_EMBED_LIB` retain their explicit override roles.
Run from this repository with `python3 -m pytest tests -q --durations=12`.

Concrete research-bytecode lifecycle batches use the existing
`retained_session.py` transport when the library exposes reference comparison.
Each new batch compares body, SSA and bytecode observations, then executes its
calls under their explicit grants and budgets. Only immutable preparation is
reused; decisions, returned values and verification results are never cached.
Admission checks the payload against the compiler's independently emitted
semantic/SSA/backend evidence. That existing evidence bundle travels in the
existing cache manifest beside the source/compiler admission facts; an intact
backend payload alone cannot replace the reference oracle. Invalid cached
evidence triggers one rebuild from the selected source and compiler.
Up to eight source sessions are retained and closed at process exit. Source,
library, compiler and profile changes invalidate admission. Compiled artifacts
use the existing bounded Store artifact cache, with checksum/identity checks.

Full experiment corpus tests (including properties, expectations, refusal
shapes and backend comparison) still use `experiment run`. Older embedding
libraries fall back to that full reference path. `MNCS_TEST_TRANSPORT=cli`
explicitly selects it for lifecycle comparisons as well; other backends retain
their existing transport. A missing reference layer never becomes a passing
backend-only check.

For diagnosis, first sample individual cases and use an aggregate budget, e.g.
`timeout -s INT -k 10s 900s python3 -m pytest tests -vv --durations=12`.
This is a diagnostic budget, not a correctness threshold. Compiler calls have
their existing per-call timeout, now applied to the full subprocess group on
timeout or interruption. A cancelled/incomplete run supplies no suite PASS.
Native calls retain semantic step budgets; the aggregate process budget also
bounds infrastructure failures. No separate scheduler or receipt layer is
introduced. Native MNCS tests already compile once and batch through embed;
this frozen host filesystem/fault-injection oracle retains its distinct role.
