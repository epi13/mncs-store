# Test infrastructure recovery — workstream A

The unchanged 23-size general-blob boundary test completes in **16.64 s with a cold artifact cache** and **13.42 s with a warm artifact cache**. It executes all 1,734 semantic batches and 4,330 cases. The complete Store suite completed: **292 passed, no skips, in 221.88 s**. The three touched Language crates passed **392 tests**.

This workstream changes verification transport and immutable preparation. It does not replace the research-bytecode oracle, change Store semantics, reduce vectors, skip tests, or increase correctness timeouts.

## Recovery and verified cause

At entry, no interrupted pytest or compiler child remained running. The reported task-1805 history (36,120 seconds, 226/286 completed, boundary test still running) could not be independently retrieved from the connected terminal history. Those historical numbers remain user-reported. A bounded reproduction verified the pathological execution path without repeating that run.

`tests/mncs_exec.py` selected the debug compiler by default. Each `Engine.run` batch launched `mncs experiment run`, which repeated frontend/compiler work, layer preparation, experiment construction and large JSON serialization. The frozen Phase-2 driver deliberately verifies put/get/reopen/recovery repeatedly and advances a manifest chain in sequential batches. These integrity checks are useful; recompiling their unchanged modules at every batch is unnecessary.

The boundary workload makes 1,734 batches against six source artifacts. A single manifest projector executes only 22 backend steps, yet the old CLI path emits 22,355,782 JSON bytes. Its research payload alone contains 22,020,610 hexadecimal characters. The identity sample emits 2,876,170 bytes. This data was generated and decoded even though the lifecycle driver needed case observations.

One instrumented release manifest call spent approximately:

| Stage | Exclusive time |
| --- | ---: |
| Compiler pipeline | 1,180 ms |
| Experiment definition | 52 ms |
| Body/SSA/backend validation, including preparation | 923 ms |
| Backend case observation, including preparation | 347 ms |
| Stateful preparation with no stateful cases | 344 ms |
| Properties | 0 ms |

Frontend preparation and final formatting also occur outside these stage totals. Process startup and the tiny kernel are not the dominant observed costs. No defective blob-size algorithm was established: retaining preparation executes the complete original loops in seconds.

## Controlled measurements

Values are observed, not projected full-suite speedup estimates. Timed-out baselines are **UNKNOWN/incomplete**. Counts in interrupted runs are attempted batches/cases and do not imply completed verification.

| Identical workload / path | Wall | User + system CPU | Compiler launches | Result |
| --- | ---: | ---: | ---: | --- |
| Original boundary test, debug CLI, 60 s budget | 60.13 s | 59.93 s | 4 | UNKNOWN, interrupted during batch 4 |
| Original boundary test, retained reference, artifact miss | 16.64 s | 16.65 s | 6 | Complete; 1,734 batches / 4,330 cases |
| Original boundary test, retained reference, artifact hit | 13.42 s | 13.38 s | 0 | Complete; six admitted sessions |
| Empty-object put/get/reopen/verify/get, release CLI | 90.14 s | 89.78 s | 42 | UNKNOWN at diagnostic budget |
| Same empty-object workload, retained reference | 2.392 s | 2.490 s | 0 | Complete; 55 batches / 60 cases |
| 992-byte put/get/reopen/verify/get, release CLI | 45.13 s | 45.05 s | 17 | UNKNOWN at diagnostic budget |
| Same 992-byte workload, retained reference | 3.739 s | 3.806 s | 0 | Complete; 237 batches / 662 cases |
| Three identical manifest calls, release CLI | 10.221 s | 10.145 s | 3 | Identical returned values and 22-step observations |
| Same three calls, retained reference | 1.043 s | 1.013 s | 0 | Artifact hit; one session admission |

The retained repeated-call sequence took 0.972 s for process/session/reference admission, then 3.79 ms and 3.59 ms for the next calls. All three returned the same value and step count as the CLI. The sampled aggregate process-tree RSS peak was **260,740 KiB** for CLI and **133,904 KiB** for retained execution (20 ms sampling; summed RSS can double-count shared pages).

Standard `time -v` maximum individual-process RSS was 252,468 KiB for the cold boundary workload and 247,724 KiB warm. Full-suite peak was 758,592 KiB; user/system CPU was 206.82/9.53 s and command wall time 222.24 s. These are not claims of a smaller full-suite peak than the historical interrupted run, whose memory history was unavailable.

The initial existing-binary comparison was:

| Module | Debug wall / peak RSS | Release wall / peak RSS | JSON bytes |
| --- | --- | --- | ---: |
| Identity | 5.124 s / 58,188 KiB | 0.415 s / 42,252 KiB | 2,876,170 |
| Manifest | 41.801 s / 236,572 KiB | 3.421 s / 220,032 KiB | 22,355,782 |

Those pre-existing debug/release builds emitted exactly identical artifacts and case observations for both samples. Their precise source-revision equivalence was not recoverable, so this is an existing-executable comparison, not a strictly isolated compiler-profile experiment. Binary SHA-256 values are recorded. The controlled repeated-call comparison uses one freshly built release executable for both transports.

**Debug selection contributed substantially, but release selection alone did not resolve the structural defect.** The release empty-object baseline still exhausted 90 seconds. The retained path removes repeated work while preserving the reference backend.

“Cold” means no compiled artifact for the selected admission key; “warm” means an existing verified artifact in the owner cache, in a fresh process. System-wide filesystem caches were not flushed because other agents/services share the machine.

## Implementation and authority

Language commit: `f8742e9` (`feat(embed): retain independently admitted reference verification sessions`).

- `BodyExecutionSession::from_shared` retains an immutable program with its validated facts.
- The existing owned embedding session admits independent `CompilationEvidenceBundle` input/intermediate/backend references, then retains separate body, SSA-comparison, backend-comparison and real-execution runtime contexts. Immutable program/SSA allocations are shared.
- Additive Rust/C ABI operations bind reference evidence and compare each new stateless corpus. A coherent, correctly sealed backend payload cannot serve as its own oracle: its semantic/SSA roots must match independently emitted compiler references. Missing admission, invalid evidence and unsupported reference preparation fail explicitly.
- The ordinary fresh-layer comparator and retained path share the same agreement implementation. Every new request still checks targets, arguments, budgets, grants and observations. Mutating effects are observed during validation and realized only by the explicit call.

Store transport:

- Prefer an available release compiler, respecting explicit toolchain overrides and debug fallback.
- Reuse `tests/retained_session.py`, already used by native substrate tests. Concrete research lifecycle batches automatically use admitted reference sessions when supported. The explicit `MNCS_TEST_TRANSPORT=cli` path remains available, and older libraries retain full CLI reference execution.
- Compile with existing `backend,evidence` emissions, `--quiet` and `--omit-result`. Avoid constructing a complete experiment result only to discard it. No result values are cached.
- Reuse the existing bounded Store artifact cache and its source/compiler/library/profile admission keys, checksum checks, leases, deduplication and retention policy. Independent compiler evidence is carried in the existing cache manifest. Malformed/corrupt/stale admission metadata triggers one rebuild. Oversized metadata remains executable in memory and is not published as an unreadable cache entry.
- Retain at most eight source sessions and close them at process exit. Recheck source/library/compiler metadata, including additions/removals, and rehash/re-admit on changes. Reject changes observed during admission.
- Route the Phase-2 explicit-budget path through the same engine without altering its per-call budgets. Preserve legacy case IDs and nullable failure reasons; retain executed-artifact provenance on native outputs.
- Keep full `run_corpus` experiment results, expectations/properties, backend/refusal tests and source-study consumers on their existing interfaces.
- Apply timeout/interrupt cleanup to the compiler's whole process group, with TERM then a bounded KILL fallback. Keep the existing 280 s per-call timeout and semantic step budgets.
- Standard pytest configuration resolves `python` and `tests`. Provider fixtures use the existing selected Language root rather than treating a linked worktree's parent as the family workspace.

No canonical Store data, transaction/recovery semantics, permission policy, session snapshots, generation authority or Store indexing contract changed. The one production Python change enforces the existing compiled-cache manifest read bound at write time. No Store migration/reclamation or competing test framework was introduced.

## Verification actually executed

- Complete Store suite: **292 passed**, 221.88 s, no skips. This retains all 286 original tests plus six regression tests.
- A 113-test lifecycle/boundary/embedded sample passed before broad execution. An initial full run found two linked-worktree provider-fixture failures; the fixture addressing was corrected, all 10 provider tests passed, and full regression was repeated successfully.
- Touched Language crates (`mncs-model`, `mncs-codegen`, `mncs-embed`): **392 passed**, including ABI, process lifecycle, task scope, JIT and projection integration tests. The final Cargo validation took 109.74 s including recompilation; individual test executable durations are in the log.
- Full-CLI/native equivalence on fresh inputs and budgets, including exhausted budgets; source invalidation; corrupted backend, reference evidence and malformed cache metadata; explicit grant changes; timeout and SIGINT cleanup with a TERM-ignoring grandchild. The post-suite legacy-field compatibility check passed all five transport tests.
- A structurally valid compiler-evidence mutation with a coherent wrong backend payload is rejected by reference admission. A real filesystem-write test verifies that repeated reference comparison does not write, while each explicit execution appends exactly once.
- Five linked-module CLI conformance cases returned their expected values, with layered validation PASS. The experiment remains **UNKNOWN** for retained unresolved compilation obligations. It is not represented as a whole-experiment PASS.
- Existing pure corpus vectors executed across research bytecode, WASM, C11, LLVM and Cranelift. Existing effect-refusal checks passed. Compiled targets were not substituted for the required SHA-256 reference oracle.

The prior full-suite PASS was never extrapolated from targeted tests: a complete run actually finished. Broader Language workspace crates were not claimed as tested.

## Existing orchestration and remaining costs

`mncs test` already compiles discovered native tests once and batches through embed. `mncs impact`, native inventory/identity selection and the existing MNCS Test verification plan provide incremental verification authority. Forge/Control/Fabric own declared execution, resource and scheduling responsibilities. This work reuses the embedding boundary; it installs no scheduler, test wrapper, receipt service or duplicate manifest family. The checked-in measurement scripts are optional reproductions, not part of routine test entry.

The frozen Python filesystem/fault-injection oracle has a useful integration role. Rewriting it into MNCS would expand scope without addressing the measured preparation bottleneck. Faster compiled backends help suitable pure workloads; current effect-bearing reference checks keep their existing role and refusals.

Remaining measured costs include the full experiment backend matrix (cross-backend agreement ~29.6 s, generation WASM ~10.8 s) and repeated native application admissions in individual native-substrate tests (~6–10 s). The full CLI also eagerly prepares its stateful session for empty stateful corpora (~344 ms in the manifest sample). These remain bounded follow-up opportunities; no general compiler/testing redesign was attempted.

## Reproduction and delivery

From the corresponding Language source, build the paired tools with:

```sh
cargo build --release -p mncs-cli -p mncs-embed
```

From Store:

```sh
python3 -m pytest tests -q --durations=12
python3 -m pytest tests/test_reference_transport.py -q
python3 outputs/test-throughput-20261114/measure.py blob /tmp/blob-measurement.json
MNCS_TEST_TRANSPORT=cli timeout -s INT -k 10s 60s \
  python3 outputs/test-throughput-20261114/measure.py blob /tmp/blob-cli-incomplete.json
```

An aggregate timeout is a diagnostic safeguard, not a correctness threshold; an interrupted run remains incomplete. Measurement scripts assume the repository root as cwd. `probe.py` and `sample_tree.py` retain the original task-temporary output location; create that directory or adjust their `OUT`/`out` variable when reproducing. They are not installed interfaces.

[Measurements](measurements.json), [full Store result](store-verified.log), [Language result](rust-verified.log), [stage timings](manifest-stages.log), and resource records are alongside this report. Source/proof artifacts remain accessible through their owning compiler/cache interfaces; multi-megabyte compiler dumps are not copied into this delivery.

Main-branch publication and canonical-checkout activation are recorded in `delivery.json` after integration. Existing long-lived processes keep their loaded libraries until normal restart; new test processes use the paired rebuilt release tools immediately. MCP latency recovery is a separate workstream and is not claimed complete by this report. Store indexing has not been modified.
