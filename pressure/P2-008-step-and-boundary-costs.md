# P2-008 — Step-cost and boundary-crossing pressure at store scale

New Phase-2 pressure (measured 2026-09-10; performance evidence, not a
correctness gap).

Storage feature being implemented: reclamation scans, chain
computation, and suite runtime for 0..992-byte objects (ROADMAP Phase 2
exit proof at bounded scale).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13, research-bytecode, x86_64 Linux, debug CLI):

- A 1024-iteration loop with a scalar body costs ~9.2k steps (~9/iter);
  carrying a `replace` per step costs ~11.3k steps (~11/iter).
- Calling a 32-iteration callee (`content_equal`) once per table byte
  costs > 250 steps/byte: the full scan exhausts even a 262144-step
  budget. Restructured to an incremental mismatch latch
  (`store.generation.scan_step`), the scan costs 33808 steps and is
  CORRECT — just above the harness's 32768 default, so reclamation
  passes an explicit 65536 budget. The per-step helper call dominates;
  `&&` does not short-circuit callees (an all-inactive scan costs the
  same as a full scan).
- Chain depth scales invocations: a 40-byte put plan needs ~10 batched
  CLI invocations (descriptors, frames, tails, digests, oids, headers,
  init, depth steps, preview, commit transitions); a 31-chunk object
  needs ~40.
- Boundary transport: a 1024-byte `table_contains` argument is ~50 KB
  of JSON (~25x expansion per stored byte, P1-016 pattern); corpus
  files carrying such cases grow ~50 KB per case.
- Single-case CLI invocations cost seconds (compile-dominated); the
  suite stays runnable only through maximal batching (one corpus per
  operation-kind per phase).

Minimal reproducer: `table_contains` over a 1024-byte table at the
default 32768-step budget -> budget_exhausted at 32768 steps; returns
the correct verdict at 65536 (33808 steps used). Bisectors in
`probe.cost` (`plain1024` 9223 steps, `replace1024` 11272 steps)
isolate the callee-call constant.

Required semantics (tooling/runtime, not storage): cheaper calls or an
in-process execution boundary (P1-016), short-circuiting `&&` over
effect-free callees, and binary (non-JSON) value transport — any one of
which collapses the constant by an order of magnitude.

Why the current behavior/API is insufficient: nothing is incorrect, but
the constants dictate driver architecture (depth-batching, explicit
budgets, 50 KB corpus cases) and bound how far bounded verification
scales before invocations dominate. If the language forces an obviously
pathological amount of copying or boundary overhead at production
granularity, that cost should be visible here first.

Safety/correctness implications: none (budgets are host execution
parameters; the step model accounts every operation exactly).

Performance implications: suite runtime is ~95% subprocess + JSON
overhead (unchanged from Phase 1a); per-object put latency is ~10
invocations; reclamation scans are 34k steps per 32-entry window.

Workaround used: maximal batching, explicit scan budgets, Engine
invocation/byte counters for honest reporting, `MNCS_BACKENDS`
narrowing for iteration speed. No semantic content.

What the language/stdlib/runtime should ideally provide: a stable C ABI
or session mode for load-once/call-many execution with binary
arguments (P1-016), plus documented step-cost guidance so call-heavy
scans can be budgeted without bisection.

Affected backend(s): all (tooling-shape absence; step constants are
bytecode-measured — compiled-backend constants unmeasured, P1-B02).

Severity: moderate (test-time only).

Status: measured (batching + counters; no semantic workaround needed)

Acceptance test: the same `table_contains` scan completes within the
default budget after a call-cost improvement, or an in-process boundary
runs the Phase-2 suite with invocation overhead excluded from the
report.
