# P3-001 — Bulk codec-window and canonical-compute pressure at store scale

New Phase-3 pressure (measured 2026-09-30; performance evidence, not a
correctness gap).

Storage feature being implemented: adaptive physical representations
(RFC 0019) — windowed RLE encode/decode over 64-byte windows and
canonical sorting of 256-byte fixed-record tables, all decided in
pure MNCS through the retained session.

Observed language/runtime/compiler behavior (mncs-language 425de20,
Source Profile 0.13, research-bytecode, x86_64 Linux, release CLI):

- RLE encode of one 64-byte window costs 0.8k-7.4k steps depending on
  shape (worst: all-distinct bytes, 7357 steps); decode costs ~1k
  steps. Both fit the 32768 default comfortably — per-window compute
  is NOT the pressure.
- Representation selection (`rank`) costs ~5k steps; block-table
  validation over 2 slots ~7k steps; plan block admission ~27k
  steps. All fit the default budget.
- Canonical sort over a 3-row table costs ~34k steps and canonical
  equality ~70k steps — just above the 32768 default, so callers
  pass explicit budgets (262144/524288). A full 16-row table sorts
  near ~130k steps by the same scaling. Correct, but the
  per-step-helper-call constant (P2-008) dominates: each insertion
  rank probes rows through non-short-circuiting `&&` callees.
- Boundary transport dominates codec throughput: each 64-byte window
  crosses the retained ABI as JSON twice at encode (bytes + length)
  and once at decode. A 512 KiB payload needs 8192 windows, i.e.
  ~16k calls at write (encode + admission re-decode) and ~8k at
  coded read. Batching (4096 requests/crossing) keeps this runnable
  but the JSON expansion per window (~20x, P1-016 pattern) is the
  throughput ceiling for coded representations at store scale.
- Single-case CLI invocations remain seconds (compile-dominated);
  the adaptive semantic suite batches one corpus per module and the
  retained session compiles the Store artifact once per process.
- Step budgets are backend-relative: the 256-byte envelope roundtrip
  fits the 32768 default on research-bytecode but exhausts it on
  portable-wasm-mvp (values agree; the wasm run needs 131072), and
  the 3-row canonical sort needs 262144 on research but 1048576 on
  wasm. Cross-backend suites must budget per backend, not per case.

Minimal reproducer: `rle_encode_block` + `rle_encode_block_len` over
one all-distinct 64-byte window -> 7357 + 6692 steps, correct;
`canonical_equal` over two 3-byte tables -> 70034 steps, correct.
Scale the window count and watch boundary bytes, not steps.

Required semantics (tooling/runtime, not storage): a binary (non-JSON)
window transport for bulk byte calls, or an in-process execution
boundary (P1-016), either of which collapses coded-representation
throughput by an order of magnitude; short-circuiting `&&` over
effect-free callees would halve canonical-sort step costs.

Why the current behavior/API is insufficient: nothing is incorrect,
and V1 fits every budget with room to spare for metadata decisions.
But coded payloads at realistic sizes (hundreds of KiB) spend their
time in JSON transport, not in MNCS compute — the cost that will
decide whether future codecs (windowed LZ, columnar schemes) are
practical in pure MNCS or need a substrate transport first.

Workaround in use: maximal batching (one retained call_batch per
window-kind per payload), window independence (no cross-window
state, so batching is sound), and explicit step budgets for
canonical entry points. No host reimplementation of codec semantics.
