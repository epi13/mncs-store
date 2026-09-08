# P1-016 — No in-process call boundary: every MNCS invocation is a subprocess + JSON round trip

Storage feature being implemented: the typed put/get path and all
verification (test/lifecycle orchestration cost).

Observed language/runtime/compiler behavior: the only way for host
tooling to execute an MNCS function is the `mncs experiment run`
subprocess (full compile + realize + execute per invocation) with JSON
corpora in and JSON results out. There is no embedded/FFI API, no
persistent compiler session, no binary value ABI callable from a host
process. Every byte crossing the boundary is decimal-encoded in JSON
(`{"byte": {"value": 12}}` ≈ 25 bytes on the wire per stored byte).

Minimal reproducer: time N single-case invocations vs one N-case batch —
per-invocation cost is dominated by process start + compilation (seconds),
not execution (milliseconds). The Phase-1a driver batches aggressively
for exactly this reason (see measurements below).

Required semantics: an in-process execution handle (load module once,
invoke functions with binary buffers, receive binary results) so storage
orchestration does not pay compile + JSON costs per operation.

Why the current behavior/API is insufficient: it dictates the driver's
batching architecture (6 batched invocations per put-phase regardless of
object count; 4 per verify-phase) and makes interactive/small-operation
use absurdly expensive. It also forces the JSON transport, which is fine
for tests but would be unacceptable as a storage data path (see
invariants: JSON must not become canonical — here it is transport
overhead, kept strictly outside canonical state).

Safety/correctness implications: LOW (transport, not semantics). One
real hazard: JSON number precision for u64 maxima (handled correctly —
`18446744073709551615` round-trips exactly in our corpora — but any host
JSON layer that parses into float64 would corrupt identities silently;
harness authors beware).

Performance implications: HIGH (dominant cost of the whole test suite).
Measured on this host (x86_64 Linux, debug CLI build): a single batched
invocation costs ~4–30 s wall (compile-dominated; C11/LLVM slower),
while the MNCS execution inside is millisecond-scale (e.g. 129 steps for
a 12-byte round trip). Per-byte JSON expansion ≈ 25x. A full lifecycle
test performs ~15–40 invocations; the suite's runtime is ~95% subprocess
and JSON overhead. Per-operation latency without batching would be
minutes per object — batching is not an optimization here, it is what
makes the suite runnable at all.

Workaround used: maximal batching (one corpus per operation-kind per
test phase), `Engine` invocation/byte counters for honest reporting,
`MNCS_BACKENDS` narrowing for iteration speed.

What the language/stdlib/runtime should ideally provide: a stable C ABI
(or equivalent) for load-once/call-many execution with binary
(cell/descriptor) arguments, plus a persistent daemon/session mode for
the CLI.

Affected backend(s) / target(s), if known: all (tooling-shape absence).

Severity: moderate (test-time only; not a storage-semantics gap)

Status: workaround (batched subprocess orchestration + instrumentation)
