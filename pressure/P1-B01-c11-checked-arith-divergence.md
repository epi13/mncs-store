# P1-B01 — C11 backend traps on large-but-valid u32 checked products (backend divergence)

Storage feature being implemented: big-endian u32 decoders
(`object_namespace` pre-fix formulation; canary
`decode_u32_plain(255,255,255,254)`).

Observed language/runtime/compiler behavior: the plain-checked form

```mncs
let high: u32 = ((b0 as u32) * 16777216) + ((b1 as u32) * 65536) + ((b2 as u32) * 256);
return high + (b3 as u32);
```

returns exact values on research-bytecode, portable-WASM, LLVM-IR, and
Cranelift for ALL inputs including `(255,255,255,254)` → `4294967294`,
but the C11 backend reports `runtime_failure` for any input whose
intermediate products exceed INT32_MAX — including `(255,255,255,254)`
and even namespace-only decodes — while small inputs (e.g. `(0,0,0,7)`)
pass. u64 paths using explicit wrapping operators (`*%`/`+%`) pass on C11,
and pure u32 literal comparison (`x == 4294967295`) passes, isolating the
fault to checked `*`/`+` lowering with large-but-valid u32 products
(consistent with a SIGNED-32 overflow check applied to UNSIGNED-32
operands, i.e. a false-positive trap at 255×16777216 = 4278190080).

Minimal reproducer: `tests/fixtures/checked_arith_canary.mncs` +
`tests/corpora/canary-corpus.json` (`decode-plain-max` case). Run on
`mncs-c11` vs any other executable backend.

Required semantics: backend-uniform checked arithmetic — identical inputs
produce identical values-or-traps on every backend, per the declared
fallback of the obligation system.

Why the current behavior/API is insufficient: a storage decoder that is
correct on four backends corrupts (traps) on the fifth for common inputs
(any namespace/object with high bytes set — i.e. most real identities).
Checked-arithmetic portability is load-bearing for a store that must read
adversarial bytes deterministically everywhere.

Safety/correctness implications: HIGH for affected configurations.
Fail-closed (trap) rather than wrong-value, but a trap on valid data is a
correctness failure for a decoder, and the divergence is SILENT (no
diagnostic names the lowering difference).

Performance implications: none (correctness issue).

Workaround used: store decoders use wrapping operators where the true
value provably fits u32 (exact, not approximate); the canary stays in the
suite with a `KNOWN_DIVERGENCES` allowlist entry so the divergence is
pinned, not hidden — if C11 starts passing, the suite fails loudly
demanding allowlist removal.

What the language/stdlib/runtime should ideally provide: fix the C11
unsigned-checked lowering (or declare the divergence + refuse the
affected shapes at compile time instead of trapping at runtime), plus a
cross-backend checked-arithmetic conformance corpus covering u32/u64
boundary products.

Affected backend(s) / target(s), if known: `mncs-c11` only (of the five
executable backends, on x86_64 Linux, clang toolchain).

Severity: major (backend-specific correctness divergence)

Status: workaround (wrapping discipline) + pinned (canary + allowlist)

## Re-baseline 2026-09-10 (Source Profile 0.13) — STILL_REPRODUCES

Compiler: mncs-language 890a653, Source Profile 0.13, x86_64 Linux.

`decode-plain-max` (255,255,255,254 -> 4294967294) still returns exact
values on bytecode/wasm/llvm/cranelift and still reports runtime_failure
on mncs-c11 (reproduced 2026-09-10 via the checked-in canary corpus).
The KNOWN_DIVERGENCES allowlist entry stays with stale-entry discipline.
All store decoders (old and new, including v2 length fields) keep the
wrapping formulation, so no store path executes the divergent shape.

Severity: major backend correctness divergence (unchanged).
