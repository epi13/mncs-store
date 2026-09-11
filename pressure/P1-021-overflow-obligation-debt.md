# P1-021 — Checked-arithmetic obligations have no discharge path for input-bounded values

Storage feature being implemented: big-endian decoders over byte inputs
(`object_namespace`, `u32_from_be`, `desc_len`, manifest length fields).

Observed language/runtime/compiler behavior: plain `*`/`+` carry
`integer-overflow` obligations; unproven obligations keep a
"conservative fallback" and the study status
`completed_with_unresolved_obligations` (CMP301). There is no source-level
proof vocabulary for the actual argument — "each factor fits in a byte,
so the product fits u32" — because `requires` contracts name declared
checkers, not value relationships, and the bound lives in the element
type (`byte`), not in a form the obligation solver consumes. Every store
decoder therefore compiles with permanent proof debt, even where a human
sees total safety in one line.

Minimal reproducer: `object_namespace` over `[byte; 12]` — all four
products are byte×constant, the sum is ≤ 0xFFFFFFFF by construction, yet
four CMP301 obligations persist with no source annotation that discharges
them.

Required semantics: a way to discharge overflow obligations from input
type bounds (byte-ness implies ≤ 255; products of bounded values fit
declared widths), e.g. bound-propagating `requires`, solver support for
element-type ranges, or explicit-but-checked `wrapping_where_exact`
intent.

Why the current behavior/API is insufficient: two costs. First, proof
debt is INDISTINGUISHABLE from real risk: every module reports
"completed_with_unresolved_obligations", so a genuinely risky plain `+`
hides among provably-safe ones. Second, the fallback is backend-defined —
and P1-B01 shows the C11 fallback TRAPS on large-but-valid products
while other backends return exact values. Unprovable-but-safe code is
thus also non-portable code, with no diagnostic pointing at the gap.

Safety/correctness implications: MEDIUM (via P1-B01: the obligation
system's sharp edge is a real backend divergence, found by this run).

Performance implications: conservative fallbacks may emit checked code
where wrapping would do; unmeasured (see P1-016 on instrumentation).

Workaround used: wrapping operators (`*%`/`+%`) everywhere exactness is
provable by construction, with comments citing P1-B01; plain operators
kept only where smallness is trivially visible (≤ 16-bit intermediates).
Plus the `checked_arith_canary` fixture pinning the divergence.

What the language/stdlib/runtime should ideally provide: (1) range-aware
obligation discharge from element types; (2) a UNIFORM, documented
fallback semantic for unproven-but-appropriate checked ops across all
backends (trap vs wrap must not differ silently); (3) per-site obligation
reporting that names the operation (today's CMP301 lists opaque
obligation identities).

Affected backend(s) / target(s), if known: all for the debt; C11 for the
divergent fallback (P1-B01).

Severity: moderate

Status: workaround (wrapping discipline + canary)

## Re-baseline 2026-09-10 (Source Profile 0.13) — STILL_REPRODUCES

Every new module compiles with permanent CMP301 obligations
(integer-overflow on decoders, iteration-exact-resource-cost on every
loop, view-range-valid on effect operands) and the conservative
fallback; no source annotation discharges byte-bound reasoning. The
wrapping discipline (`*%`/`+%` where exactness is provable) extends to
all new decoders and the chain arithmetic. Proof debt stays
indistinguishable from real risk at the study layer.

Severity: moderate+ (unchanged; extended surface).
