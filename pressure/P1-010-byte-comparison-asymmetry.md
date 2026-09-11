# P1-010 — Asymmetric byte comparisons: `<=` exists but `<` does not (MNE121)

Storage feature being implemented: canonical digest ordering
(`content_less`: first differing byte decides).

Observed language/runtime/compiler behavior: `mncs.core.bytes.v1`
defines ordering via `left <= right`, and `<=` elaborates over `byte` —
but `<` is rejected (MNE121). `!=` and `==` work over bytes. So byte
`less-than` must be spelled `(a <= b) && (a != b)`, a three-operator
idiom for a primitive relation.

Minimal reproducer (mncs-language):

```mncs
fn byte_less(left: byte, right: byte) -> (result: bool) {
    return left < right;  // MNE121; the <= spelling elaborates
}
```

Required semantics: the full ordered comparison set (`<`, `<=`, `>`,
`>=`) over `byte`, consistent with the integer types.

Why the current behavior/API is insufficient: digest ordering is load-
bearing for canonical key order (sorted chunk indices, deterministic
merges). Spelling it through a compound idiom is a readability and audit
cost on security-adjacent code; worse, a future reader may "simplify" the
idiom into something subtly different.

Safety/correctness implications: LOW (the idiom is exact and corpus-
pinned: `content-less-*` cases). Audit cost, not soundness.

Performance implications: none.

Workaround used: `((left[i] <= right[i]) && (left[i] != right[i])) as i64`
inside `content_less` (see `src/store/identity.mncs`).

What the language/stdlib/runtime should ideally provide: `<` (and `>`,
`>=`) over `byte` with the same total semantics as `<=`.

Affected backend(s) / target(s), if known: all (frontend rule).

Severity: minor

Status: workaround (compound idiom, pinned by ordering corpus cases)

## Re-baseline 2026-09-10 (Source Profile 0.13) — RESOLVED

Compiler: mncs-language 890a653, Source Profile 0.13.

`left < right` over `byte` elaborates and executes (the old MNE121
reproducer now completes). Removed from `content_less` together with
P1-009; same corpus evidence.
