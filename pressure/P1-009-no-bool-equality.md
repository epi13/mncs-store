# P1-009 — No `==` over `bool` (MNE121)

Storage feature being implemented: first-differing-byte digest ordering
(`content_less`), flag-based scan state.

Observed language/runtime/compiler behavior: `==` requires integer
operands; comparing two bools is rejected (MNE121 "comparison operands
must have an integer type"). The natural `state.decided == false` does
not elaborate. Boolean algebra must go through `&&`/`||` (which exist)
or be encoded as i64 flags (0/1) with `as i64` casts from comparisons
(which is what both `mncs.core.identity.v1` and `store.identity` do).

Minimal reproducer (mncs-language):

```mncs
fn negated(flag: bool) -> (result: bool) {
    return flag == false;  // MNE121
}
```

The working spelling is `(flag && false) || ...` contortions, or carrying
i64 flags instead of bools.

Required semantics: `==`/`!=` over `bool` (and, symmetrically, the full
comparison vocabulary over every ordered type — see P1-010).

Why the current behavior/API is insufficient: ordering/state code that is
one line in any conventional language becomes flag-encoding discipline
(`OrderState { decided: i64, before: i64 }` with `*%`/`+%` arithmetic in
`content_less`). The encoding is correct and tested, but every reader
must verify the 0/1 discipline instead of reading boolean logic.

Safety/correctness implications: LOW-MEDIUM. i64 flag encodings are a
known bug farm (a `2` where only 0/1 is expected silently misbehaves);
bool-typed state would make illegal states unrepresentable.

Performance implications: none.

Workaround used: i64 0/1 flags with wrapping arithmetic, mirroring
`mncs.core.identity.v1::less` (which hit the same wall first).

What the language/stdlib/runtime should ideally provide: `==` and `!=`
over `bool` (total, no obligations).

Affected backend(s) / target(s), if known: all (frontend rule).

Severity: minor

Status: workaround (i64 flag discipline)

## Re-baseline 2026-09-10 (Source Profile 0.13) — RESOLVED

Compiler: mncs-language 890a653, Source Profile 0.13.

`bool == bool`, `!=`, and prefix `!` elaborate and execute (CP-0004;
17-case corpus over all five backends upstream). The old MNE121
reproducer (`flag == false`) now completes clean.

Store workaround REMOVED: `store.identity.content_less` dropped the i64
0/1 flag discipline for boolean state (`OrderState { decided: bool,
before: bool }`, `!state.decided`, `left[i] < right[i]`). All 33
identity corpus cases pass on all five backends after the change.
