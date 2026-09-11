# P1-011 — Wrapping-operator literal inference is position-sensitive (MNE117/MNE119)

Storage feature being implemented: length arithmetic in chunk-frame
validation (`store.chunk.frame_valid`).

Observed language/runtime/compiler behavior: in `view.len != 4 +% declared`
(`view.len: u64`, `declared: u64`), the literal `4` on the LEFT of `+%`
fails (MNE117 "resolved name does not have the required expression type"
+ MNE119 "binary operands must have the same type"), while `declared +% 4`
(literal on the right) and `4 + x` (plain operator) both elaborate.
Minimal probe at `/tmp` (reproducible; also hit in-session in
`store.chunk` line 249 before the fix):

```mncs
mncs 0.10;
module probe.wrap;
fn literal_left(x: u64) -> (result: u64) {
    return 4 +% x;  // MNE117 + MNE119
}
fn literal_right(x: u64) -> (result: u64) {
    return x +% 4;  // OK
}
fn plain_add(x: u64) -> (result: u64) {
    return 4 + x;  // OK
}
```

Required semantics: symmetric literal inference for wrapping operators
(the literal takes the other operand's type regardless of side), matching
plain-operator behavior.

Why the current behavior/API is insufficient: the asymmetry is silent and
confusing — identical-looking expressions differ in validity by operand
order, with an error message ("resolved name ...") that points at the
WRONG operand (`declared`, not the literal `4`). Session cost: real
debugging time on a two-character difference.

Safety/correctness implications: LOW (fail-closed at elaboration). No
wrong code is accepted; valid code is merely rejected confusingly.

Performance implications: none.

Workaround used: bind typed locals first (`let framed: u64 = declared +% 4;`)
and keep literals on the right of wrapping operators throughout the store
sources.

What the language/stdlib/runtime should ideally provide: order-independent
literal coercion for `+%`/`-%`/`*%`, plus an MNE diagnostic that names the
literal when IT is the offender.

Affected backend(s) / target(s), if known: all (frontend rule).

Severity: minor

Status: workaround (coding discipline + comment in sources)

## Re-baseline 2026-09-10 (Source Profile 0.13) — STILL_REPRODUCES

Compiler: mncs-language 890a653, Source Profile 0.13.

`4 +% x` still fails (MNE117 + MNE119); `x +% 4` elaborates. Hit live
during this run at `src/store/manifest.mncs:309` (`28 +% count *% 32`),
fixed with the right-side discipline (`count *% 32 +% 28`). Coding
discipline stays; each site carries a P1-011 comment.

Severity: minor (unchanged).
