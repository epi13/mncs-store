# P1-012 — No view-width subsumption: `[byte; up_to 8]` is not a `[byte; up_to 64]`

Storage feature being implemented: whole-frame verification
(`verify_frame*` calling the shared `frame_valid` view validator).

Observed language/runtime/compiler behavior: passing a `[byte; up_to 8]`
value to a `[byte; up_to 64]` parameter is rejected (MNE133 "call argument
type does not match the callee parameter"; hit in-session at
`src/store/chunk.mncs` before the fix). View bounds are invariant: a
narrower view is not usable where a wider view is expected, even though
every `[byte; up_to 8]` value IS a valid `[byte; up_to 64]` value by
construction. (The `sha256_digest` INTRINSIC accepts any byte-sequence
width — only user functions are invariant.)

Minimal reproducer (mncs-language):

```mncs
fn check(view: [byte; up_to 64]) -> (result: u64) {
    return view.len;
}
fn use8(x: [byte; 8]) -> (result: u64) {
    let v: [byte; up_to 8] = x[0..8];
    return check(v);  // MNE133
}
```

Required semantics: width subsumption for views (a view with bound M
coerces to bound N >= M), or an explicit `widen_view` conversion, so
shared validators can be written once.

Why the current behavior/API is insufficient: without subsumption, every
shared view-validator is either generic over `N: Nat` (but host corpora
cannot name type arguments — see P1-013 — so generic validators are
untestable from the harness) or monomorphized per width. `store.chunk`
does the latter by avoidance: `verify_frame*` inlines tag checks instead
of calling `frame_valid`.

Safety/correctness implications: LOW-MEDIUM. Duplicated validation logic
across widths must be kept in sync by discipline + tests (the manifest
validator duplicates descriptor checks for the related reason P1-E04;
both are pinned by corpora, but two copies of one rule is two chances to
drift).

Performance implications: none.

Workaround used: (1) exact-array callers inline their type-known width
checks; (2) shared validators take the concrete widest view
(`[byte; up_to 64]`); (3) generic view params used only where host
callers never invoke them directly.

What the language/stdlib/runtime should ideally provide: view-bound
subsumption (covariant width), or host-nameable type arguments (P1-013),
either of which collapses the duplication.

Affected backend(s) / target(s), if known: all (type-system rule).

Severity: moderate

Status: workaround (inline checks + widest-view signatures)
