# P1-006 — No scatter-write into byte buffers; only whole-array functional construction

Storage feature being implemented: generic chunk framing (tag + length +
N-byte payload for arbitrary N), variable-length manifest assembly,
zero-padded blob handling.

Observed language/runtime/compiler behavior: sequences are immutable
values. The only "update" shape is functional rebuild of the whole array
(cf. `put4` in `library/core/sequences.mncs`, which rewrites a 4-element
array through an if-chain). There is no indexed assignment, no mutable
byte buffer, no `memcpy`-like primitive, no view-to-exact materialization.
Building a 36-byte frame for a runtime-length payload would require an
O(N) if-chain over positions or 36 explicit element expressions per width
— i.e. monomorphized literals, which is what `store.chunk` does
(`frame4/8/16/32`).

Minimal reproducer (mncs-language): frame a runtime-length view into an
exact array:

```mncs
fn frame_generic(payload: [byte; up_to 64], length: u64) -> (result: [byte; 36]) {
    // No expression writes payload[i] into result slots: indexing
    // constructs values, and there is no buffer to scatter into.
    // Only a 36-arm explicit construction works (see frame32).
    return frame32(payload); // MNE133: view is not [byte; 32]
}
```

Required semantics: a bounded scatter primitive (write `byte` at a
proven-in-bounds index of a mutable buffer, or bulk copy view->buffer
with explicit length), so framing/parsing need not be monomorphized per
width.

Why the current behavior/API is insufficient: every new payload width
costs a hand-written ~15-line framing function plus matching
digest/verify/projector variants. `store.chunk` is ~450 lines for FIVE
widths; a 6th (e.g. 48-byte frames, still under the 64 bound) costs
another ~60 lines of near-identical code. This is not abstraction
resistance in the problem — it is missing bulk-data vocabulary.

Safety/correctness implications: MEDIUM. The monomorphized code is
correct but its very volume invites copy/paste width errors (we caught
one: `get_u64` initially took `[byte; 12]`-vs-`[byte; 20]` — found only by
executing the corpus). A scatter primitive with checked indices would
have one code path, not five.

Performance implications: functional rebuilds copy whole arrays per
"update"; framing is O(widths × sizes) source and O(N) copies at best.

Workaround used: exact-width monomorphization (frame4/8/16/32 +
verify_frame* + payload* + get_*); arbitrary widths rejected with
`frame_valid` code 5; 64-byte manifest/root literals spelled element by
element (64-element array literal in `manifest.encode`).

What the language/stdlib/runtime should ideally provide: bounded mutable
byte buffers (or linear/unique buffer types) with checked indexed write
and view->buffer bulk copy; or first-class bulk constructors
(repeat/splat, concat of exact arrays, view materialization with explicit
length).

Affected backend(s) / target(s), if known: all (surface absence).

Severity: major

Status: workaround (monomorphized widths; volume-induced bug caught by tests)

## Re-baseline 2026-09-10 (Source Profile 0.13) — PARTIALLY_RESOLVED

Compiler: mncs-language 890a653, Source Profile 0.13.

New bulk vocabulary: `replace(array, index, value)` with iteration
carried state, `[value; N]` repeat literals, and 1024-wide construction
all elaborate and execute (verified: fill16, zero16, concat32_32). The
manifest-v2 chain needs exactly one 64-byte concatenation, which is now
one monomorphized function instead of an impossibility.

Remaining: no indexed assignment to buffers, no view concatenation, no
view-to-exact materialization, no scatter into file regions. Variable
geometry still costs one hand-written function per width (tail_ok4/8/16/
32, payload4/8/16/32) and dynamic table assembly stays host-side. The
monomorphization tax fell (5 widths -> 1 concat + 4 tails) but persists.

Severity now: moderate (was major).
