# P1-005 — MAX_SEQUENCE_BOUND (64) caps value, chunk, and manifest sizes

Storage feature being implemented: chunk payload widths, manifest layout,
and the value classes a Phase-1a object can carry (RFC 0003/0004/0016).

Observed language/runtime/compiler behavior:
`crates/mncs-model/src/body.rs:16: pub const MAX_SEQUENCE_BOUND: u32 = 64;`
Exact sequences `[T; N]` and views `[T; up_to N]` cannot exceed 64
elements. Consequences hit in three places: (1) chunk payloads are capped
(60 bytes framable after a 4-byte header, 32 bytes supported); (2) the
manifest had to fit 64 bytes TOTAL to stay hashable in one `sha256_digest`
call (see P1-004); (3) `iterate` bounds (1..=32, MNE142) further restrict
single-level scans to half a view (see P1-008).

Minimal reproducer (mncs-language):

```mncs
fn wide() -> (result: [byte; 65]) {
    return [0, 0, /* ... 65 elements ... */];
}
```

Elaboration rejects the 65-element sequence (bound exceeded). Likewise a
`[byte; up_to 128]` parameter is unspellable.

Required semantics: bounded-but-larger sequences/views (storage needs at
least low-KB chunks to amortize per-chunk file and hash overhead), with
the bound remaining explicit and backend-declared.

Why the current behavior/API is insufficient: 32-byte payloads make every
value above trivial size multi-chunk, but multi-chunk roots are blocked
by P1-004 — so the two bounds multiply: values are capped at 32 bytes not
by architecture but by interacting language ceilings. A storage engine
with 32-byte-maximum values is a proof vehicle, not a substrate.

Safety/correctness implications: LOW (bounds are explicit and fail
closed). The cost is expressive range, not soundness.

Performance implications: HIGH. 4–36-byte chunk files turn every value
into a file-descriptor + hash + directory-entry event; real chunking
(4–64 KiB) would amortize this by ~1000x. Current per-object file counts
and hash calls are an artifact of the bound.

Workaround used: exact-width value classes (4/8/16/32-byte payloads);
arbitrary-length byte strings deferred; manifest squeezed to 64 bytes;
`frame_valid` code 5 (`UNSUPPORTED_WIDTH`) marks the boundary explicitly.

What the language/stdlib/runtime should ideally provide: raise (or
parameterize) the sequence bound toward low-KB views with backend
capability declarations, plus the streaming hash of P1-004 so larger
views remain hashable.

Affected backend(s) / target(s), if known: all (shared model constant);
individual backends may need their own lower caps declared honestly.

Severity: major

Status: workaround (exact-width classes; arbitrary sizes deferred)

## Re-baseline 2026-09-10 (Source Profile 0.13) — PARTIALLY_RESOLVED

Compiler: mncs-language 890a653, Source Profile 0.13.

Ceiling moved 64 -> 1024: `[byte; 65]` and `[0; 65]` elaborate and run;
`[byte; 1025]` refused (MNE105). Manifest views to 1024 bytes, 1024-wide
traversals, and 992-byte objects now execute (Phase-2 path). The store no
longer limits values to 32 bytes.

Remaining: the bound moved, it did not disappear. Arbitrary-sized
objects are still inexpressible; storage past 1024-byte views needs
chunked observation loops plus the missing streaming hash (P2-001).
Reframed as bounded-storage pressure: every bound (views 1024, digest
operands 64, iterations 1024/level, work envelope 1M) is explicit and
fail-closed, but their interaction caps objects at 992 bytes.

Severity now: moderate (was major; 32-byte proof vehicle -> 992-byte
bounded store).
