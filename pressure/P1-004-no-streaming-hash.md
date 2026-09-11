# P1-004 — No streaming or incremental hash API; single-shot digests over ≤64-byte views only

Storage feature being implemented: content identity for multi-chunk
objects and manifest roots larger than one view (RFC 0004 content IDs,
RFC 0016 multi-chunk reservation).

Observed language/runtime/compiler behavior: `sha256_digest(view)` is
single-shot over one view (`elaborate_sha256_digest` in
`crates/mncs-compiler/src/frontend.rs`; the operand is any byte sequence
but bounded by `MAX_SEQUENCE_BOUND = 64`). There is no init/update/final
streaming shape, no multi-part digest, and no way to hash a value larger
than 64 bytes. A 64-byte manifest root fits exactly one view; anything
larger (two chunk digests = 64 bytes of digests alone, before any header)
cannot be hashed in one call.

Minimal reproducer (mncs-language): hash the concatenation of two
32-byte chunk digests `a` and `b`:

```mncs
fn root_of_two(a: [byte; 32], b: [byte; 32]) -> (result: [byte; up_to 64])
    capability store_chunk
    effect sha256_digest authorized_by store_chunk
{
    // No expression builds the 64-byte concatenation as a view:
    // views cannot be concatenated, and [byte; 64] literals over two
    // 32-byte inputs require 64 explicit element expressions.
    // A 68+ byte manifest has NO hashing path at all.
    return sha256_digest(a); // wrong: covers only half the identity
}
```

Required semantics: an incremental digest (state type + update + finalize)
or a bounded multi-view digest, so content identity scales past one view
without weakening domain separation.

Why the current behavior/API is insufficient: Phase-1a manifests were
designed DOWN to 64 bytes specifically to fit one hash call. Any richer
root (multi-chunk references, provenance binding, generation links)
exceeds the bound and has no sound identity construction. The 64-byte
manifest is a language-imposed ceiling, not an architectural choice.

Safety/correctness implications: MEDIUM-HIGH. Workarounds (truncated
digests, ad-hoc chaining like `sha256(sha256(p1) || p2)`) change the
security argument of content addressing and must be designed by
cryptographers, not improvised to fit a view bound. We refused to do
either (RFC 0016 keeps multi-chunk roots unhashed-but-structural).

Performance implications: none (correctness-gated).

Workaround used: Phase 1a commits single-chunk objects only; the manifest
reserves `chunk_count` for the future and validators reject count != 1
(code 5). Per-chunk SHA-256 integrity is full-strength; root-level
multi-chunk identity is deferred, explicitly.

What the language/stdlib/runtime should ideally provide: a streaming
digest capability (`digest_init/update/finalize` over an opaque state, or
a bounded `digest_concat` over ≤K views) with the same verify-only,
no-secrets authority model as `sha256_digest`.

Affected backend(s) / target(s), if known: all (intrinsic-shape absence).

Severity: major

Status: partially implemented (single-chunk path complete; multi-chunk deferred)

## Re-baseline 2026-09-10 (Source Profile 0.13) — REFRAMED (see P2-001)

Compiler: mncs-language 890a653, Source Profile 0.13.

Elaboration widened: `sha256_digest` accepts any byte-view width (a
128-byte view elaborates; only the standard CMP301 view-range note).
Realization did NOT widen: `host_view_bytes` caps effect operands at
HOST_GRANT_MAX_BYTES = 64, so digesting > 64 bytes fails at execution
with InvalidRequest ("sha256_digest requires one byte-view operand",
reproduced 2026-09-10). Still no init/update/finalize streaming shape.

The store answers with a Merkle chain of <= 64-byte steps (manifest v2,
RFC 0017) instead of single-shot roots. The pressure is reframed from
"single-shot over <= 64 B views only" to "single-shot realization bound
64 B with no incremental API" and continues as P2-001.

Severity now: major (bounded workaround complete; arbitrary sizes still
impossible).
