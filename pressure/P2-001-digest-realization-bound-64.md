# P2-001 — Single-shot digest realization bound (64 B) with no incremental API

Continues P1-004 (reframed 2026-09-10; do not treat the old report as
current truth — the bound moved layers).

Storage feature being implemented: content identity for multi-chunk
objects and manifest roots larger than one 64-byte view (RFC 0004
content IDs, RFC 0017 manifest v2).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13, research-bytecode): `sha256_digest` elaborates over
any byte-view width (a 128-byte view passes elaboration with only the
standard CMP301 view-range note), but realization caps operands at
`HOST_GRANT_MAX_BYTES = 64` (`host_view_bytes` in
`crates/mncs-model/src/execution.rs`). Digesting a 128-byte view fails
at execution with InvalidRequest ("sha256_digest requires one byte-view
operand", reproduced 2026-09-10). There is still no
init/update/finalize streaming shape and no bounded multi-view digest.

Minimal reproducer (mncs-language, elaborates, fails at execution):

```mncs
mncs 0.13;
module probe.hash2;
fn digest128(payload: [byte; 128]) -> (result: [byte; up_to 64])
    capability store_chunk
    effect sha256_digest authorized_by store_chunk
{
    let view: [byte; up_to 128] = payload[0..128];
    return sha256_digest(view);
}
```

with a 128-byte sequence argument: status invalid_request at step 4.

Required semantics: an incremental digest (opaque state + update +
finalize) or a bounded multi-view digest, so content identity scales
past 64 bytes without weakening domain separation.

Why the current behavior/API is insufficient: any root binding more
than 64 bytes of structure (multi-chunk references, provenance binding,
generation links) has no sound single-shot construction. The store
answers with a Merkle chain of <= 64-byte steps (RFC 0017): H0 over the
28-byte header, then H_{i+1} = SHA-256(H_i || chunk_digest_i). The chain
is sound but costs one effect invocation per chunk plus init, and chain
depth scales with object size.

Safety/correctness implications: MEDIUM. The chain is designed
cryptography (domain-separated header binding + ordered steps), not an
improvised truncation — but every hand-rolled composition is a new
security argument the language should ideally provide once.

Performance implications: chain depth = effect invocations (P2-008);
objects past 992 bytes need deeper or wider compositions that do not
exist yet.

Workaround used: manifest-v2 chained root (`chain_init`/`chain_step` in
`src/store/manifest.mncs`), pinned by chain corpora with hashlib
oracles including order-sensitivity pairs. Semantically exact for the
bounded domain (0..992 B, 1..31 chunks), a bootstrap substitute for
arbitrary sizes.

What the language/stdlib/runtime should ideally provide: a streaming
digest capability (`digest_init/update/finalize` over opaque state, or a
bounded `digest_concat` over <= K views) with the same verify-only,
no-secrets authority model as `sha256_digest`.

Affected backend(s): all (intrinsic-shape absence at realization;
elaboration already widened).

Severity: major.

Status: workaround (chained roots to 992 B; arbitrary sizes deferred)

Acceptance test for the future fix: the `digest128` reproducer above
returns `hashlib.sha256(bytes(range(128)))` on research-bytecode, and a
streaming construction hashes a 1024-byte view identically to the
single-shot oracle.
