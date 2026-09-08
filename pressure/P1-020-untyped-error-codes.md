# P1-020 — No typed-error vocabulary at the process boundary (u64 status codes instead)

Storage feature being implemented: corruption-vs-absence-vs-unsupported
discrimination across the MNCS/host boundary (RFC 0002/0015 failure
behavior: "a content hash mismatch is an integrity failure, not 'object
not found'").

Observed language/runtime/compiler behavior: `mncs.core.result.v1`
defines a `Result` shape with reason payloads, and `fail` aborts with a
fixed reason — but neither crosses the process boundary usefully for
storage: `Result`-with-payload is a RECORD (see nominal-identity friction
P1-014), and `fail` carries no structured payload the harness can match
on. Validators therefore return `u64` status codes (0 = OK, 1..13 =
specific rejections), and the host maps codes to exception types. The
code tables live in MNCS comments, not in a shared machine-readable
contract.

Minimal reproducer: return `Result`-with-`BadMagic`-reason from
`descriptor.validate` and match on the reason in the host harness. The
host receives a record value with a nominal identity string (P1-014)
instead of a matchable error sum it can rely on.

Required semantics: sum-typed errors with stable, documented boundary
encodings (discriminant + bounded payload), so corruption/missing/
unsupported/unauthorized stay distinct by construction on both sides.

Why the current behavior/API is insufficient: the u64-code discipline
works (15 corruption tests assert exact codes end-to-end) but the
meaning of code 7 vs 8 vs 9 exists in two places (MNCS comments + host
`if` chains) with no shared authority. A renumbering on either side
silently desynchronizes the other — exactly the stringly-typed failure
mode the store architecture forbids for IDs.

Safety/correctness implications: MEDIUM. Today the codes are pinned by
169 corpus cases, so drift is caught — but the pinning is by value, not
by type. A typed error sum would make unknown-code states
unrepresentable instead of merely tested-against.

Performance implications: none.

Workaround used: documented u64 code tables per validator
(`descriptor.validate` 0–13, `manifest.validate` 0–9, `frame_valid`
0–5), each code pinned by ≥1 corpus case; host maps codes to
`IntegrityError` subtypes by table.

What the language/stdlib/runtime should ideally provide: boundary-stable
error sums (or enriched `fail` reasons) with a machine-readable contract
emitted by `mncs abi`, so hosts match on meaning, not magic numbers.

Affected backend(s) / target(s), if known: all (ABI-shape gap).

Severity: moderate

Status: workaround (code tables + corpus pins)
