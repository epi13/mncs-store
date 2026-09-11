# P2-006 — In-language generation scans bound at 23 records

New Phase-2 pressure (discovered 2026-09-10 implementing
`store.generation.header_validate` + `_classify_generation_file`).

Storage feature being implemented: verification-driven recovery over
generation files (RFC 0015: every authoritative root transitively
checked; commit metadata integrity-checked).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13): `header_validate` takes `[byte; up_to 1024]`, so a
generation file validates in-language only while 8 + 44*N <= 1024, i.e.
N <= 23 records. A 24-record generation (1064 bytes) cannot be passed as
an argument at all — and view-width invariance (P1-012) forbids chunking
the file into successive validator calls over sub-views (views cannot be
sub-sliced; narrower views do not subsume).

Minimal reproducer: validate a 24-record generation file (1064 bytes)
with `header_validate` — the argument does not bind (bound exceeded);
splitting into two views fails to typecheck (MNE133).

Required semantics: one of (a) wider views with matching iteration
envelope for metadata scans, (b) view slicing with provenance-preserving
bounds so a file validates window by window, or (c) a multi-view digest
so the COUNT cross-check lifts off the full-file view (P2-001 composes).

Why the current behavior/API is insufficient: full-snapshot generations
accumulate every object, so the 23-record ceiling is a store-growth
cliff, not a corner case. The driver handles wider files with the
Phase-1a host length gate (same equality, transport parity) plus full
per-object MNCS verification — fail-closed and honest, but the header
COUNT cross-check for wide generations is host-arithmetized.

Safety/correctness implications: LOW-MEDIUM. The fallback is the exact
equality the validator computes, and per-object roots still verify
in-language; the risk is audit duplication (two count checks to keep in
sync), the P1-012 pattern at larger scale.

Performance implications: none (correctness-gated).

Workaround used: `_classify_generation_file` runs MNCS `header_validate`
to 23 records and the host length gate beyond, with per-object MNCS
verification in both cases. Semantically exact for structure (same
equality), bootstrap substitute for authority. Phase-2 test generations
stay within 23 records so the primary path is fully in-language.

What the language/stdlib/runtime should ideally provide: sub-slicing or
windowed validation vocabulary so bounded metadata scans compose past
one view (composes with P2-001 streaming digests and P2-007 chunked
observation).

Affected backend(s): all (type/bound rules).

Severity: moderate (growth cliff with an exact fallback).

Status: workaround (host length-gate parity past 23 records)

Acceptance test: a 64-record generation file validates in-language
(header + count cross-check) with the same verdicts as the host gate on
every tested backend.
