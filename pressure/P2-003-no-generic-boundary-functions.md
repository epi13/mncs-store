# P2-003 — No generic boundary functions forces per-width tail families

Continues P1-013 (host cannot invoke generics; Source Profile 0.13 adds
source-level inference but no type-argument channel on execution
requests). This is the Phase-2 instantiation of that gap.

Storage feature being implemented: canonical tail framing across the
four frozen widths {4, 8, 16, 32} (RFC 0017: every tail frame reuses a
v1 width and its single-shot identity).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13, research-bytecode): execution requests still carry
(module, function, arguments) with no type-argument channel, so a
width-parameterized tail handler cannot sit on the boundary. Phase 2
needs five operations per width — frame, digest, verify, tail_ok
(canonical-padding predicate), payload projection — and gets them as
FOUR monomorphic families, twenty concrete boundary functions:

- `frame4/8/16/32`, `digest4/8/16/32`, `verify_frame4/8/16/32`,
  `tail_ok4/8/16/32`, `payload4/8/16/32` in `src/store/chunk.mncs`.

The host carries the dispatch mirror: `_tail_width` (remainder ->
width) plus the `TAIL_FNS` table in `tests/store_phase2.py`, and
`test_tail_width_host_mirror_matches_mncs` pins the host mirror to
MNCS `tail_width_for` over remainders 0..33 on every run.

Required semantics: either host-nameable type arguments on execution
requests (invoke `tail_ok<16>` as source does) or argument-driven
instantiation, so one polymorphic tail family serves all widths.

Why the current behavior/API is insufficient: adding a fifth width
means five new boundary functions plus host-table rows plus mirror
tests; the dispatch table is orchestration the language should own.
The twenty functions are thin (each delegates to its width's frozen
v1 identity), but the family must be maintained per width and the
mirror kept in agreement by a dedicated test.

Safety/correctness implications: LOW. The mirror test fails loudly on
any disagreement, and the MNCS predicate (`tail_okW`) is the authority
on every put AND every read — a wrong host dispatch produces bytes
the predicate rejects, never bytes it accepts.

Performance implications: none (dispatch is host-side selection among
already-batched calls).

Workaround used: `TAIL_FNS` width-family table + `_tail_width` host
mirror; `chain_step` stays monomorphic over 32-byte digests (no
generics needed there by construction).

What the language/stdlib/runtime should ideally provide: the P1-013
ask — a `type_arguments` field on execution-request targets with
elaboration-time checking — at which point the four tail families
collapse to one generic family and `TAIL_FNS` shrinks to a width
argument.

Affected backend(s): all (execution-request shape).

Severity: moderate.

Status: workaround (four monomorphic tail families + pinned host mirror)

Acceptance test for the future fix: one generic tail family
(`frame<W>`, `digest<W>`, `tail_ok<W>`, …) invoked from a corpus with
explicit or inferred width arguments; `TAIL_FNS` deleted and the
mirror test removed with nothing else changing.
