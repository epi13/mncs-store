# P2-002 — No view-width subsumption forces widest-view boundary signatures

Continues P1-012 (STILL_REPRODUCES on Source Profile 0.13; do not treat
this as a new report — it is the Phase-2 cost of that gap).

Storage feature being implemented: v2 boundary functions that accept
short exact structures (8-byte generation headers, 28..1020-byte v2
manifests, 1024-byte digest tables) under RFC 0017.

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13, research-bytecode): a `[byte; up_to 8]` view still
cannot feed a `[byte; up_to 64]` parameter (MNE117 + MNE133, reproduced
2026-09-10). Hit live in Phase 2: the planned in-language
generation-header round trip was removed because an 8-byte header view
cannot feed `header_validate`'s up-to-1024 view; the corpus pins the
round trip across the harness boundary instead (see P1-012 re-baseline).

Required semantics: width subsumption on view arguments (a shorter view
satisfies a wider `up_to` bound), so functions take the view they mean.

Why the current behavior/API is insufficient: every boundary function
must take the WIDEST view it may ever receive, and recover precision
with manual length gates:

- `header_validate(view: [byte; up_to 1024])` validates an 8-byte
  header; the first thing it does is `if view.len < 8 { return 1; }`
  (code 1 BAD_VIEW_LEN) — a check the signature could have carried.
- `validate_v2` takes `[byte; up_to 1024]` for manifests of 28..1020
  bytes, with codes 1 BAD_VIEW_LEN / 8 COUNT_MISMATCH doing the work
  the type system declines.
- `table_contains(table: [byte; 1024], ...)` takes an EXACT 1024-byte
  table, forcing the host to zero-pad every digest window to exactly
  1024 bytes before transport (see `reclaim` in `tests/store_phase2.py`).

Safety/correctness implications: LOW-MEDIUM. The manual gates are
tested (every `validate_v2` code has corpus cases), but each gate is a
hand-written replica of a subsumption rule — one missed `view.len`
check is an over-read-shaped bug the signature should prevent.

Performance implications: argument bytes on the wire are the true
length (views are length-carrying), so there is no transport bloat;
the cost is surface area and audit load, not steps.

Workaround used: widest-view signatures everywhere on the boundary
(`src/store/generation.mncs`, `src/store/manifest.mncs`); exact-1024
tables with host zero-padding; manual BAD_VIEW_LEN gates as the first
check of every validator.

What the language/stdlib/runtime should ideally provide: argument
subsumption for `up_to` views (short view into wider bound), keeping
exact-width parameters exact where padding is semantically load-bearing
(`table_contains`).

Affected backend(s): all (elaboration-shape rejection).

Severity: moderate.

Status: workaround (widest-view signatures + manual length gates)

Acceptance test for the future fix: an 8-byte header view passes
directly to a `[byte; up_to 64]` (or narrower-than-1024) parameter and
`header_validate` takes `[byte; up_to 64]`; the header round trip moves
back in-language and the harness-boundary pin is removed.
