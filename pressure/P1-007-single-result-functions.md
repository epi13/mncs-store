# P1-007 — Single-result functions force parse-then-project access patterns

Storage feature being implemented: descriptor/manifest/generation-record
parsing (decode a validated byte string into its fields).

Observed language/runtime/compiler behavior: `fn f(...) -> (result: T)`
returns exactly one value. There are no tuples or multi-out params. A
12-byte object encoding parses to (namespace, serial) only through TWO
functions (`object_namespace`, `object_serial`), each re-walking the
bytes; a 44-byte generation record splits through `gen_record_obj` +
`gen_record_root`; every descriptor field has its own accessor.

Minimal reproducer (mncs-language): the desired shape has no spelling —

```mncs
// NOT EXPRESSIBLE:
fn object_parse(id: [byte; 12]) -> (namespace: u32, serial: u64) { ... }
```

— so `store.identity` ships 4 parse functions where one would do, and
callers that need K fields invoke K traversals.

Required semantics: multiple named results, or lightweight tuple/record
destructuring at call sites, so one parse produces all fields with one
traversal and one validity argument.

Why the current behavior/API is insufficient: mostly ergonomics and
auditability — K single-field projectors must each independently maintain
the "caller validated first" precondition, and a reader cannot tell that
`object_namespace(x)` and `object_serial(x)` describe the SAME validated
x. Records as return types exist but hit the nominal-identity boundary
friction of P1-014 for host callers.

Safety/correctness implications: LOW (each projector is total and
tested). The risk is desynchronization: field offsets live in N function
bodies instead of one parse.

Performance implications: K traversals instead of one (negligible at
these widths; matters if descriptors grow).

Workaround used: per-field projectors (`desc_type/dim/len/alignment`,
`root_chunk_digest/root_descriptor/root_total_len/root_type`,
`gen_record_obj/gen_record_root`), each pinned by corpus cases.

What the language/stdlib/runtime should ideally provide: multi-result
functions or tuple returns with destructuring `let`.

Affected backend(s) / target(s), if known: all (surface absence).

Severity: moderate

Status: workaround (projector families + corpus pins)

## Re-baseline 2026-09-10 (Source Profile 0.13) — STILL_REPRODUCES

Functions still return exactly one value; no tuples or multi-out params
in any 0.13 feature list. Projector families grew (v2_type/v2_count/
v2_total_len/v2_descriptor/v2_header, conflict_observed/attempted,
snap_gen) following the same pinned pattern.

Severity: moderate (ergonomics + audit surface, unchanged).
