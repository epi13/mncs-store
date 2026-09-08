# P1-014 — Record nominal identities are brittle at the process boundary

Storage feature being implemented: nominal separation of ChunkId /
ContentId / RootId (all 32 bytes, must not interchange) across the
MNCS/host test boundary.

Observed language/runtime/compiler behavior: record values cross the
process boundary with nominal `type_identity` strings such as
`mncs:0.2:record-type:examples.abi.unsigned_records::WordPair::hi%3Au64%3Blo%3Au64%3B`
— strings that embed the profile version, module path, field names, and
field-type spellings. A host constructing a record argument must emit
exactly the compiler's current spelling; any drift (profile bump,
field reorder, backend-specific representation) breaks the call opaquely.
The `mncs abi` command can report the identities, but the harness would
have to query-and-splice per call.

Minimal reproducer: call `store.identity.logical_equal` (takes two
`LogicalId` records) from a corpus. The request must carry two
`{"record": {"type_identity": "...", ...}}` arguments whose exact
spelling is discoverable only by running `mncs abi` first — there is no
stable, documented construction rule a host can implement once.

Required semantics: a stable value-level record encoding for the
boundary (positional fields + a versioned type tag the host can
construct), or record-param erasure to structural shape with nominal
checking done in-language.

Why the current behavior/API is insufficient: nominal typing is exactly
what the store wants for ChunkId vs RootId — but it is unusable across
the boundary it must cross for testing. The store therefore keeps its
harness boundary to scalars + byte sequences (which are stable) and
proves the record layer only through scalar round-trip laws
(`logical_roundtrip`, `content_roundtrip`, …).

Safety/correctness implications: LOW-MEDIUM. The workaround preserves
honest coverage (records ARE constructed, projected, and compared
in-language; corpora pin the scalar-visible behavior), but cross-type
confusion at the RECORD level has no executable negative test — only the
width-level confusion does (`type_confusion.mncs` → MNE133).

Performance implications: none.

Workaround used: scalar/bytes harness boundary + in-language record
round-trip laws + one width-level negative elaboration fixture.

What the language/stdlib/runtime should ideally provide: a documented,
stable record-argument encoding (or `mncs abi`-driven stub generation)
so hosts can construct nominal values without reverse-engineering
identity strings.

Affected backend(s) / target(s), if known: all (ABI-string format).

Severity: moderate

Status: workaround (scalar boundary + round-trip laws)
