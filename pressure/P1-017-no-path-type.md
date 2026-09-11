# P1-017 — No path/string type: layout names are unrepresentable in-language

Storage feature being implemented: store layout naming (`chunks/<hex>`,
`objects/<hex>`, `generations/<gen>`, `temp/`) as store-owned
implementation detail (RFC: "the filesystem layout is an implementation
detail; the store abstraction must own the layout").

Observed language/runtime/compiler behavior: MNCS has bytes, byte views,
and bounded text-scan vocabulary over granted bytes — but no string,
path, or filename type, no hex formatting, and no way to construct a
name from a digest. `host_read` grants bind capability→path OUTSIDE the
language (`--grant-read capability=path`); inside, the program sees only
anonymous bytes. A store abstraction cannot "own its layout" if it cannot
name it.

Minimal reproducer (mncs-language): format a 32-byte digest as 64 hex
characters to derive a chunk filename. There is no string builder, no
byte-to-hex primitive, and no filename value to pass anywhere even if
formatting existed.

Required semantics: bounded string/path values with hex/base encoding,
join/normalize operations that cannot escape a store root (no `..`
traversal by construction), and capability-gated resolution of paths to
byte sources/sinks (composing P1-001/P1-003 with names).

Why the current behavior/API is insufficient: layout derivation (content
hash → path) is a pure, deterministic, SAFETY-CRITICAL function
(path confusion = reading the wrong chunk = integrity failure), and it
must live in host string code because the language has no strings.
`bytes.hex()` in the driver is correct but unaudited-by-compiler.

Safety/correctness implications: MEDIUM. Path-confusion bugs (wrong
directory, unsanitized serial, case/encoding mismatch) are a classic
storage corruption vector; a path type with root-confinement would rule
out the entire class.

Performance implications: none.

Workaround used: host-side `digest.hex()` / f-string paths in
`store_phase1a.py`; layout documented in RFC 0016 as host-owned Phase-1a
mechanics pending path + I/O capabilities.

What the language/stdlib/runtime should ideally provide: bounded `Path`
values (root-confined join, hex encoding of digests, explicit
normalization errors) composed with the file effects of P1-001/P1-003.

Affected backend(s) / target(s), if known: all (type absence).

Severity: moderate

Status: blocked (layout naming host-owned)

## Re-baseline 2026-09-10 (Source Profile 0.13) — STILL_REPRODUCES

No string/path/filename type in any 0.13 feature list. The fs_* design
deliberately avoids paths: u64 entry indices plus <= 64-byte name values
keep authority in grants and indices as data. Layout derivation
(digest -> hex filename) stays host `bytes.hex()`; names crossing into
MNCS are opaque bytes. The pressure narrows (indices compose better than
nothing) but the type absence stands.

Severity: moderate (unchanged).
