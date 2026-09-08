# P1-001 — No file-write effect: persistence mechanics cannot be expressed in mncs-language

Storage feature being implemented: Phase 1 store lifecycle — chunk files,
object-root files, generation files, store metadata (all of §6–7 of the
campaign: create/open, write data, close, reopen, retrieve).

Observed language/runtime/compiler behavior: the compiler exposes exactly
four host effects, all read/observe-only (`clock_read`, `sha256_digest`,
`ed25519_verify`, `host_read`; see `elaborate_*` in
`crates/mncs-compiler/src/frontend.rs` and the `operation_id` dispatch in
`crates/mncs-model/src/execution.rs` / `ssa_execution.rs`). There is no
`host_write`, `file_write`, append, truncate, or any other mutation
effect in any source profile (0.1–0.11). A function declaring an unknown
effect is rejected at elaboration.

Minimal reproducer (mncs-language):

```mncs
mncs 0.10;

module store.write_probe;

fn persist_chunk(frame: [byte; 12]) -> (result: bool)
    capability store_writer
    effect host_write authorized_by store_writer
{
    return host_write(frame);
}
```

`source-study` rejects the `effect host_write` declaration (unknown
effect; cf. MNE235/MNE236 for the `host_read` analogue). There is no
spelling of this program that elaborates.

Required semantics: bounded, capability-authorized mutation of named
persistent bytes with explicit error reporting (absent path, quota,
I/O failure), so that chunk/object/generation writes are MNCS operations
with compiler-checked authority.

Why the current behavior/API is insufficient: the entire write path —
the core verb of a storage engine — must live outside the language in
the Phase-1a host driver (`tests/store_phase1a.py`). Authority over
mutation is therefore unenforceable by the compiler, and the store's most
safety-critical operations are invisible to MNCS verification.

Safety/correctness implications: HIGH. Write-path invariants (never
overwrite committed chunks, atomic generation publication, serial
issuance) are enforced by unaudited host code instead of typed MNCS
capabilities. A host bug can silently violate committed-content
immutability with no language-level tripwire.

Performance implications: every put/get crosses a subprocess + JSON
boundary (see P1-016); in-language writes would not by themselves fix
that, but they are a prerequisite.

Workaround used: `tests/store_phase1a.py` performs all file mutation in
Python (clearly labeled host transport, never semantics). MNCS owns every
semantic byte; Python owns files, fsync, and rename.

What the language/stdlib/runtime should ideally provide: a bounded
`host_write(name, bytes)`-family effect behind an explicit write
capability, with positioned/append/create-exclusive variants, returning a
rich status (written/denied/absent/unsupported) rather than trapping.

Affected backend(s) / target(s), if known: all (frontend-level absence).

Severity: blocker

Status: blocked (lifecycle mechanics host-owned; semantics in-language)
