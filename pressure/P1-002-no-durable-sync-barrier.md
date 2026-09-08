# P1-002 — No durable-sync barrier distinct from userspace buffering

Storage feature being implemented: commit durability — the promise that a
generation acknowledged as durable survives process and OS crashes
(invariants 11–12, RFC 0005 durability, RFC 0015 recovery).

Observed language/runtime/compiler behavior: even if P1-001 (writes) were
resolved, no API exposes a persistence barrier (`fsync`/`fdatasync`/
flush-to-stable-media or a durability-policy acknowledgment). The four
existing host effects are pure observations. There is no spelling for
"make preceding writes durable" and no way for an MNCS program to learn
which durability barrier, if any, backs its storage.

Minimal reproducer: any program requiring post-commit crash survival —
e.g. write chunk bytes, issue a (nonexistent) sync barrier, acknowledge
durability. The barrier step has no expression; see P1-001 reproducer.

Required semantics: an explicit, capability-gated sync operation whose
completion means bytes reached the durability domain promised by the
active policy, with failure (I/O error, read-only media, unsupported
barrier) reported as a value, not assumed.

Why the current behavior/API is insufficient: without a barrier, no MNCS
program can truthfully distinguish "visible to this process" from
"durable". Any durability acknowledgment would be a fabrication
(invariant 11 forbids acknowledging an uncompleted barrier).

Safety/correctness implications: HIGH. Crash-consistency arguments for
generations (Phase 2) cannot even be stated, let alone proven, until the
barrier exists. Recovery semantics rest on a host promise the language
cannot name.

Performance implications: barrier batching/group-commit policy cannot be
expressed or tuned from MNCS; durability costs stay invisible.

Workaround used: host driver calls `os.fsync` after every file write and
directory mutation, and documents the exact barrier order in
`store_phase1a.py` (`_write_sync`, `_write_create_exclusive`,
`_fsync_dir`). Durability claims in this run cover the host driver only.

What the language/stdlib/runtime should ideally provide: a `sync`
effect (file + directory scope) tied to the durability-policy vocabulary
of RFC 0008, with explicit completion/failure and backend capability
declarations (some targets cannot offer more than "process-visible").

Affected backend(s) / target(s), if known: all (frontend-level absence);
some future targets (bare metal, WASM sandboxes) may only ever offer
weakened barriers, which must then be DECLARED, not silent.

Severity: blocker (for any durability claim; visibility-only commits work)

Status: blocked
