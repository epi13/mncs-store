# P2-005 — No atomic publication, namespace mutation, or durability barrier

Narrows P1-002/P1-003 (2026-09-10): observation resolved, publication
and durability still absent, with Phase-2 commit-protocol evidence.

Storage feature being implemented: the generation commit protocol
(stage candidate -> publish atomically), crash recovery over torn
publication, reclamation deletes, serial-counter durability (RFC 0005,
RFC 0015, RFC 0017 commit states).

Observed language/runtime/compiler behavior (mncs-language 890a653,
Source Profile 0.13): no rename, mkdir, delete, stat, exclusive-create,
fsync, or flush primitive exists (source grep, 2026-09-10). The commit
state machine (`commit_next`: PREPARING -> CONTENT_READY -> CANDIDATE ->
DURABLE_PREREQ -> PUBLISHED) runs its TRANSITIONS in-language, but every
state's host step — chunk writes, candidate staging, meta/current
pointer moves, directory fsyncs — executes in Python. Recovery
(`recover_decide`: STAY/PROMOTE/REFUSE) decides in-language, but the
PROMOTE pointer move is host `os.replace`.

Minimal reproducer: any program that atomically renames a staged
generation file onto its committed name, fsyncs a directory, or deletes
a pruned chunk — e.g. the Phase-2 publish step. No effect, intrinsic, or
stdlib function names these operations.

Required semantics: capability-authorized namespace mutation
(`file_rename` with same-filesystem all-or-nothing visibility,
`dir_create`, `file_delete`), a durability barrier (`sync` scoped to
file + directory, completing into the promised durability domain), and
exclusive creation — each with explicit error values, composed with the
fs_* root-grant model.

Why the current behavior/API is insufficient: atomic publication IS the
commit mechanism of RFC 0005 ("readers never observe an invented mixture
of two generations"). The driver proves the SEMANTICS (fault injection
at all 7 commit boundaries; readers see old-or-new, never mixed) but the
ATOMICITY itself is a host promise (`_write_sync`: tmp + fsync +
`os.replace` + dir fsync), invisible to MNCS verification. Durability
claims cover the host driver only (invariant 11).

Safety/correctness implications: HIGH for crash consistency. A host bug
in barrier ordering can publish a partially durable generation with no
language-level tripwire; the fault-injection suite pins the ORDER but
not the mechanism.

Performance implications: barrier batching/group-commit cannot be
expressed or tuned from MNCS.

Workaround used: host `_write_sync`, `_write_create_exclusive`,
`_fsync_dir`, `os.replace`, `os.listdir` (candidate scan only),
`os.remove` (reclamation only after MNCS membership verdicts). Each maps
1:1 to a missing capability in method docstrings. Semantically a
bootstrap substitute; the commit TRANSITIONS and recovery SELECTION are
exactly in-language.

What the language/stdlib/runtime should ideally provide: the namespace
+ barrier effect family above, shipped with the P2-004 write shapes and
per-backend capability declarations (some targets can only offer
"process-visible" — declared, not silent).

Affected backend(s): all (frontend-level absence).

Severity: major (blocker for the in-language commit protocol; the
semantic protocol itself is complete and tested).

Status: workaround (host publication mechanics; transitions/decisions
in-language)

Acceptance test: an MNCS commit program drives all five commit states
through granted file effects (no host file calls), survives a
fault-injection matrix identical to `test_phase2.py`'s 7 boundaries,
and replays byte-identical store layouts to today's host path.
