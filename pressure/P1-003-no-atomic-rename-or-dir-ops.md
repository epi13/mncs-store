# P1-003 — No atomic rename, directory creation/iteration, or file metadata

Storage feature being implemented: generation commit protocol (stage in
`temp/`, atomically publish via rename), store layout bootstrap
(`chunks/`, `objects/`, `generations/`, `temp/`), crash-recovery scans
(listing `objects/` to heal the serial counter), existence checks.

Observed language/runtime/compiler behavior: no filesystem-namespace
primitive exists — no mkdir, no readdir/iteration, no rename, no stat, no
existence probe, no exclusive-create. `host_read` consumes a single
operator-granted file as an opaque byte source; it cannot name, list, or
inspect the namespace. Path/path-component values have no type at all
(see P1-017).

Minimal reproducer: any program that creates a directory, lists it, or
atomically renames a staged file — e.g. the Phase-1a commit step
`os.replace(temp, generations/<gen>)`. No effect, intrinsic, or stdlib
function names these operations.

Required semantics: capability-authorized namespace operations with
explicit outcomes: create-dir (exists/absent/denied), iterate-dir
(bounded entries), atomic rename (same-filesystem replace with
all-or-nothing visibility), stat (size/kind), exclusive-create.

Why the current behavior/API is insufficient: atomic publication is THE
commit mechanism of RFC 0005 ("readers never observe an invented mixture
of two generations"). Without atomic rename, generation commits cannot be
made atomic in-language; without directory iteration, recovery scans
(RFC 0015) cannot enumerate candidate state.

Safety/correctness implications: HIGH for Phase 2 (crash-consistent
generation protocol is inexpressible). Phase 1a works around it in the
host driver with `os.replace` + fsync ordering.

Performance implications: none observed yet (namespace ops are rare).

Workaround used: `store_phase1a.py` owns `makedirs`, `os.replace`,
`os.listdir` (recovery scan only), `O_EXCL` creation. Each maps 1:1 to a
missing language capability noted in method docstrings.

What the language/stdlib/runtime should ideally provide: a bounded
namespace effect family (`dir_create`, `dir_read`, `file_rename`,
`file_stat`, `file_create_exclusive`) behind a namespace capability, with
bounded iteration over entries and explicit error values.

Affected backend(s) / target(s), if known: all (frontend-level absence).

Severity: major (blocker for Phase 2 commit protocol; worked around in 1a)

Status: workaround (host-owned namespace mechanics)

## Re-baseline 2026-09-10 (Source Profile 0.13) — PARTIALLY_RESOLVED

Compiler: mncs-language 890a653, Source Profile 0.13.

Observation half resolved: `fs_list_count`, `fs_generation`,
`fs_entry_name_at`, `fs_entry_kind_at` (effect `fs_list`) and
`fs_read_bytes_at(entry, offset, length)` (effect `fs_read`) elaborate
and execute on research-bytecode via `--grant-fs capability=root`.
Verified: 3-entry listing, generation counter, indexed names/kinds,
16-byte positioned read, short-read EOF (offset 10 + len 64 over 16
bytes returned 6 bytes), wild index 99 fails closed InvalidRequest.
Recovery scans and chunked file reads are now expressible in-language.

Mutation half still absent: no rename, mkdir, exclusive-create, stat,
delete, or sync. Atomic publication stays host `os.replace`; the Phase-2
commit protocol stages content in-language but publishes via the host.

Severity now: major for commit atomicity (was blocker for Phase 2
protocol; observation resolved, publication still host-owned).
