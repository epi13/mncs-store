# P1-019 — host_read grants are single-file, ≤64-byte, whole-content snapshots

Storage feature being implemented: in-language read-path verification
(`store.read_verify`: read chunk file → hash → compare).

Observed language/runtime/compiler behavior: `--grant-read
capability=path` binds ONE capability name to ONE file path, and the
realized value is the whole file as `[byte; up_to 64]`. A function
declares one capability, so one function reads one file; verifying N
chunks needs N capabilities/grants/functions (or N invocations with
different grants). Files larger than 64 bytes cannot be fully observed
(truncation/error behavior for oversize grants is undocumented from the
source side). There are no positioned reads, no partial grants, no
grant-per-invocation parameterization.

Minimal reproducer: verify two chunk files in one MNCS call. Each needs
its own `host_read()` under its own capability, i.e. two grants and —
since authority is per-function — awkward plumbing that scales with the
number of files, not the logic.

Required semantics: parameterized read grants (a read effect that takes a
bounded path/index argument within an authorized root), positioned and
partial reads, and documented oversize behavior, so bulk verification is
one function over many files.

Why the current behavior/API is insufficient: `store.read_verify` proves
the SHAPE (in-language file→digest verification works, dual effects
compose — a positive finding), but only one file per function. The
lifecycle driver verifies through argument-passed bytes instead, because
per-file capabilities do not scale to a store with thousands of chunks.

Safety/correctness implications: LOW-MEDIUM. Capability-per-file is
arguably GOOD least-authority design; the pressure is ergonomic/scale,
not safety. The real gap is the missing parameterized form: per-file
capabilities without a way to mint them programmatically.

Performance implications: one CLI invocation per file for in-language
reads (compounds P1-016).

Workaround used: lifecycle verification passes file bytes as corpus
arguments to pure MNCS validators (one batched invocation for N files);
`read_verify` covers the single-file in-language path as proof, tested
in `test_in_language_chunk_file_verification` (skips honestly if combined
read+crypto grants are refused).

What the language/stdlib/runtime should ideally provide: root-scoped
read capabilities with bounded path arguments
(`host_read_at(root_cap, index)`), positioned reads, and documented
oversize/truncation semantics.

Affected backend(s) / target(s), if known: all (grant-shape design).

Severity: moderate

Status: workaround (argument-passed bytes for bulk; single-file proof kept)
