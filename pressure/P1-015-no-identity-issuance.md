# P1-015 — No unique-identity issuance primitive (ObjectId serials are host-issued)

Storage feature being implemented: logical ObjectId issuance — minting a
fresh (namespace, serial) pair that has never been used (RFC 0002
issuance mechanism, deliberately deferred there).

Observed language/runtime/compiler behavior: no randomness, UUID,
monotonic-counter, or compare-and-swap primitive is reachable from MNCS
source. `mncs.core.random.v1` documents itself as deterministic
transparency, not uniqueness; `clock_read` is wall time (explicitly
unpinned, skew-tolerant) and unsuitable for identity; there is no atomic
or persistent-state primitive at all (functions are pure over their
arguments). An MNCS function cannot mint a fresh identity: same inputs
always yield the same outputs, by design.

Minimal reproducer (mncs-language): any `fn fresh_id() -> u64` with no
inputs — either it returns a constant (not fresh) or it cannot be
written. Freshness requires state or entropy; the language offers neither
to source programs.

Required semantics: a capability-gated uniqueness source (monotonic
serial allocation, UUID generation, or CAS-backed counter) with explicit
durability of the allocation itself (a serial that is issued but whose
object never commits must not be silently reusable — or the reuse policy
must be explicit).

Why the current behavior/API is insufficient: Phase-1a serial issuance
lives in the host driver's `meta.next_serial` counter file, with
torn-write healing (`max(objects)+1` scan on open) implemented in
unaudited host code. The most collision-sensitive operation in the store
— the one that keeps distinct logical objects distinct — is outside the
language.

Safety/correctness implications: MEDIUM. A host counter bug (reuse after
a torn meta write, concurrent creators) merges logical identities
silently. The O_EXCL guard on `objects/` converts the worst case into a
loud failure, but issuance itself deserves language-level authority.

Performance implications: none at this scale.

Workaround used: host-owned `meta` counter (16-byte file, fsync'd per
commit) + `O_EXCL` creation guards + open-time serial healing scan, all
in `store_phase1a.py` and labeled as host mechanics.

What the language/stdlib/runtime should ideally provide: a uniqueness
capability (`fresh_serial(namespace)`, `uuid_v4`, or durable atomic
counter) with stated crash semantics for the allocation record.

Affected backend(s) / target(s), if known: all (surface absence).

Severity: moderate

Status: workaround (host counter file + exclusive-create guards)
