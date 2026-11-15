# RFC 0021 — Bounded retention maintenance

Status: accepted for this bounded implementation. Amends RFCs 0005, 0015 and
0017. It does not establish a production Environment retention window.

## Problem and decision

The existing generation prune only held the publication lock, which excluded
writers but not historical readers. It accumulated candidate lists without a
resource bound, provided no crash audit, and reclaimed no content. A replay
walk bound is not evidence-retention authority.

The owner explicitly selects a keep-last window, protected generation pins,
and a policy identifier. MNCS `store.recovery.v1.retention_floor` rounds the
window and oldest pin down to an existing full checkpoint. The host verifies
that checkpoint and retained generation chains before unlinking files. A pin
conservatively preserves its entire checkpoint suffix, never just one delta.
Genesis and all generations at/above the floor survive. No committed generation
bytes are rewritten. No policy is inferred from age or a current-head reference.

`policy_id` is a caller assertion for audit, not a new ACL or authentication
token. File access and the owning consumer's authorization still apply. The
default dry-run identifies candidates without granting deletion authority.
Apply now requires explicit policy, a dedicated exclusive maintenance handle,
and an external audit file. This intentionally refuses the old unsafe apply
pattern; existing dry-run arguments and count fields remain compatible.

## Reader, writer and recovery safety

Every updated EmbeddedStore handle holds a shared kernel flock on the existing
immutable meta inode until close. An explicit maintenance handle instead
requires a nonblocking exclusive lock. Readers and writers defer while it is
held; maintenance defers while any other handle is alive. This also works for
read-only consumers without creating a lock file or writing the Store. Meta
must not be replaced during online operation. Apply additionally holds the
publication lock, excluding writes through the maintenance handle. Maintenance
handles are dedicated and must not be shared with concurrent caller threads.

Old binaries do not participate in this protocol. An offline drain and verified
client upgrade are mandatory before applying to any pre-existing deployment.
Locks do not discover durable replay cursors: the owner must supply all pins or
preserve the relevant history. No reader registry becomes generation authority.

Apply proves eligibility before any unlink. Pending transactions cause dry-run
deferral; normal writable reopen retains the existing recovery protocol. A
fresh plan is computed on every invocation. A saved audit is never replayed as
deletion authority. Old generation files are removed before chunk files. A
crash can leave a subset of eligible artifacts, but cannot remove retained
roots. Reopen needs no reclamation repair or head change. Repeating maintenance
recomputes from the authoritative remaining files. Derived lookup caches are
invalidated even after interruption; Environment's binding-directory caches
are closed with their leased provider before exclusive admission is possible.

## Reachability and verification bounds

Generation-only pruning verifies retained generation headers, exact parents,
typed records and checkpoint boundaries. It does not claim a payload scrub:
`verification_scope=generation-chain-only`. Objects, bindings and content are
untouched. Production preparation must separately verify retained payloads.

Optional orphan-chunk reclamation additionally traverses every retained v2
physical root using the existing binding, manifest, descriptor, node, chunk,
length and native digest checks. Immutable physical roots are deduplicated.
The host unions verified chunk names; existing native `prune_decide` owns the
unreferenced verdict. Only regular canonical chunk filenames absent from the
complete union are eligible. Logical objects are append-only in the current
implementation: old session revisions are not disposable because a newer one
exists. No binding, object manifest, node, descriptor, sidecar or temporary
transaction file is collected by this API. Adaptive/v3 payload reachability
is explicitly UNKNOWN/deferred; it is not approximated by a base-only scan.

Analysis has explicit file-operation, byte and elapsed-time budgets, with
hard accepted bounds. Its sets/lists are capped by the file budget; one payload
is materialized at a time, bounded by the byte budget. No partial proof permits
unlink. Budget/unsupported-format/head-change outcomes are UNKNOWN. Corrupt
committed content raises integrity failure. Deletion checks the deadline at
each artifact boundary and can return partial completion; individual kernel
I/O/fsync operations remain subject to an external process budget. Large
stores can complete generation-only analysis separately; a content sweep that
exceeds the verified budget must defer, not bypass proof or just raise timeouts.

## Audit and durability

One bounded owner-selected JSONL audit outside the reclaimed Store is created
exclusively. Before deletion it durably records the deterministic plan digest,
head, floor, policy, pins and exact candidates. Each attempt is synced before
unlink; the containing directory is synced before recording completion. A
crash between these records leaves that attempt UNKNOWN, never a false receipt.
The audit is transport evidence, not canonical generation state or a resumable
authority file. Normal reads/writes consult no audit or mark index. Existing
filesystem durability limitations continue to apply.

## Alternatives rejected and remaining pressure

Current-head-only GC would lose old physical versions. Wall-clock age and the
4096 replay limit cannot authorize evidence loss. In-place checkpoint rewriting
would violate immutable committed bytes. A service, duplicate history database,
or production migration is unnecessary for this slice. Fully incremental
verified marking for stores exceeding bounded content-sweep budgets, adaptive
reachability and consumer-owned replay-pin policy remain explicit follow-ups.
No ambient deletion is enabled.
