# Retention maintenance and production preparation

`EmbeddedStore.prune_generations` is the Store-owned mechanism. Environment
composes consumer requirements; it does not decide Store reachability. This
implementation enables isolated reclamation and read-only production planning.
It does **not** authorize production deletion, pick an Environment retention
window, or activate ambient cleanup.

## Preservation and policy

Current EmbeddedStore commits append immutable logical objects. Updating an
Environment session creates another bound revision; an older snapshot, claim,
event, checkpoint, handoff or receipt is not superseded physical garbage merely
because a newer record exists. All retained generations are roots, including
their older physical versions. Bindings, descriptors, manifests and node trees
remain canonical content, even when a derived lookup index exists.

Environment's Store replay source uses historical generations and a durable
cursor. Its maximum walk and re-baseline behavior do not authorize deleting
evidence. Other callers can explicitly request `objects_at(generation)` or
`domain_bindings_at(generation)`. Consumers must establish which cursors and
historical generations are protected. Store will not infer that policy from
opaque application payloads. Include the oldest needed generations in
`protected_generations`, or preserve all existing history with
`keep_last=(1 << 64) - 1`. An unavailable protected generation fails explicitly.

An explicit window retires generation visibility below the selected checkpoint,
except genesis. It does not delete logical objects from the surviving full
checkpoint. Consumers requiring those old **generation reads** need pins or a
separately verified preservation strategy. Do not equate surviving object
payloads with preservation of every historical generation mapping.

## Existing clients and bounded use

Ordinary read/write and read-only constructors acquire a shared kernel lease on
the existing immutable `meta` inode. Close releases it. A dedicated
`maintenance=True` constructor requires an exclusive nonblocking lease; any
live reader or writer makes it defer with `DENIED`/maintenance-busy. Conversely,
new normal handles defer during maintenance. Read-only admission creates no
lock file. Do not share a maintenance handle between concurrent caller threads.

The old unsafe pattern `normal_store.prune_generations(dry_run=False)` is now
explicitly denied. Apply needs a dedicated maintenance handle, `policy_id`, and
an external, previously nonexistent `audit_path` when artifacts are eligible.
`policy_id` records the owner's policy assertion; knowing a label is not an ACL
grant. Existing dry-run arguments and count fields remain supported.

The operation defaults to 8,192 file/entry visits, 64 MiB of file reads and
30 seconds, with accepted ceilings of 200,000 visits, 1 GiB and 900 seconds.
Analysis exhaustion yields `status=unknown`, with no unlink and no audit file.
Corruption raises integrity failure. Unknown artifacts and adaptive/v3 content
defer orphan-chunk collection. A partial deletion deadline returns
`status=partial`, not a verified full cleanup. Kernel I/O is not preemptible at
an artifact boundary; use an external execution limit as well.

Generation-only planning verifies generation-chain structure and typed records;
`verification_scope=generation-chain-only` is **not** a payload-integrity PASS.
Optional `reclaim_chunks=True` validates the complete retained v2 payload graph
before allowing any unlink. Only generations and proven unreferenced chunks are
collected. Other content, aliases, temporary transactions and sidecars are not
guessed disposable. Existing recovery owns interrupted transactions.

No durable in-Store mark/index becomes an independent source of truth.
When `mark_path` is omitted, a single invocation proves a fresh plan and returns
`status=unknown` if any resource ceiling is hit. When an explicit external
`mark_path` (outside the Store) is supplied, `prune_generations` records
self-verifying, crash-safe binary mark checkpoints bound to the Store's `meta`
inode/digest, genesis, `floor`, `keep_last`, `protected_generations`, and
`reclaim_chunks` flag across bounded slices without repeating finished phases or
re-reading already-verified unique chunks/nodes. Any policy or floor shift
invalidates stale progress; compatible `head` advancement under an unchanged
floor extends incrementally and requires exclusive `maintenance=True` +
nonblocking `publication.lock` revalidation (plus optional `expected_head` and
`expected_plan_digest` guards) before any deletion. The external bounded JSONL
audit records the plan, synced attempts and completed unlink barriers; an attempt
without a completion receipt is UNKNOWN, and audits are never replayed as deletion
authority.

`EmbeddedStore.create_backup`, `EmbeddedStore.verify_backup`, and
`EmbeddedStore.restore_backup` provide external SHA-256 manifest-sealed backup
capture, isolated verification, and atomic staging restore. A full production-scale
backup rehearsal has not yet been executed against the 31+ GB live Store.

## Read-only planning

Use an explicitly selected Store checkout and an existing Store. This example
does not grant a retention policy. Its window is hypothetical until approved:

```python
from pathlib import Path
from mncs_store import EmbeddedStore

with EmbeddedStore(Path(STORE_PATH), read_only=True) as store:
    plan = store.prune_generations(
        keep_last=4096, dry_run=True,
        protected_generations=PROTECTED_GENERATIONS,
        max_files=120_000, max_bytes=512 * 1024 * 1024, max_seconds=20,
    )
# Inspect status, scope, head, floor, pins and candidates. No deletion occurred.
```

Inventory filesystem metadata before requesting payload reachability. On the
2026-11-15 production sample, generation-only analysis read 290 MB in 6.65 s,
with a 479 MiB process peak including admission and inventory. All-payload
production reachability was not attempted. Its content exceeds the bounded
sweep's 1 GiB accepted read ceiling; it currently must defer. Do not increase
timeouts to disguise that limitation. A resumable verified marking algorithm
is a separate prerequisite for a full production content sweep.

## Production activation gates — not executed by this assignment

1. Obtain explicit authorization naming the Store, retention policy, protected
   generations, preservation obligations and activation window. No current
   Environment retention policy is implied by the replay cap. Preserve all
   ambiguous history. Record the policy reference in the execution authority.
2. Inventory all consumers through their existing Environment/Control/Fabric
   owners. Drain and fence writers and readers, including idle pooled handles
   and standalone processes. Upgrade/recycle every Store consumer. Older
   binaries do not participate in the new lease protocol: a successful flock
   alone cannot establish that an old reader is absent. Do not force-close
   foreign work or treat PID absence from one tool as a complete inventory.
3. With ingress fenced and a stable head, create a recoverable backup or
   filesystem preservation snapshot covering the **whole** Store, including
   metadata, generations, bindings, physical content and recovery state.
   Verify the backup head and retained generation reconstruction using the
   selected Store artifact. Perform a restore/reopen drill at an isolated path.
   A successful copy or generation-only dry-run is not backup verification.
4. Verify pending transaction recovery and retained payload integrity before
   cleanup. Preserve the original evidence if verification fails. Run bounded
   read-only planning on the exact approved pins and window; require complete
   eligibility, stable head and a self-contained floor. Check available memory,
   disk and execution capacity through the existing execution owner. Provide
   margin above the measured 479 MiB planning peak and space for backup plus
   bounded audit. Stop on UNKNOWN or an integrity failure.
5. Under the approved offline window or fully upgraded online protocol, open a
   dedicated maintenance handle. Recompute eligibility while exclusive; do not
   reuse a previous candidate list. Use the owner's audit/evidence directory,
   resource-limited existing job facility, a 60-second external batch limit,
   and explicit file/byte/time limits. A generation-only batch can use the
   planning limits above and `max_seconds=30`. Deadlines are safeguards, not
   correctness thresholds. Partial cleanup can require multiple fresh batches.
6. Apply **only** the approved operation. For generation-only activation:

   ```python
   with EmbeddedStore(Path(STORE_PATH), maintenance=True,
                      verify_on_open=False) as store:
       result = store.prune_generations(
           keep_last=APPROVED_WINDOW,
           protected_generations=APPROVED_PINS,
           policy_id=APPROVED_POLICY_REFERENCE,
           dry_run=False, reclaim_chunks=False,
           audit_path=NEW_EXTERNAL_AUDIT_PATH,
           max_files=120_000, max_bytes=512 * 1024 * 1024,
           max_seconds=30,
       )
   ```

   This is an activation template, not permission to run it against production.
   Chunk collection is not production-ready at the current footprint; do not
   switch it on without its separate complete reachability prerequisite.
7. Verify head equality, retained floor and pinned generation reads, ordinary
   transactions, current bindings, payload integrity, and Environment session,
   checkpoint, event, claim and handoff reconstruction. Reopen consumers so
   they rebuild derived caches. Report completed, partial and unknown audit
   outcomes separately before reopening ordinary ingress.

Interruption removes only a subset of eligible files; retained roots and head
remain valid. Reopen uses ordinary Store recovery. Preserve the audit, treat
uncompleted attempts as UNKNOWN, and compute a fresh plan before continuation.
If retained integrity or policy is disputed, keep ingress fenced and restore
the verified preservation snapshot through the owner-controlled restore
procedure. Do not copy arbitrary files over a live Store or roll back its head
while newer acknowledged writes exist.

## Automation readiness

Store owns the mechanism (`EmbeddedStore.prune_generations`, backup/verify/restore).
Environment can compose consumer replay pins (`StoreBackend.retention_pins`) and
evaluate non-destructive dry-run readiness (`StoreBackend.maintenance_readiness`
and `mncs-env store --inventory` / `--maintenance-readiness`) without inventing a
retention window or spawning a new daemon. No automatic deletion is enabled.

Resumable bounded v2 chunk reachability (`mark_path`) and isolated backup/restore
verification are implemented in Store, while adaptive/v3 content reclamation
continues to fail closed and defer (`status=unknown`). The remaining production
activation prerequisites are: explicit retention window/pin policy approval,
consumer inventory and offline drain/upgrade of legacy handles, a production-scale
verified backup and isolated restore rehearsal, and completing the resumable v2
reachability sweep under the approved offline/maintenance window. Until then,
automatic work may only inventory, plan dry-run, and defer on busy leases,
active claims, pending recovery, resource pressure, UNKNOWN proofs, or corrupt state.
