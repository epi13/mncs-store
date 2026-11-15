# Store historical retention recovery

This assignment delivers a bounded, policy-governed extension of the existing
Store prune API, with verified orphan-chunk collection for v2 content. Production
was inventoried and generation-planned read-only. **No production deletion,
pruning, compaction, migration, replacement or automatic cleanup was performed.**

## Stage A — authority, preservation and production accounting

The implementation began at Store `5464f04` and Environment `2a4d0ff`. Native
MCP entry refused the project's configured workspace as outside its execution
grant. The documented standalone CLI entry succeeded, created
`ses_dfe7e716770e3f7c`, and acquired the exact new Store worktree claim recorded
in `scope.json`. Required service probes were ready; overall entry readiness
remained degraded with 29 UNKNOWN capabilities. This work did not suppress them.
No other repository was modified.

At observed head **131859**, bounded filesystem metadata passes found:

| Component | Files | File bytes | Allocated file blocks |
| --- | ---: | ---: | ---: |
| Chunks | 377,315 | 18,236,641,701 | 18,575,077,376 |
| Generations | 97,557 | 13,122,984,396 | 13,502,672,896 |
| Nodes | 135,179 | 21,497,948 | 553,693,184 |
| Physical manifests | 132,140 | 22,199,520 | 541,245,440 |
| Descriptors | 132,138 | 13,836,406 | 541,237,248 |
| Bindings | 132,137 | 22,557,487 | 541,233,152 |
| Remaining small files | 49 | 34,552 | 188,416 |
| **Total** | **1,006,515** | **31,439,752,010** | **34,255,347,712** |

These are decimal bytes; file-block accounting excludes filesystem inode,
directory/index metadata and backup/snapshot sharing. They are not a filesystem
free-space forecast. The first full metadata pass stopped at its 15-second
budget while counting chunks. A subsequent chunks-only pass completed in 1.02 s
at the same unchanged head. `accounting.json` explicitly combines the completed
non-chunk components with that pass; the original partial observation remains
in `physical-inventory.json`. No historical object payloads were scanned.

A separate 15-second-capped generation-header inventory completed in 12.39 s:
**2,475 MG full snapshots occupy 13,108,117,132 bytes; 95,082 MD deltas occupy
14,867,264 bytes.** The initial arithmetic estimate based only on checkpoint
ordinals was insufficient: older full snapshots also occur between the modern
256-generation checkpoints. The current format already supports those legacy
full generations; no generation format was redesigned here.

Generations 1–34303 are already absent; genesis and a self-contained checkpoint
at 34304 remain. This pre-existing gap was observed, not created by this work.
Its original authorization was not independently established. It disproves a
claim that production history has never been pruned, but does not authorize
another prune. The new API refuses an explicitly protected generation that is
already unavailable.

Preservation obligations follow the actual consumers:

- Current generation roots retain immutable Environment session revisions,
  claims, events, checkpoints, handoffs, evidence and section records. A newer
  revision does not remove the old logical binding. Store cannot infer an
  application's evidence-retention policy from age or opaque payload contents.
- Retained generation reads are public API. Environment Store replay uses an
  older generation cursor, `domain_bindings_since`, and historical fallback.
  Its 4096 walk limit and re-baseline behavior do not grant permission to delete
  other evidence. Protected generations conservatively preserve their entire
  checkpoint suffix. Ambiguous history must remain protected.
- Bindings, manifests, descriptors, node trees and content chunks are canonical
  dependencies, not disposable derived indexes. Pending transactions introduce
  recovery roots and make analysis defer. In-memory generation/binding caches
  are reconstructible and remain subordinate to disk authority.

The accounting therefore distinguishes confidence from size:

| Classification | Evidence-backed result |
| --- | --- |
| Known necessary lower bound | 16,385,668 bytes: current checkpoint/delta chain, genesis, meta and head. Payload/binding dependencies additionally remain protected; their exact necessary-byte union was not scanned. |
| Known authorized production reclaimability | **0 bytes identified and authorized**. No approved production window/pin policy or complete chunk reachability proof exists. |
| Hypothetical window candidates | 93,440 generation files / 12,848,515,320 bytes, **conditional on an approved 4096 window with no older pins**. This is part of the uncertain category, not authorized savings. |
| Uncertain/protected remainder | 31,423,366,342 bytes beyond the necessary lower bound. This includes the hypothetical candidates; do not add them twice. |
| Known temporary artifacts | No staging entries or `.stage` files observed in the completed metadata passes. No temporary-byte reclamation is claimed. |
| Derived/reconstructible canonical-file bytes | None proved disposable. Resident lookup caches add no persistent mark/index files. |

### Why growth continued

The existing prune removes generation files only, retains genesis and a full
checkpoint suffix, and never collects chunks. Call-site inspection found the
Environment forwarding methods and tests, not an ambient retention-policy
invocation. An available API is not an activated maintenance policy.

Logical bindings accumulate by design. Every 256th commit copies the complete
entry/relation/provenance inventory into another full snapshot, so checkpoint
size grows with the entire binding set. The sampled current checkpoint is
16,381,788 bytes; continued full checkpoints retain cumulative metadata. Legacy
full snapshots account for additional historical amplification. Content-address
reuse avoids identical chunks but does not eliminate distinct old snapshot
sections, events or evidence that remain committed. Small canonical files also
incur substantial allocation overhead. No evidence-retention rule makes these
objects disposable merely because sessions have ended.

## Stage B — bounded reclamation mechanism

See [RFC 0021](../../rfcs/0021-bounded-retention-maintenance.md) and
[the operational contract](../../docs/retention.md).

The existing `EmbeddedStore.prune_generations` now accepts protected generation
pins, an explicit policy assertion, file/byte/time budgets, optional verified
v2 chunk collection, and an external audit path. The actual floor calculation
and orphan verdict execute native `store.recovery.v1` functions; Python performs
bounded filesystem transport, existing structural parsing and orchestration.
There is no new storage manager, background service or independent history
authority. The 15-line native addition is active policy computation, not a
receipt-only facade.

Ordinary handles hold shared kernel leases on the existing immutable meta inode;
dedicated maintenance requires an exclusive, nonblocking lease. Read-only opens
create no lock files. Apply also takes the publication lock. This excludes
other updated readers/writers and serializes mutations through the maintenance
handle. Old binaries require an offline upgrade/drain; locks alone cannot
discover their readers or durable replay cursors.

Apply first completes its proof, then durably records a deterministic external
audit plan. It removes eligible old generation files before orphan chunks.
Each unlink has a synced attempt record and a directory barrier before its
completion record. Process death can leave an UNKNOWN attempt and a subset of
eligible files, but leaves retained state and head intact. Reopen and retry use
the existing Store recovery and a fresh eligibility proof; audits are never
replayed as authority. Relevant caches are invalidated even on interruption.

The chunk proof visits every retained generation's distinct physical root,
validates its binding/manifest/descriptor/tree/payload with existing native
digest checks, and unions referenced chunk names. Unknown names, symlinks,
pending recovery, exhausted budgets, changed head and adaptive/v3 reachability
defer without unlink. Corrupt committed content fails. Only generations and
proven orphan chunks are collected; other artifact families remain untouched.

Generation-only analysis intentionally verifies chain structure without reading
all payloads. Its `verification_scope=generation-chain-only` is not a payload
integrity PASS. Chunk reclamation requires the complete v2 payload proof.
The existing unsafe apply call on a normal handle is explicitly denied; dry-run
arguments/count fields remain supported. `policy_id` is traceable owner input,
not a token granting authority to an otherwise unauthorized caller.

## Stage C — correctness, faults and measurements

The complete Store suite passed **308 tests in 225.23 s**, no skips, under a
600-second external limit. Command wall was 225.59 s, user/system CPU
210.53/9.33 s, peak process RSS 764,840 KiB. After adding three further cases,
the final targeted run passed **21 tests in 11.38 s**: all 19 retention tests
plus the two existing generation-prune tests. The earlier 18-test targeted
run is also retained. The final test additions and the nonblocking maintenance
recovery correction were checked by the 21-test run; a single final 311-test
full invocation is not claimed. Recent practical test throughput is preserved.

Verification includes:

- Multiple checkpoint boundaries, exact historical reads, protected older
  generations, shared chunks, deterministic plans, read-only non-mutation,
  genuine uncommitted chunks, unchanged head and idempotent second cleanup.
- Native floor/orphan functions and seven new independent policy vectors in
  the existing recovery corpus across its existing backend matrix. The full
  suite retains all reference/oracle and backend-refusal requirements.
- Committed chunk, binding and checkpoint corruption fails before deletion;
  budgets return UNKNOWN with zero unlinks. Unknown filenames, symlinks,
  pending transactions and adaptive older physical versions are preserved.
  Adaptive-only old physical reachability is deliberately **not** claimed
  solved: the native adaptive history test verifies safe UNKNOWN/refusal.
- A spawned historical reader remains stable while another writer commits and
  blocks exclusive maintenance. Maintenance blocks new readers/writers in the
  same and separate processes. Kernel leases release after process death and
  failed constructor tracebacks do not pin the Store. A simulated legacy writer holding only the publication lock also makes maintenance recovery defer nonblockingly rather than wait outside its budget.
- A worker exits 137 after unlink but before its receipt. Reopen reconstructs
  retained generations; a fresh retry completes and a subsequent ordinary
  transaction succeeds. Another native test interrupts normal publication,
  automatically recovers the old head, and reclaims only the new orphan chunk
  while keeping a shared committed chunk.
- The real Environment consumer preserves snapshots, event sequences, active
  and historical claim rows, checkpoint and handoff records across maintenance.
  Its binding directory is cleared on close; an open provider blocks admission.
  This verifies interaction with `2a4d0ff` without changing Environment code.

The eight existing Environment binding-directory tests also passed in 13.65 s;
the maintenance-forwarding test passed separately. An initial cross-repository
test invocation correctly refused mixed Store checkouts (five failures/three
passes). Selecting the tested checkout explicitly with `MNCS_STORE_PYTHON` and
`MNCS_STORE_ROOT` resolved the harness mismatch; no authority check was relaxed.
Raw failed and corrected results are retained. E/F lint on the new test and
implementation files, Python compilation and `git diff --check` pass. One old
unused-variable warning in the existing filesystem test file was left unchanged.

### Isolated platform scaling

The optional `benchmark.py` uses the existing deterministic semantic-session
double to isolate filesystem/metadata costs; it is **not** presented as native
execution performance. Native semantics and real Environment reconstruction
are verified separately above. Baseline loads `embedded.py` from `5464f04` only
against new temporary fixtures. Both paths use identical generated workloads,
including 32 shared live chunks and 32 chunks from an interrupted transaction.
The legacy fixture uses the existing full-generation encoding at every commit;
the delta fixture uses the current 256 checkpoint interval. No live data is
copied or rewritten to create either fixture.

| Layout / generations | Old prune wall / CPU | Verified reclaim wall / CPU | File bytes removed, old → new | Allocated blocks removed, old → new |
| --- | ---: | ---: | ---: | ---: |
| Delta / 32 | 0.00005 / 0.00005 s | 0.01256 / 0.01250 s | 0 → 2,097,152 | 0 → 2,097,152 |
| Delta / 1024 | 0.02438 / 0.02410 s | 0.33172 / 0.33014 s | 214,620 → 2,311,772 | 3,231,744 → 5,328,896 |
| Legacy full / 1024 | 0.02666 / 0.02625 s | 0.66400 / 0.65993 s | 36,539,880 → 38,637,032 | 38,113,280 → 40,210,432 |

The verified operation performs more work than the unsafe generation-only
baseline. It adds complete retained v2 payload verification and durable audit.
The large cases perform 799 unlinks and 2,400 explicit fsync calls, versus 767
unlinks/one directory sync before. These comparable filesystem counters measure
the maintenance phase; other raw open/stat counters are selected Python-level
calls, not an aggregate kernel syscall profile. Persistent in-Store metadata
overhead is **zero new files**. External audits are bounded: 10,503 bytes for
the small case and 139,372/142,259 bytes for large delta/legacy cases.

Large delta baseline/final process peaks were 55,140/55,256 KiB; legacy peaks
55,132/55,332 KiB. Small peaks were 40,188/39,260 KiB. These include fixture and
lookup preparation, not just the reclaim call. No universal memory reduction
is claimed. Raw JSON includes before/after file bytes and allocation, CPU,
peak RSS, filesystem counters and lookup/transaction/recovery timing.

For 1024-generation delta fixtures, normal reopen was 18.24 → 16.27 ms,
interrupted-transaction recovery 267.26 → 270.91 ms, first current lookup
1.523 → 1.309 ms, 20 warm lookups 3.142 → 3.171 ms, historical binding lookup
32.78 → 31.58 ms, and an ordinary transaction 1.283 → 1.231 ms. After cleanup,
current/historical lookup was 0.359/32.06 ms. Legacy results are similar in the
raw artifacts. These single-run samples do not establish statistical speedups;
they show no new full-history maintenance work in routine operations. Read
results before/after are compared, not inferred from a cleanup status field.

## Stage D — production preparation and remaining limitations

The real generation-only dry-run at stable head 131859 completed in **6.65 s**,
reading **290,318,656 bytes** across 101,675 counted visits. The entire diagnostic
including header inventory/admission took 20.79 s and peaked at 490,512 KiB.
It removed **zero files/bytes** and had `policy_asserted=false`. The hypothetical
window would leave 18,591,236,690 current file bytes before any later writes,
but this is neither approved savings nor a guarantee about filesystem free
space. Payload integrity remains unscanned/UNKNOWN in production.

The exact non-destructive preparation and activation gates are in
`docs/retention.md`: explicit policy/pins and authorization, consumer drain and
upgrade, verified whole-Store preservation plus restore drill, stable head and
complete eligibility, bounded execution/audit, interruption handling, rollback
through the owner and post-cleanup data/consumer verification. A generation
batch may stop partially and be reproved; no saved candidate list can authorize
a later delete. No activation command was run on production.

Store owns reclamation; Environment can later compose consumer pins and invoke
it through its existing maintenance boundary. No daemon is needed. Automatic
deletion remains **disabled**, pending production policy, legacy-client drain
and activation approval. Idle pooled providers hold shared leases and must be
closed by their owner before maintenance can enter; an idle pool is not proof
that history is disposable.

The bounded content collector cannot yet sweep this 18 GB production content
set under its 1 GiB read ceiling. Resumable verified marking and adaptive/v3
reachability are explicit follow-ups. Do not work around them with an
unbounded timeout or a current-head-only scan. Logical binding retirement is
also not implemented or authorized; committed historical application records
continue to consume space even after generation-file pruning. Lossless archival
representation work would require its own immutable-format/reader contract,
not an in-place rewrite of old checkpoints.

## Delivery

See `delivery.json` for published commits and ref readback. Only Store source,
tests and documentation changed. Environment `2a4d0ff`, Control, Language,
foreign claims, existing worktrees and unrelated source were preserved. New
Store handles select the updated native artifact through the existing bounded
admission cache; no Cargo rebuild, Store migration, new persistent index or
automatic maintenance activation is required. Existing long-lived clients must
be drained/upgraded before any authorized production reclamation.
