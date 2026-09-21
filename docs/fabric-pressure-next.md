# Fabric pressure after the Forge cutover

Forge is now the first ordinary consumer of the supported embedded Store
boundary. Fabric is the next pressure source, but its migration should begin
with one reusable Store capability rather than a repository-wide rewrite.

## Current Fabric authority map

Fabric currently keeps several canonical JSONL histories:

- controller lifecycle and dispatch history in `FabricLedger`;
- management and desired-state history in a second ledger;
- worker/process and transport observations in worker-owned histories;
- scheduled-work transitions as replayed ledger events;
- execution receipts and placement observations as typed JSON records.

Bundle transfer uses a separate staged directory and immutable published cache.
`target-evidence-index.json` and similar lookup projections are rebuildable from
the execution history. The current implementation has useful local locks,
`fsync`, atomic replacement, tail recovery, duplicate identity checks, and
bounded compaction, but a multi-ledger controller decision is not one Store
generation transition.

## Next Store capability to pressure

Fabric should pressure a **bounded transactional batch generation with snapshot
pins and retention-aware reachability**:

```text
controller decision
  ├── worker/lease state
  ├── placement/admission relation
  ├── bundle or execution receipt binding
  └── dispatch/recovery event
          ↓
one typed Store generation
          ↓
commit feed → mncs-index
```

The capability must provide:

1. one compare-and-transition decision over a bounded set of typed objects and
   generic relations;
2. domain-owned identities for workers, bundles, placements, attempts, and
   receipts, without Store interpreting Fabric policy;
3. snapshot reads that do not mix generations while a controller or index is
   rebuilding state;
4. stale-generation rejection without automatic retry;
5. crash recovery that chooses a verified old or new batch, not an individual
   ledger tail;
6. compaction/reclamation driven by verified generation reachability and
   retained snapshot pins, not by filename age or bare roots; and
7. a deterministic commit feed with object, relation, provenance, and root
   identities for the disposable Index projection.

The existing Store content tree, typed relation/provenance records, generation
publication, commit feed, and recovery evidence are the base. The missing
pressure is batch publication plus safe retention/reclamation at the same
semantic boundary. Fabric should supply adversarial workloads for worker
concurrency, placement races, receipt/bundle binding, interrupted compaction,
and rebuild-after-index-deletion once that capability is added.

The current admitted research-bytecode path also remains a reusable Store/runtime
performance blocker for objects beyond the measured 1 KiB proof range. The host
may transport bounded file windows, but native SHA remains the authority; this
must not be papered over with a host-side digest or a second Store representation.
