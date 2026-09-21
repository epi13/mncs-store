# Store pressure reconciliation

The historical Store pressure ledger is a record of what was once missing,
not a permanent capability contract. The current campaign classifies the old
claims against the implementation that is now admitted by the supported
embedded boundary.

## Resolved or stale claims

| Historical claim | Current fact | Classification |
|---|---|---|
| no incremental SHA-256 (`P1-004`) | `store.content.v1` carries `mncs.std.sha256.v1.DigestState` through bounded transport/file segments | stale capability claim; keep the old record as history only |
| digest is only a 64-byte effect (`P2-001`) | payload identity is streamed across immutable chunks; a single MNCS value is never used for the full object | stale representation claim |
| sequence bound is 64 (`P1-005`) | sequence bounds apply to each bounded content window; the object is a chunk/tree composition | stale object-size claim |
| no retained in-process session (`P1-016`) | the supported adapter retains one Store application session | stale boundary claim |
| append-only files only (`P2-004`) | Store publication uses staged immutable files plus atomic head replacement | stale host-realization claim |
| no atomic mutation (`P2-005`) | compare-and-transition is decided by `store.generation.v1`; the host serializes publication and performs atomic replace | stale realization claim |
| no synchronization / directory durability | the local realization issues file and directory durability barriers where the platform supports them | stale realization claim; power-loss evidence remains platform-scoped |
| Store requires Ingest | typed handoff is now optional and scoped to import/migration | stale contract-surface claim |

## Remaining obligations

The remaining work is consumer and measurement work rather than another Store
representation family:

- Forge's ordinary path is cut over; differential parity and historical import
  remain scheduled regression obligations.
- The bounded research-bytecode realization is semantically correct but has
  no usable large-object throughput: the transport-only native-window probe
  exceeded ten minutes at 32 KiB. The reusable blocker and measured rows are
  recorded in `docs/scalable-content-measurement.md`.
- Store's generic relation representation is consolidated in
  `src/store/relationship.mncs`; no v1/v2 source family remains active.

No native Forge state is routed through Ingest. Index remains a rebuildable
projection and is not part of Store recovery authority.
