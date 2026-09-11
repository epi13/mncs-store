# mncs-store RFCs

RFCs record storage semantics that affect more than one implementation unit or consumer. The initial suite is **Accepted for architecture**: it defines the intended contract while implementation remains incomplete.

| RFC | Title | Status |
|---|---|---|
| [0001](0001-machine-native-storage-model.md) | Machine-native storage model | Accepted |
| [0002](0002-persistent-object-identity.md) | Persistent object identity | Accepted |
| [0003](0003-native-representation-descriptors.md) | Native representation descriptors | Accepted |
| [0004](0004-content-addressed-chunks.md) | Content-addressed chunks | Accepted |
| [0005](0005-generations-snapshots-transactions.md) | Generations, snapshots, and transactions | Accepted |
| [0006](0006-object-relationships.md) | Object relationships and references | Accepted |
| [0007](0007-provenance-lineage.md) | Provenance and lineage | Accepted |
| [0008](0008-placement-durability.md) | Placement and durability | Accepted |
| [0009](0009-zero-copy-access.md) | Zero-copy and memory-mapped access | Accepted |
| [0010](0010-index-provider-interface.md) | Index provider interface | Accepted |
| [0011](0011-query-execution-interface.md) | Query execution interface | Accepted |
| [0012](0012-replication-distributed-stores.md) | Replication and distributed stores | Accepted |
| [0013](0013-capability-security-model.md) | Capability and security model | Accepted |
| [0014](0014-import-export-boundaries.md) | Import/export boundaries | Accepted |
| [0015](0015-recovery-verification.md) | Recovery and verification | Accepted |
| [0016](0016-phase1-canonical-encodings.md) | Phase-1 canonical encodings v1 | Accepted for implementation |
| [0017](0017-phase2-multichunk-generations-recovery.md) | Multi-chunk objects, generation commits, recovery | Accepted for implementation |

A future RFC may supersede an accepted RFC, but implementations must not silently diverge from accepted semantics.
