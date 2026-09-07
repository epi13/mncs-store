# Normative invariants

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are used normatively.

## Identity and content

1. A logical object ID **MUST** remain distinct from content/chunk identity.
2. Committed chunks **MUST** be immutable.
3. Content identity **MUST** be derived deterministically from the canonical bytes/structure covered by that identity.
4. Reusing identical content **MUST NOT** merge distinct logical object identities.

## Representation

5. A stored representation **MUST** have a versioned descriptor sufficient for safe interpretation.
6. Size/shape/stride calculations **MUST** reject overflow and impossible layouts.
7. A representation requiring transformation **MUST NOT** be exposed as if it were a compatible zero-copy view.
8. Unknown representation versions **MUST** fail explicitly rather than guess.

## Generations

9. Readers bound to a committed generation **MUST** observe a stable mapping for the lifetime of that snapshot.
10. Uncommitted content **MUST NOT** become authoritative merely because chunks reached durable media.
11. A durability acknowledgment **MUST NOT** precede the durability barrier promised by the selected policy.
12. Recovery **MUST** choose a verifiably committed state; it **MUST NOT** synthesize a mixed generation from partially persisted metadata.

## References and provenance

13. First-class references **MUST** identify their target namespace/object semantics without requiring payload parsing.
14. Deleting or making an object unreachable **MUST NOT** silently rewrite historical snapshots that still reference it.
15. Provenance, when claimed as authoritative, **MUST** be integrity-bound to the state/evidence it describes.

## Placement and replication

16. Moving or replicating content **MUST NOT** change logical identity.
17. A replica **MUST** verify canonical integrity before becoming authoritative for reads that require verified content.
18. Cache loss **MUST NOT** imply canonical data loss.
19. Index loss **MUST NOT** imply canonical data loss.

## Security and access

20. Capability checks **MUST** occur before exposing payload, metadata that is access-controlled, or mutable operations.
21. Zero-copy access **MUST** preserve bounds, lifetime, alignment, type, and mutability guarantees.
22. External descriptors, manifests, peers, and import formats **MUST** be treated as untrusted input.

## Boundaries

23. JSON, SQL rows, protobuf messages, filesystem paths, or other interchange forms **MUST NOT** become required canonical representations unless a future RFC explicitly replaces this invariant.
24. Export adapters **MAY** be lossy only when the loss is explicit to the caller.
25. `mncs-index` and other derived systems **MUST NOT** be required to reconstruct information omitted from canonical store state.
