# Terminology

**Logical object** — an evolving persistent entity with stable identity across committed versions.

**Object ID** — identifier of a logical object. It is not a content hash.

**Content identity** — deterministic identity of immutable canonical content, typically derived cryptographically.

**Chunk** — immutable content-addressed storage unit.

**Manifest / root** — immutable structure that identifies the chunks and representation data forming a committed value.

**Representation descriptor** — versioned machine-readable description required to interpret a representation safely: type/layout/shape/alignment/encoding and related facts.

**Generation** — atomically committed mapping/state transition visible as a stable snapshot.

**Snapshot** — reader view bound to one committed generation.

**Placement** — physical availability of a representation/chunk on a medium or node. Placement can change without changing logical identity.

**Durability policy** — conditions that must be satisfied before a write may be acknowledged at a requested persistence level.

**Reference** — first-class relationship from one persistent object/state record to another identity.

**Provenance** — evidence describing where state came from: sources, producer, transformation, ancestry, verification, and related execution facts.

**Canonical state** — information whose loss changes the meaning of persisted MNCS state.

**Derived state** — rebuildable acceleration or projection such as many indices and caches.

**Direct view** — safe typed access to stored bytes without semantic deserialization/copy into another representation.

**Transformation** — explicit conversion between incompatible representations.

**Boundary format** — external/interchange representation such as JSON, SQL, CSV, protobuf, Parquet, or a conventional filesystem encoding. Boundary formats are not implicitly canonical.
