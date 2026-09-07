# Architecture

## Purpose

`mncs-store` is a persistence substrate, not a conventional database with MNCS syntax. Its canonical abstraction is a typed persistent object whose committed representation is rooted in immutable, verifiable content.

## Layers

### 1. Logical object layer

A logical object is the stable referent used by applications. Its identity does not change merely because its value evolves, its representation changes, or another replica is created.

A generation resolves a logical object identity to a committed representation root plus metadata visible in that snapshot.

### 2. Representation layer

A representation descriptor states enough machine semantics to interpret bytes safely and deterministically. Depending on type it can include scalar type, shape, stride/layout, alignment, encoding, endianness, schema/type identity, representation version, and transformation requirements.

Representations are explicit because `bytes` alone are insufficient for machine-native reuse.

### 3. Content layer

Committed representation content is immutable and content-addressed. Large values are described by manifests/trees whose leaves are chunks. Unchanged chunks can be structurally shared across object generations and replicas.

Content identity answers "are these immutable bytes/structures the same?" Logical identity answers "is this the same evolving object?" The two must not be conflated.

### 4. Generation layer

Writes are assembled privately and become visible through an atomic generation commit. Readers bind to a stable generation/snapshot. A crash cannot make half a generation authoritative.

The exact commit journal/superblock mechanism is an implementation choice governed by RFC 0005 and RFC 0015.

### 5. Relationship and provenance layer

Objects can carry typed references to other persistent identities. Provenance records producers, source objects, transformations, verification evidence, and ancestry. These structures must remain inspectable without decoding an application's opaque payload.

### 6. Placement layer

An object or chunk can have zero or more placements: local NVMe, RAM cache, accelerator memory, remote worker, archive, and future media. Placement is mutable operational state and never part of logical identity.

Durability policy describes required persistence/replication conditions. A system must not report a durability level until the relevant barrier is satisfied.

### 7. Derived services

Indices, caches, query plans, and learned accelerators are derivatives over canonical store state. `mncs-index` is the primary indexing boundary. A lost index may degrade performance but cannot make canonical information unrecoverable.

## Read path

```text
object id + reader generation
          ↓
resolve committed representation root
          ↓
validate descriptor + capability
          ↓
select usable placement
          ↓
verify/load required chunks
          ↓
direct typed view OR explicit transformation
```

## Write path

```text
native value / transformation
          ↓
representation descriptor
          ↓
chunk + hash immutable content
          ↓
build manifest/root
          ↓
stage object/reference/provenance updates
          ↓
commit generation atomically
          ↓
publish derived-index update opportunity
```

## What is deliberately not fixed yet

The bootstrap RFCs avoid prematurely choosing a particular hash algorithm, chunk-size strategy, on-disk directory layout, consensus protocol, query optimizer, compression algorithm, or distributed consistency model. Those choices require measurement and implementation pressure while preserving the invariants.
