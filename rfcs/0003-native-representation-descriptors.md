# RFC 0003 — Native type and representation descriptors

**Status:** Accepted for architecture

## Problem

`key → bytes` preserves storage but not machine meaning. Safe direct reuse requires knowing how those bytes represent a value.

## Decision

Every canonical representation root has a versioned descriptor sufficient to interpret it deterministically. Descriptor fields vary by representation class but can include:

- MNCS type/schema identity;
- scalar/element type;
- dimensions and shape;
- stride/layout/order;
- alignment;
- endianness;
- encoding/compression;
- nullable/sparse/adjacency semantics;
- representation version and compatibility requirements.

Descriptors are themselves canonical, bounded, and integrity-covered. Size and offset calculations must be checked for overflow before memory allocation or view creation.

## Representation classes

The architecture must permit classes such as dense/sparse tensors, struct arrays, vectors, graph/adjacency blocks, token sequences, model weights, executable artifacts, bitmaps, and ordinary scalar/aggregate MNCS values without forcing them through a single document tree.

## Compatibility

A reader may expose a direct view only if the stored descriptor is compatible with the requested native view. Otherwise it must perform or request an explicit transformation. Unknown versions fail explicitly.

## Security

Descriptors are untrusted until validated. Malicious dimensions, strides, compression metadata, or nesting must not cause overflow, excessive allocation, or out-of-bounds access.
