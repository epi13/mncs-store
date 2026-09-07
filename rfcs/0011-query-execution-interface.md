# RFC 0011 — Query execution interface

**Status:** Accepted for architecture

## Problem

SQL would pull the storage model toward tables even when callers operate on tensors, graphs, typed structs, model artifacts, or provenance. At the same time, applications need composable filtering/projection without manually scanning everything.

## Decision

The native query boundary is a typed MNCS expression/plan over object metadata, types, relationships, provenance, and values where supported. Query syntax is owned with `mncs-language`; store defines execution semantics and provider contracts rather than creating "MNCS SQL."

Conceptually:

```text
store.scan<Artifact>(snapshot)
  .where(target == ptx)
  .where(producer == compiler)
  .map(performance.compile_time)
```

The compiler/runtime may lower this to index lookups, metadata scans, chunk reads, vectorized execution, or remote plans.

## Requirements

- execution is bound to an explicit/stable reader generation;
- capability checks apply during planning and materialization;
- plans cannot infer canonical facts from stale derived indices without declaring that limitation;
- projections should avoid materializing unused payload regions when representation metadata permits;
- deterministic queries over the same committed generation and deterministic functions should have deterministic logical results.

## Deferred

Cost models, distributed query scheduling, user syntax, joins/graph traversal syntax, vector operators, and learned plan selection require later pressure and RFCs.
