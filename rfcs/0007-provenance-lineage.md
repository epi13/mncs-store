# RFC 0007 — Provenance and lineage

**Status:** Accepted for architecture

## Problem

MNCS increasingly produces derived artifacts through compilers, models, agents, ingest transforms, verifiers, and distributed workers. Persisting only the final value discards information needed for reproducibility, debugging, learning, and security.

## Decision

Provenance is a first-class, integrity-bindable persistent structure. It can describe:

- source/input Object IDs and representation roots;
- producer identity/type/version;
- transformation or operation identity;
- execution context relevant to reproducibility;
- generation/time ordering;
- verifier/attestation evidence;
- parent/ancestry links;
- declared confidence or quality metadata when domain-appropriate.

Not every object must carry every field. Provenance schemas are typed and versioned rather than an unstructured metadata bag.

## Integrity

When provenance is presented as authoritative evidence, it must be cryptographically or structurally bound to the state it describes. Mutable annotations may exist but must be distinguishable from committed evidence.

## Privacy and capability

Provenance can reveal sensitive inputs, worker locations, model identities, or user activity. Access to provenance is capability-controlled independently where necessary; "first-class" does not mean globally visible.

## Ecosystem use

`mncs-memory`, `mncs-learn`, compiler tooling, Atlas, actions, micro-verifiers/debuggers, and security tooling should be able to reuse the same lineage substrate rather than each inventing bespoke JSON logs.
