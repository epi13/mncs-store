# Adaptive physical representations

Stable semantic identity, adaptive physical representation, minimum
necessary materialization. This page describes what is implemented;
[RFC 0019](../rfcs/0019-adaptive-representations.md) holds the normative
invariants.

## Model

```text
logical object (stable 12-byte identity)
  ├─ envelope (256 bytes, content-addressed, inspectable alone)
  │    ├─ identity, type, synopsis pointer
  │    ├─ representation inventory + fidelity bitmap
  │    ├─ block topology + default costs
  │    └─ provenance, generation
  ├─ representations (foundation: base exact over the chunk tree)
  │    ├─ [0] base: exact identity bytes, chunk-addressed, block-covered
  │    ├─ [1..] opaque blobs: synopses, coded payloads, alternates
  │    └─ each: fidelity, codec, stored/plain bytes, decode class, root
  ├─ block table (spans with digests, tags, dependency edges)
  └─ manifest binds envelope + tables by digest (v3; v2 synthesizes)
```

Logical identity never depends on which physical forms exist. Adding a
representation changes bytes on disk, never what the object *is*.

## Fidelity ladder

Levels are cumulative: each includes all lower information.

| Level | Name | Materializes |
|---:|---|---|
| 0 | identity | logical identity only, no bytes |
| 1 | synopsis | producer synopsis blob |
| 2 | structural | envelope, representation/block tables, no payload |
| 3 | normalized | canonical/alternate representation bytes |
| 4 | payload | decoded representation bytes |
| 5 | exact | bit-exact original bytes |

The envelope bitmap records which levels an object offers. The exact
flag on a representation additionally means Store *verified* byte
equality with the original; fidelity-5 claims over non-matching bytes
are refused at write.

## Access intent

Callers state constraints without naming implementations:
`store.intent.v1` carries required fidelity, latency/compute/memory/
transfer weights, expected frequency, lifetime, and an opaque locality
domain. Weights default to neutral when unspecified; unknown advisory
codes normalize instead of failing. No intent names a codec, a
sibling project, or a ranking model.

Selection is a total deterministic pairwise tournament
(`store.representation.rank`) folded over the stored table: validity
first, then satisfaction, then latency permission, then estimated
cost, then exact stored/plain/codec tie-breaks. When nothing fully
satisfies, the richer fidelity wins and the caller sees an explicit
`satisfied: false` rather than a silent substitution.

With no intent at all, Store normalizes to the default intent (exact,
unconstrained) and selects the cheapest exact form — usually the base
representation, since it has no decode cost.

## Cost model

`estimate_cost` combines stored bytes × transfer weight, plain bytes ×
memory weight, and decode-class × plain × compute weight with
saturating arithmetic (larger stays larger). Latency permission is reported separately from fidelity satisfaction and
is subordinate to it during ranking. It is not a weight: interactive budgets admit only copy/cheap decode
classes. All weights are relative costs, not physical units.

## Blocks and selective materialization

A representation's payload is partitioned into blocks: byte spans with
content digests, semantic tags, and dependency edges. Wanted sets are
u64 bitmasks over block indices:

- select by mask (`read_blocks`) or by semantic tag (`materialize`
  with `tag`), without touching payload;
- Store verifies the mask lies in the representation range, grows it
  to the dependency fixpoint (`closure_step`, monotone), verifies
  closure (`closure_closed`), and fetches only covered chunks —
  verifying every node and chunk on the traversed path exactly as the
  whole-object reader does;
- each fetched span is digest-checked; full-coverage reads additionally
  verify content identity.

V1 bounds: one table of at most 8 blocks per object, indices tiling
0..N-1, dependency-free base spans. The MNCS closure machinery itself
is general (multi-table fixpoints, sparse indices, 4-deep dependency
edges) and is covered by semantic tests with dependency chains.

## External plans

`store.plan.v1` admits proposals from any authority — planner,
ranker, memory policy, compiler, agent, human rule — naming a
representation root, block mask, and transfer cap. Store validates
the plan against its own facts (representation offered, fidelity
satisfied and offered, mask nonempty exactly when coverage exists,
mask described and closed, cap respected) and refuses violations with
the code naming the violated fact. No authority is required.

## Codecs

Representations name codecs by stable 32-byte identity plus a numeric
code. V1 ships:

- `identity` (code 0): bytes as stored, decode class 0;
- `rle-v1` (code 1): windowed run-length coding over independent
  64-byte windows (literal runs + repeat runs of length ≥ 3), decode
  class 1. Every window encodes to at most 65 bytes; decode validates
  structure and length and fails closed with zeros, never partial
  bytes.

Windows are framed as `(encoded length, plain length, bytes)` triples
in blob order; MNCS owns every encode/decode verdict while the host
only frames transport. Unknown codecs fail closed at validation,
selection, and materialization. Future codecs are registry additions.

## Canonical forms

`store.canonical.v1` sorts fixed-record tables (≤ 256 bytes, rows
1..32 bytes, ≤ 16 rows) into stable byte-lexicographic order so
reordered-but-equal structures share identities and compress
identically. The orderedness witness (`rows_sorted`) and canonical
equality are total; out-of-bounds input passes through unchanged and
provably fails the witness, so callers that check cannot mistake it
for canonical.

## Storage layout (v3)

Objects stored with adaptive parameters use manifest v3 (v2 fields +
envelope and representation-table digests). Sidecars are immutable and
content-addressed: `envelopes/`, `representations/` (count + N×128),
`blocks/` (count + M×128), `reps/` (opaque payload blobs). The
generation entry still binds only the manifest root, so commit,
recovery, and snapshot semantics are unchanged. Objects stored
without adaptive parameters keep byte-identical v2 manifests and
synthesize their adaptive view deterministically at read time.

## Machine-native metrics

`tests/measure_adaptive_store.py` reports observed numbers for a
configurable N-block workload: whole vs selective stored/materialized
bytes, decode amplification (materialized / needed), storage ratio
per representation, semantic overhead (sidecars / payload), time to
first useful representation (envelope inspect vs full read), and
exactness verdicts. See its JSON output for the current figures.

## What is implemented vs future work

Implemented (v1):

- envelope, representation, intent, block, plan, codec (identity +
  RLE), and canonical MNCS modules with total validators;
- cost estimation, tournament selection, closure fixpoints, plan
  admission, windowed RLE encode/decode, table canonicalization;
- EmbeddedStore v3 manifests, sidecars, envelope inspection,
  selective/tagged reads, intent selection, plan execution, synopsis
  reads, legacy v2 synthesis, corruption fail-closed behavior;
- semantic tests through the real CLI, integration tests through a
  real retained session, and the benchmark script.

Designed but not implemented:

- multi-table objects and sparse/non-tiling block indices;
- delta-coded blocks and producer block dependencies;
- additional codecs (zstd/brotli instrumental codecs would arrive as
  transport-verified registry entries with MNCS-owned framing laws);
- adaptive lifecycle migration (hot→dense, cold→archive) driven by
  access accounting — the accounting hooks (per-read byte counts)
  exist; no automatic migration runs;
- capability-gated materialization and rights-aware plan admission;
- atomic batch puts with adaptive parameters (single puts only);
- index integration over envelopes/tables (Index-owned; Store
  exposes the facts).

## Callable provider and explicit evolution

See [provider.md](provider.md) for authoritative discovered operations and the
real Environment consumer proof. `EmbeddedStore.add_representation` publishes a
new generation/physical manifest while keeping logical and content identity
stable; previous generations and their inventories remain immutable. Repeated
admission of the same record is idempotent. Removal/migration policy remains
future work. `Selection.satisfied` means fidelity; `latency_permitted` is separate,
and `constraints_satisfied` combines the two. Frequency, lifetime and locality
remain advisory inputs for caller policy, without arbitrary Store defaults.
