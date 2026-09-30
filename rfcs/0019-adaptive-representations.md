# RFC 0019 — Adaptive physical representations

**Status:** Accepted for implementation (v1 implemented)

## Problem

Store persists one physical form per object version: the base chunk
tree. Every read pays whole-object materialization even when the caller
needs a synopsis, a cost estimate, one semantic region, or a
transfer-cheap encoding. "Compression level" as a single scalar cannot
express the independent axes callers actually care about: density,
decode cost, granularity, fidelity, access pattern, transfer budget,
and lifetime.

Higher-order systems (planners, memory policy, compilers, agents) need
to answer *whether* an object is relevant and *what it costs to use*
before paying to expand it. Today they cannot: relevance requires a
full fetch.

## Decision

Separate stable logical identity from adaptive physical representation:

- A logical object owns one or more **representations** at different
  costs and fidelities. Representation 0 is always the base exact form
  over the chunk tree; further representations are opaque single blobs
  (synopses, coded payloads) or share the block table.
- Every object exposes a 256-byte **semantic envelope**: identity,
  type, synopsis pointer, representation inventory, block topology,
  costs, provenance, and the fidelity levels offered. Envelopes are
  content-addressed and readable without touching any payload.
- A representation's payload is partitioned into **blocks**: byte
  spans with digests, semantic tags, and dependency edges. Wanted sets
  are bitmasks; Store fetches only the dependency closure.
- Callers state constraints as a generic **access intent** (required
  fidelity, cost weights, frequency, lifetime, locality). Store folds
  a total MNCS pairwise tournament over the stored representations.
  No intent names a codec, a sibling project, or a ranking model.
- External authorities may propose a **materialization plan**
  (representation + block mask + transfer cap). Store validates the
  plan against its own envelope/representation/block facts and refuses
  it with a distinct code when it does not hold. No authority is
  required: intent-driven selection is always available.
- Physical form is replaceable: representations name a codec by stable
  identity, unknown codecs fail closed, and logical identity never
  depends on which forms exist. V1 ships `identity` and a bounded
  windowed RLE codec; the registry admits more without reinterpreting
  stored bytes.
- Fixed-record tables gain a **canonical form** (stable row order) so
  reordered-but-equal structures share identities and compress
  identically downstream. Exact originals remain recoverable.

V1 bounds (explicit, not incidental): one block table of at most 8
blocks per object with indices tiling 0..N-1; RLE windows of 64 bytes;
canonical tables of at most 256 bytes; synopses of at most 64 KiB.
The MNCS decision layer stays general (multi-table closure, sparse
indices); lifting a bound is transport work.

## Invariants

1. Logical identity is independent of representation inventory: adding,
   removing, or re-encoding a representation never changes the logical
   object, its content identity, or its generation bindings.
2. Committed representations, envelopes, tables, and blobs are
   immutable and content-addressed; the manifest binds them by digest.
3. The base representation (index 0) is always exact identity bytes
   whose root is the object content identity.
4. A representation carrying the exact flag MUST satisfy fidelity 5
   AND be Store-verified to reproduce the object bytes (fidelity-5
   claims over non-matching bytes are refused at write).
5. Selection is total and deterministic: the same table and intent
   always elect the same winner; unknown inputs degrade to explicit
   refusal codes, never guesses.
6. Materialization fetches exactly the verified dependency closure of
   the wanted set; out-of-range masks and unclosed plans are refused
   rather than silently subset.
7. Fidelity levels are cumulative: satisfying a level satisfies all
   lower levels.
8. Envelopes and tables are inspectable without payload expansion;
   inspection reads are integrity-checked like any other read.
9. Unknown codecs, fidelities, and record versions fail explicitly
   (invariant 8 of the normative set).
10. V2 objects without sidecars synthesize a deterministic adaptive
    view (one exact representation, implicit whole-payload block) with
    byte-identical behavior to stored equivalents of the same shape.

## Failure behavior

- Malformed envelopes, representations, tables, plans, intents, and
  RLE windows fail closed with distinct per-module codes; RLE decode
  yields zeros on failure, never partial bytes.
- Missing sidecars behave like missing chunks (not-found); corrupt
  sidecars are integrity failures.
- A plan naming an unoffered representation, unsatisfiable fidelity,
  undescribed block, unclosed mask, or exceeded cap is refused with
  the code naming the violated fact; the caller falls back to
  intent-driven selection.
- Producer-input violations at write (overlapping spans, fidelity-5
  mismatch, unknown codec, oversize or empty synopsis, duplicate
  representation roots) are denied before any durable staging; nothing
  is partially committed. Read paths additionally deny closures that
  escape the selected representation's block range.

## Security and integrity

Representation roots, block digests, envelope digests, and table
digests are untrusted input until verified against the manifest chain
and recomputed identities. A plan or intent grants no access: it only
selects among representations the caller is already entitled to read
(capability enforcement itself remains future work per the roadmap).
Exactness is re-verified by content identity after coded
materialization; partial reads verify per-block digests.

## Rejected alternatives

- A single "compression level" scalar: cannot separate density from
  decode cost, granularity, or fidelity, and couples policy to one
  codec's knob.
- Storing selection policy (learned weights, sibling-specific rules)
  in Store: makes basic usefulness depend on the whole ecosystem and
  hides decisions from audit. Policy inputs stay generic data.
- Whole-blob RLE/LZ over unbounded payloads in one MNCS value:
  violates the bounded-value discipline the language enforces;
  windowed codecs keep every operation bounded and blocks
  independently decodable.
- JSON sidecars for envelopes/tables: violates the canonical
  representation boundary (normative invariant 23) and prevents
  fixed-width integrity reasoning.

## Implementation pressure

- Bulk codec windows cross the retained ABI as JSON values
  (P1-016/P2-008 pattern); see `pressure/P3-001-*.md` for fresh
  measurements and the binary-window ask.
- Canonical sort over 256-byte tables costs ~35k steps and equality
  ~70k steps on research-bytecode; explicit budgets are required
  (P2-008 pattern).
- Multi-table objects, delta-coded blocks, producer block
  dependencies, adaptive migration/lifecycle policy, and
  capability-gated materialization are designed but not implemented;
  see `docs/adaptive-representations.md` (future work).
