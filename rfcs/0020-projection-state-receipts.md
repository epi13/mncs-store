# RFC 0020 — Projection state and publication receipts

**Status:** Proposed (v1 implemented: `store.projection.v1`, `store.receipt.v1`)

## Problem

Derived representations (documentation sections, Atlas pages, journal
entries, media artifacts, publications) need restartable
desired-vs-observed state: which canonical generation is desired,
which generation the observed output reflects, what verification
gates it, and what was published where. Without durable records,
reconcilers re-derive this from scratch on every run, crash recovery
guesses, and publications duplicate or lose their cross-links.

## Decision

Two fixed native records beside `semantic_state`/`commit_feed`:

- `store.projection.v1` — 132 bytes, `PJ` magic. Projection
  identity, canonical generation, observed generation, verification
  code (`0 fail`, `1 pass`, `2 unknown`), status code (`0 current`,
  `1 stale`, `2 unknown`, `3 blocked`, `4 failed`), evidence
  identity, supersedes identity (zero = none).
- `store.receipt.v1` — 140 bytes, `PR` magic. Artifact identity,
  target identity, external (provider-issued handle) identity,
  canonical generation, evidence identity. Presence is the
  publication fact; all three identities must be non-zero.

Store persists producer-supplied codes and identities; it does not
interpret reconciliation policy, verification ontologies, or
publication targets. Verification codes mirror the automation
verdict positions by convention, documented here and owned there.

## Predicates

- `projection.is_current`: observed equals canonical, status
  current, verdict pass, structurally valid. Anything else must
  reconcile before any authoritative claim.
- `receipt.same_publication`: the artifact/target/external triple
  agrees. Generation and evidence may differ across re-recordings;
  triple agreement is the dedup key.

## Dependency edges

Projection-level impact narrowing reuses the generic v2 relation
records (RFC 0018) with registered relation-type identities:
`mncs.relation/projects-from/1`, `mncs.relation/depends-on/1`,
`mncs.relation/supersedes/1`. A relation-type identity is the first
32 bytes of the SHA-256 of its canonical descriptor JSON, computed
by the host and compared by byte equality natively. No new record
layout is needed for edges.

## Consumers

Reconcilers (mncs-automation RFC 0002) read projection state,
decide through native `reconcile_tick`, regenerate through
projection owners (mncs-doc apply, Atlas projector, media
backends), advance observed generations, and record receipts that
dependent projections cross-link. Journal prose derives from
journal events plus receipts; it never becomes a second authority.

## Tests

`tests/corpora/projection-corpus.json` (24 vectors) and
`tests/corpora/receipt-corpus.json` (17 vectors) pin layouts,
validators, accessors, and predicates across backends, following
the house oracle discipline (explicit big-endian assembly).
