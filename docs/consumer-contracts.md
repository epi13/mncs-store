# Store consumer contracts

`mncs-store` is a reusable persistence substrate, not a domain database. A
consumer should declare the smallest contracts it uses and keep its domain
meaning in its own repository.

## Contract inventory

| Contract | Current representation | Consumer capability | Store does not decide |
|---|---|---|---|
| `persistent-object/1` | identity, descriptor, chunk, manifest, generation, recovery | immutable typed object, current/historical generation, integrity-bound reopen | application lifecycle or business meaning |
| `typed-relations/1` | `store.relationship.v1` | compatibility for the original Commons relation vocabulary | any future relation vocabulary |
| `typed-relations/2` | `store.relationship.v2` | generic relation type identity, endpoints, generation, provenance, ordinal, optional typed metadata identities | what the relation type means |
| `provenance/1` | `store.provenance.v1` | source, producer, transformation, generation, evidence and ancestry identities | evidence sufficiency or authority |
| `commit-feed/1` | `store.commit_feed.v1` | deterministic Store-to-Index generation/count/root feed | query ranking or domain projections |
| `semantic-state/1` | `store.semantic_state.v1` | producer-supplied identity-bound codes and set identities | Commons lifecycle/severity ontology |

The existing `persistent-state/1` manifest entry is a bootstrap aggregate for
the current vertical and compatibility discovery. New consumers should name
the granular contracts above in their own manifests once they have executable
bindings.

## Consumer bindings

These are target bindings for the migration campaign, not claims that all have
already shipped:

| Consumer | Smallest expected Store contracts | Domain authority retained by consumer |
|---|---|---|
| Forge | `persistent-object/1`, `typed-relations/2`, `provenance/1`, `commit-feed/1` | candidate lifecycle, assurance, repair, evidence interpretation |
| Fabric | `persistent-object/1`, `typed-relations/2`, `provenance/1`, `commit-feed/1` | placement, scheduling, worker admission, trust, reconciliation |
| RAVEL | `persistent-object/1`, `typed-relations/2`, `provenance/1` | verification planning, candidate/experience semantics, retention policy |
| Doctor | `persistent-object/1`, `provenance/1` | inventory meaning and invalidation policy; cold-cache rebuild |
| Test / Debug / Actions | `persistent-object/1`, `typed-relations/2`, `provenance/1` | test, diagnosis, workflow and external protocol semantics |
| Index | `commit-feed/1`, relation/object fields | disposable discovery and query acceleration |
| Service | bounded projections of object/generation/relation/feed state | serving, cancellation and transport lifecycle |

Consumers must not declare a contract merely because a future mapping appears
convenient. The manifest update belongs with the consumer's differential
implementation and proof. A JSON receipt or artifact may remain at an external
boundary while its internal canonical evidence is a Store object.

## Version discipline

Frozen v1 bytes are never reinterpreted as v2. An incompatible representation
gets a new explicit version and a native validation/recovery proof. Store may
inspect generic identity fields for indexing and recovery, but the owning
consumer supplies relation vocabulary, lifecycle transitions, and sufficiency
decisions.
