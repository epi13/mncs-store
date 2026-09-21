# Store consumer contracts

`mncs-store` is a reusable persistence substrate, not a domain database. A
consumer should declare the smallest contracts it uses and keep its domain
meaning in its own repository.

## Contract inventory

| Contract | Current representation | Consumer capability | Store does not decide |
|---|---|---|---|
| `persistent-object/1` | supported `mncs_store.EmbeddedStore` over the current content tree, descriptor, binding, generation, publication, recovery, typed-record, and commit-feed paths | immutable typed object, current generation, integrity-bound reopen, typed compare-and-transition outcomes | application lifecycle or business meaning |
| `scalable-content/1` | bounded 64 KiB chunks, 31-way structural nodes, one manifest/root, and streaming `mncs.std.sha256.v1` state | payloads larger than one MNCS value, deterministic root identity, corruption/missing/reorder/truncation rejection | application serialization semantics |
| `durable-publication/1` | retained Store session plus local lock, immutable staging, fsync, atomic head publication, and evidence-based recovery | old-or-new process-crash outcome and stale-generation rejection | retry policy and domain workflow |
| `typed-relations/2` | current generic relation vocabulary: type identity, endpoints, generation, provenance, ordinal, optional typed metadata identities | generic structure | what the relation type means |
| `provenance/1` | `store.provenance.v1` | source, producer, transformation, generation, evidence and ancestry identities | evidence sufficiency or authority |
| `commit-feed/1` | `store.commit_feed.v1` | deterministic Store-to-Index generation/count/root feed | query ranking or domain projections |
| `semantic-state/1` | `store.semantic_state.v1` | producer-supplied identity-bound codes and set identities | Commons lifecycle/severity ontology |

`persistent-object/1` is now an executable family contract through the
supported embedded adapter. The former aggregate `persistent-state/1` is no
longer advertised. Ingest remains an optional import/migration dependency;
native producers may bind directly to Store.

## Consumer bindings

Forge is the first shipped binding. The remaining rows are pressure targets,
not claims that their persistence migrations have shipped:

| Consumer | Smallest expected Store contracts | Domain authority retained by consumer |
|---|---|---|
| Forge | `persistent-object/1`, `scalable-content/1`, `durable-publication/1`, `provenance/1` | candidate lifecycle, assurance, repair, evidence interpretation |
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

The current Store path has one active generic relation representation. Historical
v1 relation bytes are no longer accepted by normal Store execution; any future
legacy import must be an explicit external migration adapter. Store may inspect
generic identity fields for indexing and recovery, but the owning consumer
supplies relation vocabulary, lifecycle transitions, and sufficiency decisions.

## Compare-and-transition ownership

`mncs.std.store.v1.cas` is the reusable abstract compare-and-transition law:
an expected base either advances the state or returns the observed generation
without mutation. `store.generation.v1.cas_decide` is the Store application
projection of that same law with Store generation widths and conflict-token
encoding. It is not a second retry or transaction model. The embedded adapter
only serializes the platform publication mechanism, asks the native Store
decision, and maps the bounded result; retry policy remains with the consumer.

The next pressure source is Fabric's controller/worker/bundle state. See
[Fabric pressure after the Forge cutover](fabric-pressure-next.md) for the
bounded batch-generation, snapshot, and retention capability that should be
added before a Fabric migration.
