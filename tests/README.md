# Test strategy

Tests will be added with executable implementation. The initial suite should prioritize semantic invariants over API snapshots.

Required categories include:

- deterministic content/descriptor encoding;
- logical identity versus content identity;
- typed put/get and process reopen round trips;
- chunk reuse and structural sharing;
- corrupted chunk/manifest rejection;
- torn commit and restart fault injection;
- stable reader snapshots during concurrent writes;
- capability enforcement and metadata visibility;
- direct-view eligibility and unsafe-layout rejection;
- stale/destroyed index behavior;
- replication integrity and heterogeneous representation compatibility.

Fixture formats such as JSON may be used to drive tests, but passing a JSON round trip is not proof of the machine-native storage model.
