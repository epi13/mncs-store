# Test strategy

Executable since Phase 1a. The suite prioritizes semantic invariants over
API snapshots.

Phase-1a coverage (all passing; see `corpora/README.md` for the semantic
layer):

- deterministic content/descriptor encoding (172 corpus cases + repeat-run
  determinism + cross-backend agreement);
- logical identity versus content identity (width + nominal types,
  negative elaboration fixture, dedup-without-merge lifecycle test);
- typed put/get and process reopen round trips (u32/u64/pair/blobs/empty,
  close + fresh-instance reopen);
- chunk reuse and structural sharing (identical re-put shares one chunk
  file; distinct logical IDs);
- corrupted chunk/manifest rejection (15 bit-flip/truncation/missing
  cases; integrity vs not-found kept distinct);
- torn commit and restart fault injection (truncated chunk/manifest files,
  missing committed files, serial healing on open);
- store version/magic gates (unknown versions fail explicitly).

Deferred to later phases (tracked, not silently dropped):

- stable reader snapshots during concurrent writes (Phase 2 generations);
- capability enforcement beyond test-grant boundaries (needs language
  capability maturation);
- direct-view eligibility and unsafe-layout rejection (Phase 3);
- stale/destroyed index behavior (Phase 4);
- replication integrity and heterogeneous representation compatibility
  (Phase 5).

Harness layers:

- `test_mncs_corpora.py` — checked-in semantic vectors across all five
  executable backends (plus elaboration, negative, agreement,
  determinism checks).
- `test_lifecycle.py` — real on-disk stores via `store_phase1a.py`.
- `mncs_exec.py` — subprocess transport + value parsing (no semantics).
- `store_phase1a.py` — host transport/layout/atomicity driver (every
  semantic byte MNCS-produced; see its scope contract).

Fixture formats such as JSON may be used to drive tests, but passing a JSON round trip is not proof of the machine-native storage model.
JSON here is harness transport only — never canonical state.
