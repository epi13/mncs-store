# Store Retention Completeness Recovery (`mncs-store`)

## Summary

Closed the retained-history completeness gap in `EmbeddedStore.prune_generations` in [python/mncs_store/embedded.py](python/mncs_store/embedded.py) starting from `mncs-store/main` commit `83bc983f564ba3a1743379157d8f62eeb8a96c33`.

## Root Cause and Reproduction

Previously, `EmbeddedStore.prune_generations` skipped `0 < n < floor` and verified only delta `parent` pointers for existing `generations` entries in `[floor, head]`. Because full checkpoints (`GENERATION_MAGIC`) carry no delta parent link, an absent generation interval immediately before a full checkpoint escaped detection.

Reproduced on an isolated native `StoreSession()` fixture (`GENERATION_CHECKPOINT_INTERVAL = 4`, `head = 12`, unlinking generations `8–11`, `keep_last = 8` -> `floor = 4`):

- Dry runs with `reclaim_chunks=False` and `reclaim_chunks=True` both returned `status="complete"` with `prunable_files=3`.
- Applying with `reclaim_chunks=True` (`dry_run=False`) returned `status="complete"` with `removed_files=3` and deleted generations `1–3`.

## Change

Added `_validate_retained_generation_inventory(generations, head, floor, pins)` in [python/mncs_store/embedded.py](python/mncs_store/embedded.py) and invoked it in `EmbeddedStore.prune_generations` immediately after scanning `generations/` and before parsing checkpoints, creating `audit_path`, scanning chunks, or unlinking files:

- Requires `head in generations` and `0 in generations` (`StoreIntegrityError("head or genesis generation is missing")`).
- Requires `0 <= floor <= head` and `floor in generations` (`StoreIntegrityError("retention checkpoint is missing")`).
- Requires every protected pin `n in pins` to exist (`StoreIntegrityError(f"protected generation is missing: {n}")`).
- Requires complete contiguous coverage in `[floor, head]` via `sum(1 for n in generations if n >= floor) != head - floor + 1` (`StoreIntegrityError("retained generation history is incomplete in [floor, head]")`). Because the preceding `os.scandir` pass rejects any entry `< 0` or `> head` with `_ReclamationIncomplete`, all keys in `generations` lie in `[0, head]`; this check runs in `O(len(generations))` without iterating `range(floor, head + 1)`.
- Continues permitting historical gaps strictly in `(0, floor)` while preserving genesis `0` and explicit pins.

## Regressions Added

Added five regressions in [tests/test_retention.py](tests/test_retention.py):

- `test_prune_rejects_missing_interval_before_checkpoint_both_modes`
- `test_prune_rejects_missing_interval_at_floor_zero_without_mutation`
- `test_prune_rejects_missing_floor_head_or_genesis_without_mutation`
- `test_prune_allows_historical_gaps_strictly_below_positive_floor`
- `test_prune_completeness_validation_is_bounded_for_arbitrary_numeric_head`

## Verification

- Worktree checks on `fix/store-retention-completeness-20261115`:
  - `ruff check --select F,W,I,UP,B,E7,E9 tests/test_retention.py`: passed.
  - `python3 -m py_compile python/mncs_store/embedded.py tests/test_retention.py`: passed.
  - `git diff --check`: passed.
  - `pytest tests/ -q`: `316 passed in 249.51s (0:04:09)` (0 skipped, 0 failed).
  - Cross-repo smoke test in `mncs-environment` with `MNCS_STORE_PYTHON` set to the worktree `python/` path (`pytest tests/test_store_session_lookup.py -v`): `8 passed in 14.40s`.
- Production Store data in `~/.local/share/mncs-environment` was not pruned, compacted, migrated, or mutated. No automatic reclamation or background service was activated.
