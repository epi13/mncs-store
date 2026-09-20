# Profile 0.18 pressure reconciliation

This is the current Store consumer view of the historical Phase-1/Phase-2
pressure notes. The older files remain historical reproducers and frozen
evidence; they are not a second current pressure registry. Current family
pressure state is coordinated by MNCS-Commons.

| Historical pressure | Current classification | Current boundary |
| --- | --- | --- |
| P1-001, P1-002, P1-003 | partially resolved | Profile 0.16 expresses bounded create/write/rename/sync; Profile 0.18 adds the no-follow size observation used by the native publication policy. Host code still performs platform file mechanics, directory durability, and the ordinary large-generation publication sequence. |
| P1-004, P2-001 | still real | The existing bounded hashing/canonical paths remain valid, but arbitrary streaming/digest scale is not claimed by this tranche. |
| P1-005, P1-006, P1-007, P1-008, P1-009, P1-010, P1-011, P1-012, P1-013 | retained as scoped historical or implementation constraints | The current typed-record slice does not require a new language feature. Each remains covered by its original reproducer where it still constrains the Store implementation. |
| P1-014 | partially resolved | Typed relation, provenance, and commit-feed records now have explicit versioned native encodings; host adapters still own C-ABI/JSON transport. |
| P1-015 | still real | Logical identity issuance remains an explicit host/application boundary; Store preserves and validates identity rather than silently collapsing identity domains. |
| P1-016 | partially resolved | `tests/retained_session.py` admits one retained typed session and reuses it across semantic batches. The legacy reference matrix is intentionally still available as a differential oracle. |
| P1-017 | still real | A path/handle protocol is not invented here; filesystem path and capability realization remain host-owned. |
| P1-018, P1-019, P1-020, P1-021 | still real or scheduled | Zero-copy views, host read grants, untyped effect boundaries, and overflow obligations remain separate follow-up work. |
| P2-004, P2-005 | partially resolved | Native publication admission and a granted effectful publication proof exist, including a typed no-follow size check. The ordinary large-object lifecycle is not yet fully routed through that entrypoint. |
| P2-006, P2-007, P2-008 | scheduled/retained | Recovery scan, observation granularity, and scale cost remain bounded verification or scheduled obligations rather than default development work. |

## Scope decision

No profile declaration was blanket-updated to 0.18. Pure typed records remain
on Profile 0.13; the versioned publication module now requires Profile 0.18
because it observes its final entry through the generic metadata operation;
the repository compatibility ceiling remains 0.18.
