# Adaptive Store provider

Store publishes authoritative invocation descriptors in `.mncs/project.json`.
Environment discovers those declarations, pins their selected Language toolchain,
applies authority, and presents results. No Store algorithm lives in Environment.
Store remains usable independently through `EmbeddedStore` and this provider.

## Enter and discover

From the Environment checkout:

```bash
./scripts/mncs-env --state-dir /absolute/writable/state enter \
  --consumer store-agent --definition /absolute/mncs-store/.mncs/environment.json
./scripts/mncs-env --state-dir /absolute/writable/state capabilities SESSION
./scripts/mncs-env --state-dir /absolute/writable/state invoke SESSION \
  mncs-store:adaptive-representations
```

Store's local definition selects only Environment, Language and Store. It does
not require Forge or any planner. Entry from a Store subdirectory selects the
same local definition. Repeating entry with the same definition, consumer and
state directory reuses durable work. Use the returned absolute action addresses
when changing working directory. An unwritable state directory now reports
`entry-state-unwritable` and explicitly asks for `--state-dir`; it never silently
creates a different continuation store.

Write operations require a writable Store intent and a claim for protected
selected worktrees. Read operations do not acquire write authority. Neither an
intent's relevance nor a plan's `authority` bytes grant access. The current
Environment authority projects provider/worktree effects; it is not a generic
filesystem or object-level rights enforcement system. Integrators must supply
only transport paths they are already authorized to access. Future rights gates
belong before invocation, with Store continuing to validate the proposal itself.

## Operations and transport

| Capability suffix (`mncs-store:`) | Effect | Meaning |
|---|---|---|
| adaptive-representations | read | Describe operations, request fields and bounds |
| adaptive-status | read | Execute selected retained Store intent validation |
| adaptive-admit | write | Admit an immutable logical object with sidecars |
| adaptive-add-representation | write | CAS-publish a new physical inventory |
| adaptive-inspect-envelope | read | Verify envelope without payload expansion |
| adaptive-list-representations | read | Verify and list representation metadata |
| adaptive-select | read | MNCS ranking plus fidelity/latency verdicts |
| adaptive-read-synopsis | read | Fetch only synopsis |
| adaptive-read-blocks | read | Verified base block dependency closure |
| adaptive-materialize | read | Select and materialize; optional mask or tag |
| adaptive-materialize-plan | read | Admit and execute an external proposal |

Except describe/status, operations take `--request /absolute/request.json`.
This JSON is transport, never canonical Store state. Common fields are `store`
(an absolute Store directory), `domain_schema` and `domain_identity`. Byte values
use exactly one of `{"utf8":"..."}`, `{"hex":"..."}`, `{"base64":"..."}`,
or `{"file":"/absolute/path"}`. Admission requires `expected_generation`;
there is no guessed retry. Inputs are bounded to 64 KiB of request JSON and
32 MiB per byte input. All integer transport is unsigned 64-bit.

Envelope inspection and representation listing accept optional `generation`
to inspect a committed historical physical inventory. Future/uncommitted
generations are refused; omitting it selects the current head.

Example envelope request:

```json
{
  "store": "/absolute/state/artifacts",
  "domain_schema": {"utf8": "my.consumer.state/1"},
  "domain_identity": {"utf8": "work-42"}
}
```

```bash
./scripts/mncs-env --state-dir /absolute/writable/state invoke SESSION \
  mncs-store:adaptive-inspect-envelope -- --request /absolute/request.json
```

Selection/materialization accept an `intent` object with the existing MNCS
fields: `fidelity`, `latency`, `compute`, `memory`, `transfer`, `frequency`,
`lifetime`, `locality`. Defaults remain exact fidelity and neutral costs.
Materialization accepts either `mask` or `tag`. An explicit selector requires
block coverage before native cost ranking, so transfer-heavy access cannot
elect an opaque whole-object encoding for a partial request. Plans use `plan` with `fidelity`,
`root` (32-byte representation identity), `mask`, optional `cap`, and optional
32-byte `authority` provenance. Store encodes and validates these through MNCS.

Results use `mncs.store.provider-result/1`, with `status`, `operation`, selected
runtime paths, result, artifact SHA, cache/startup timings and actual retained
call/batch/JSON-byte counters. Small
payloads use base64; large payloads are written under Environment's session
artifact directory, with size and independent SHA-256 transport receipts.
Independent provider consumers can supply `--artifact-dir /absolute/directory`.
Canonical content verification remains MNCS-owned. No arbitrary output path
from a request is executed. Operation descriptors fix the operation; additional
arguments cannot turn a read capability into admission.

`adaptive-status` proves the selected runtime can open the Store artifact and
execute native intent validation. It does not claim every database or stored
payload is healthy. Environment bounds readiness probes to three seconds and
16 KiB, preserving lifecycle/readiness separation. For a cold artifact cache,
enter first prepares the selected Store for its own persistence. Standalone
consumers must impose a bounded process deadline (as Environment does).

## Read-only retrieval

Provider reads use `EmbeddedStore(..., read_only=True)`. This mode never
initializes directories, creates locks, executes recovery, writes Store files,
or builds the full payload projection. It verifies committed generation
metadata and checks only the requested object's manifest chain and covered
payload. An unrelated missing payload therefore does not prevent envelope
inspection or another region's retrieval. Whole-object verification still fails
when it reaches the missing/corrupt material. Derived compiler artifact caching
is platform preparation, separate from Store database mutation.

Existing default opening behavior is unchanged: writable opening recovers and
verifies whole objects. Recovery is an explicit writable responsibility; a
read-only handle refuses publication/recovery. Inspection accounting describes
the Store operation's logical metadata acquisition, not total process I/O,
compiler artifact loading, or Environment's separate continuation database.

## Diagnostics

Legacy `StoreResultCode` values remain stable. Structured diagnostics preserve
that `code` and add `detail_code` where admission semantics have a specific
verdict: `INVALID_BLOCK_MASK`, `REPRESENTATION_MISSING`, `PLAN_N`,
`PLAN_BLOCKS_N`, and `UNSUPPORTED_REPRESENTATION`. Numeric plan codes belong to
`store.plan.v1`; for example `PLAN_BLOCKS_3` is transfer-cap refusal.
Malformed transport/intent and unavailable selected runtimes have distinct
provider codes. Corruption remains `INTEGRITY_FAILURE`; selection reports
unsatisfied fidelity and disallowed latency separately, without silently
relaxing either assertion. No provider operation falls back to an ambient
compiler, embed library, Store package, or precompiled Store artifact override.

## Repeatable real consumer proof

```bash
cd /absolute/mncs-environment
python3 scripts/adaptive_store_proof.py --family-root /absolute/family
```

The proof persists actual Environment context, capabilities, and durable state
as three tagged JSON regions. It admits base exact + synopsis, publishes a later
RLE exact inventory, verifies idempotence, re-enters from a subdirectory, and
retrieves a region in fresh processes with an unrelated chunk absent. It then
verifies coded and base exact reconstruction and Store checkpoint/resume/handoff.
The proof clones committed providers and copies the selected runtime bytes into
an owned campaign; its report remains inspectable. Original source checkouts
and foreign claims are not modified by the proof. Native codec decisions are never reproduced in the host.
