# Embedded scalable-content measurement

The measurement command is `PYTHONPATH=python python
tests/measure_embedded_store.py --sizes ...` with an admitted Store artifact.
Store content identities up to one 1,024-byte window use the standard SHA-256
implementation in `mncs.std.sha256.v1`. Python's `hashlib` is an independent
oracle and remains the implementation for resident read checks, larger
resident buffers, and host-file hashing.

Observed on 2026-09-20 with the retained research-bytecode artifact
`f3ef34e43b9fa4a76bfe957b6c2d6347e59f1a54882f40a5c336b0022aa3c876`:

| payload | chunks | depth | structural bytes | Store calls | write | verify | reopen |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 B | 1 | 0 | 479 | 48 | 3.845 s | 5.238 s | 2.182 s |
| 1,024 B | 1 | 0 | 485 | 52 | 11.721 s | 21.242 s | 10.099 s |

The 4,000,000-byte Python realization test uses 62 bounded 64 KiB chunks,
three structural nodes (depth 1), and under 10 KiB of structural overhead;
that geometry test passes without placing the object in one MNCS value.

The retained research-bytecode path still has no usable large-object
throughput: a 32 KiB real admission exceeded ten minutes and was stopped. The
Store now uses MNCS round constants rather than asking the host to carry the
SHA table for small content identities. `EmbeddedStore` keeps `hashlib` for
resident read integrity checks: Store open verifies hundreds of records, and
per-record research-bytecode execution made that ordinary path unacceptably
slow. Larger resident buffers and explicit host-file hashing also retain
`hashlib` as a performance boundary. The 4 MB geometry test proves bounded
storage shape, not 4 MB native hashing throughput. Forge cutover keeps the 4
MB domain limit, while large-object end-to-end native hashing remains an open
runtime-performance obligation.

The measurement table above predates the new MNCS standard-constant entry
points. The Store session regression tests check native hashing for a 56-byte
new content identity against the independent Python oracle, while resident
read verification remains on the host path for both small and large buffers
to keep repeated Store integrity scans responsive.

## Deterministic Environment remediation (2026-11-07)

Environment's full invocation profile found small native digest production to
dominate a controlled Store-backed request. The correction keeps the same standard
SHA-256 and canonical bytes: up to 64 bytes use one native init/update/finalize
call without the generic window copy; 65–1024 bytes use one native window call.
Larger resident buffers use the existing optimized host digest directly, avoiding
temporary file staging, fsync and reread. Files still use the existing file API.
Single-chunk content reuses its already-computed payload digest, including empty
objects. This is equality of the same bytes within one operation, not a cache.

Three randomized-order runs of eight measured calls per treatment retained all
events and Store generations and checked persisted contexts after reopen:

| Resident Store path | Baseline median | Native call composition | Plus single-chunk digest reuse |
| --- | ---: | ---: | ---: |
| Existing invocation, no request observation | 206.434 ms | 200.512 ms | 152.756 ms |
| Invocation with request observation | 300.008 ms | 285.575 ms | 215.349 ms |
| Difference of pooled medians | 93.574 ms | 85.063 ms | 62.593 ms |

Observed calls retain three events and five generations each; pre-observation
controls retain two events and four generations. Retained native calls per eight
observed invocations fall from 928 to 336 to 312. There is no suppressed event,
permission bypass, private cache or alternate Store. Reopen measurements and raw
samples live under Environment's `outputs/deterministic-remediation-20261107/`.
Machine load was not isolated, so these are descriptive measurements of the
bounded fixture, not a production latency guarantee. VM steps, ABI call counts,
logical bytes, physical I/O and agent reasoning remain different quantities.

New independent SHA oracle cases cover empty input, padding/block boundaries and
the 1024-byte limit. Single/multichunk boundary tests preserve content identities,
selective readback and reopen. Existing crash/CAS/corruption tests remain required.
This changes transport composition and redundant computation, not identity,
encoding, publication, recovery or durability semantics.
