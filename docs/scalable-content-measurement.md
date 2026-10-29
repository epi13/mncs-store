# Embedded scalable-content measurement

The measurement command is `PYTHONPATH=python python
tests/measure_embedded_store.py --sizes ...` with an admitted Store artifact.
Store content identities and resident read checks up to one 1,024-byte window
use the standard SHA-256 implementation in `mncs.std.sha256.v1`. Python's
`hashlib` is an independent oracle and remains the implementation for larger
resident buffers and host-file hashing, where the research-bytecode path is
currently too slow.

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
SHA table. `EmbeddedStore` routes resident buffers through MNCS when they fit
one 1,024-byte window; larger resident buffers and explicit host-file hashing
retain `hashlib` as a performance boundary. The 4 MB geometry test proves
bounded storage shape, not 4 MB native hashing throughput. Forge cutover keeps
the 4 MB domain limit, while large-object end-to-end native hashing remains an
open runtime-performance obligation.

The measurement table above predates the read-side migration. Its verify times
are historical and do not measure the new native small-buffer path. The Store
session regression test now checks both the MNCS path for a 56-byte resident
buffer against the independent Python oracle and the unchanged host boundary
for buffers above 1,024 bytes.
