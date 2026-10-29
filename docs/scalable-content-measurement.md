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
