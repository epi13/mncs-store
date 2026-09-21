# Embedded scalable-content measurement

The measurement command is `PYTHONPATH=python python
tests/measure_embedded_store.py --sizes ...` with an admitted Store artifact.
Store content identities are produced by `mncs.std.sha256.v1`; Python's
`hashlib` is used only as an independent oracle.

Observed on 2026-09-20 with the retained research-bytecode artifact
`f3ef34e43b9fa4a76bfe957b6c2d6347e59f1a54882f40a5c336b0022aa3c876`:

| payload | chunks | depth | structural bytes | Store calls | write | verify | reopen |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 B | 1 | 0 | 479 | 48 | 3.845 s | 5.238 s | 2.182 s |
| 1,024 B | 1 | 0 | 485 | 52 | 11.721 s | 21.242 s | 10.099 s |

The 4,000,000-byte Python realization test uses 62 bounded 64 KiB chunks,
three structural nodes (depth 1), and under 10 KiB of structural overhead;
that geometry test passes without placing the object in one MNCS value.

The host transport now supplies bounded 1,024-byte file windows to the same
native streaming `DigestState`; Python does not hash or choose the digest. This
avoids the earlier filesystem-effect step-budget failure, but a 32 KiB real
admission still exceeded ten minutes on the retained research-bytecode
interpreter and was stopped as a practical throughput limit. This is a precise
reusable Store/runtime blocker: the streaming SHA semantics are correct, while
the current realization has no usable large-object throughput. Forge cutover
therefore uses the same Store path and retains the 4 MB domain limit, while
4 MB end-to-end admission remains a scheduled backend-performance obligation
rather than an unproven success claim. No host hash is substituted for the
native SHA authority.
