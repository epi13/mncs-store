# Adaptive integration measurements

## Store integrity scan memory

`store-verify-streaming-20261008.json` records a canonical Store `--verify`
run before and after changing verification to stream one payload at a time.
The Store generations differ by 61 publications, so this is a close operational
comparison rather than a byte-identical benchmark. The old RSS figure is one
process sample, not a measured high-water mark. The new run includes child
wait-accounted high-water RSS, `/proc` sampling, CPU, I/O, descriptor, host
memory, and cgroup observations. Missing cgroup memory limits are recorded as
unknown; unchanged cumulative `oom_kill` counters do not establish why a prior
process may have exited.

These are independent executions of the existing `tests/measure_adaptive_store.py`
workload: eight 64 KiB regions, alternating compressible and patterned data,
base exact + synopsis + RLE exact. The selected Language source is
425de2016a1273b66fa44609ea75aa25e04f0a0f; the release compiler/embed are explicitly
selected. Artifact identity and byte-accounting fields are in each result.

The integration preserves coded storage (307,704 bytes), index size (1,672
bytes), envelope inspection (769 bytes), selective materialization (65,536
bytes), and 8x -> 1x decode amplification. The observed on-disk file size is
834,556 bytes including both exact forms and metadata. Keeping alternatives
increases stored inventory while reducing necessary retrieval; compression
alone does not imply lower total storage.

The instrumented run exposes 92,278,201 bytes of JSON transport, 229 retained batches,
and 33,349 semantic calls. Additional metadata calls project latency
permission and block coverage; the codec transformation is unchanged. Timing is from single runs under
concurrent workload and is not a controlled speedup/regression experiment.
The final native block-aware selection is included in the instrumented evidence
file; the original whole-object codec and exactness workload is unchanged.

The real Environment proof uses actual context, capability inventory and session
records; its sizes depend on those records. It reports per-operation bytes and
checks retrieval with an unrelated chunk absent, providing a correctness proof
stronger than accounting alone. Every retained codec decision remains MNCS.
