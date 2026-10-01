# Adaptive integration measurements

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

The instrumented run exposes ~92.2 MB of JSON transport, 226 retained batches,
and 33,337 semantic calls. Two additional calls project latency permission;
the codec transformation is unchanged. Timing is from single runs under
concurrent workload and is not a controlled speedup/regression experiment.
The final block-aware ranking addition is measured again before delivery;
its final artifact and counters replace the instrumented evidence file.

The real Environment proof uses actual context, capability inventory and session
records; its sizes depend on those records. It reports per-operation bytes and
checks retrieval with an unrelated chunk absent, providing a correctness proof
stronger than accounting alone. Every retained codec decision remains MNCS.
