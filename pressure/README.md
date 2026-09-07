# Language and system pressure

This directory records cases where implementing `mncs-store` reveals missing or awkward capabilities in `mncs-language`, its runtime, compiler backends, Fabric, or adjacent MNCS primitives.

A pressure report should include:

- the storage feature being implemented;
- minimal MNCS-language reproducer or pseudo-code;
- what cannot be expressed or cannot meet the required semantics;
- safety/correctness requirement;
- performance requirement and measurements when available;
- workaround used, if any;
- preferred language/runtime capability;
- affected targets/backends.

Pressure is useful evidence, not a reason to hide the core implementation in another language. Storage is expected to stress lifetimes/borrowing, mmap and raw views, atomics, async I/O, checked arithmetic, crash-consistency barriers, typed reflection, SIMD, heterogeneous memory, CUDA interaction, capability types, concurrency, and distributed execution.
