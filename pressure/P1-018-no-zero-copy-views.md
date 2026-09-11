# P1-018 — No memory-mapped / zero-copy view primitive (RFC 0009 design pressure)

Storage feature being implemented: future direct typed views over stored
bytes (Phase 3 exit proof); Phase 1a notes the gap so the manifest and
descriptor designs do not preclude it.

Observed language/runtime/compiler behavior: views (`[T; up_to N]`) are
borrowed spans in the LANGUAGE's memory model (host-provided inputs,
slices of exact arrays), but there is no mapping of FILE bytes into that
model — no mmap intrinsic, no view-over-placement, no lifetime-bound
lease type. `host_read` COPIES granted file bytes (≤64) into a value.
Every stored byte is therefore copied at least twice (disk → grant
buffer → MNCS value) with no path to do otherwise.

Minimal reproducer: express "a `[u32; 8]` view over bytes [16..48) of the
granted chunk file, valid for the read lease, invalidating on close".
No type names a file-backed view, a lease, or placement residency.

Required semantics (Phase 3): file/placement-backed views with
lifetime, bounds, alignment, endianness, and mutability checking;
explicit incompatibility (transform instead of view); copy/allocation
instrumentation so zero-copy claims are evidence-based (RFC 0009
measurement).

Why the current behavior/API is insufficient: Phase-1a values are tiny,
so copies are harmless NOW — but the descriptor/manifest formats must be
mmap-eligible LATER (alignment fields, BE canonical layout, no
pointerful structures). This report records the forward constraint and
the missing primitive together.

Safety/correctness implications (future): HIGH when views land —
use-after-close, truncation races, and writable-aliasing of immutable
content are the classic mmap hazards; the language should type them
(leases/capabilities), not document them.

Performance implications (future): copies dominate at scale; zero-copy
is the difference between a store and a deserializer.

Workaround used: none needed in Phase 1a (copies fit the proof scale);
formats designed mmap-eligible (fixed offsets, explicit alignment,
big-endian, no interior pointers) per RFC 0016 §6.

What the language/stdlib/runtime should ideally provide: leased,
file-backed typed views with compiler-checked lifetimes and placement
awareness (RAM/NVMe/accelerator), arriving with Phase 3.

Affected backend(s) / target(s), if known: all (surface absence);
accelerator-backed views are additionally hardware-gated.

Severity: moderate (design pressure only; Phase 3 scope)

Status: design pressure only

## Re-baseline 2026-09-10 (Source Profile 0.13) — STILL_REPRODUCES

No mmap/file-backed view, lease, or placement-residency type in any 0.13
feature list. `host_read`/`fs_read_bytes_at` copy (<= 64 B per call);
every stored byte is still copied disk -> grant buffer -> MNCS value.
At the 992-byte object scale copies remain harmless; formats stay
mmap-eligible (fixed offsets, explicit alignment, BE, no interior
pointers). Correctly sequenced after the core; still Phase-3 scope.

Severity: moderate design pressure (unchanged).
