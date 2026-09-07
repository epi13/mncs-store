# RFC 0009 — Zero-copy and memory-mapped access

**Status:** Accepted for architecture

## Problem

A storage stack that always performs disk → parse → allocate object graph → convert → copy defeats much of the value of retaining native representation semantics.

## Decision

Compatible stored representations may be exposed as direct typed views, including memory-mapped views where the platform/backend supports them. Zero-copy is an optimization with strict semantic eligibility, never a reason to bypass validation.

A direct view requires compatible type/layout, alignment, bounds, endianness, lifetime, mutability, capability, and verified content. If any requirement is not met, the runtime performs or requests an explicit transformation/copy.

## Safety

- immutable committed content must not become writable through a view;
- mappings cannot outlive their backing placement/lease;
- descriptor arithmetic is validated before mapping;
- access faults/truncation/corruption must surface as storage errors, not undefined behavior;
- architecture-specific layouts require explicit compatibility checks.

## Accelerator direction

The abstraction should eventually permit optimized movement such as NVMe → pinned host memory → CUDA or platform equivalents, and direct accelerator placement where supported. This RFC does not claim universal GPU direct-storage support.

## Measurement

Implementations should measure bytes copied, allocations, transformations, mapping faults, and transfer paths so zero-copy claims remain evidence-based.
