# RFC 0015 — Recovery, corruption detection, and verification

**Status:** Accepted for architecture

## Problem

A persistent system is only trustworthy if it can distinguish committed state from torn metadata, corrupted chunks, stale replicas, and incomplete writes after failure.

## Decision

Recovery is verification-driven. The store maintains enough versioned commit/root evidence to identify the latest state that is both declared committed and structurally/integrity valid under the configured durability model.

## Required properties

- every authoritative representation root can be transitively integrity-checked;
- commit metadata has its own integrity/version checks;
- recovery never fabricates a generation by combining whichever pieces happen to exist;
- corrupted canonical chunks are surfaced explicitly and can be repaired from verified replicas when policy allows;
- unreferenced staged chunks may be reclaimed only after proving they are not reachable from retained generations, pins, recovery roots, or active readers;
- replicas and caches are verified before promotion to canonical read sources.

## Fault testing

Implementation must support deterministic fault injection around chunk writes, manifest writes, commit publication, fsync/barrier boundaries, truncation, bit corruption, and restart. The critical property is: after interruption, the visible state is a valid prior or completed generation according to the advertised semantics.

## Repair

Repair must be evidence-based. If no verified copy exists, the store reports unrecoverable corruption rather than manufacturing replacement data. Re-derived objects may be rebuilt by higher layers only when their canonical status/provenance allows it.

## Deferred

Exact journal/superblock structure, scrub scheduling, erasure coding, and background repair algorithms depend on the first implementation and later distributed pressure.
