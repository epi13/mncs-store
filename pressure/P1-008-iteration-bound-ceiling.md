# P1-008 — Bounded-iteration ceiling (1..=32 per level, two levels) shapes all scans

Storage feature being implemented: digest comparison/ordering (32-byte
loops), descriptor validation, any future multi-chunk root scan.

Observed language/runtime/compiler behavior: counted/traversal bounds are
1..=32 per level (MNE142 otherwise); Profile 0.11 allows exactly two
nested levels (third refused, MNE147). A 64-byte manifest cannot be
scanned in one level; 32-byte digest loops sit exactly AT the ceiling, so
any wider digest (e.g. SHA-512) or any scan with setup margin needs
nesting for purely arithmetic reasons.

Minimal reproducer (mncs-language):

```mncs
fn scan64(x: [byte; 64]) -> (result: bool) {
    iterate i over x carrying found: bool = false {  // MNE142: bound 64 refused
        next found = found || x[i] == 0;
    }
    return found;
}
```

Required semantics: bounds that scale with the sequence bound (a full
single-level traversal of any legal array), or an explicit
higher-bound construct for u8-scale scans.

Why the current behavior/API is insufficient: it worked (barely) for
Phase 1a — digest loops use bound 32, manifest validation avoids loops
via direct indexing — but the ceiling interacts badly with P1-005: if
views ever grow to KB scale, two levels of 32 (1024 steps) still cannot
scan them, so raising the sequence bound REQUIRES raising iteration
bounds in lockstep.

Safety/correctness implications: LOW (refusals are explicit). The subtle
risk is step-budget accounting across nesting levels, which we did not
need to probe.

Performance implications: none observed.

Workaround used: direct-index field checks (no loops) for 16/64-byte
structures; single-level bound-32 loops only for 32-byte digests.

What the language/stdlib/runtime should ideally provide: single-level
traversal bounds that cover the maximum sequence bound, documented
together (one "bounded data" envelope, not two independent ceilings).

Affected backend(s) / target(s), if known: all (profile-level rule).

Severity: moderate

Status: workaround (indexing discipline; no nested loops needed in 1a)
