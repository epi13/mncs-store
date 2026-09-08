# P1-B02 — Host effects realized only by research-bytecode; compiled backends refuse (silently shape-shifted)

Storage feature being implemented: every hash-dependent proof —
content identity, chunk verification, root identity (i.e. the store's
entire integrity core).

Observed language/runtime/compiler behavior: of the five executable
backends, ONLY `mncs-research-bytecode` (the interpreter) realizes host
effects (`sha256_digest`, `host_read`). The four compiled backends
(portable-WASM, C11, LLVM-IR, Cranelift) refuse ANY effect-bearing
program with exit code 1, a compilation-result JSON body (has
"emissions", no "cases"), and EMPTY stderr. Probed 2026-09-08 with
`tests/fixtures/effects_probe.mncs` (one `sha256_digest` case, two
`host_read` cases): bytecode executes all three; all four compiled
backends emit the refusal shape — even for `host_read`, the simplest
effect. The language's own suites confirm the boundary: `crypto_verify.rs`
and `host_effects.rs` run ONLY on research-bytecode.

Three compounding gaps:

1. No backend advertises effect support. `mncs experiment matrix` lists
   `supported_machine_intents` per backend, but NO backend — bytecode
   included — lists any crypto/host/effect intent. Support is
   undiscoverable except by attempting execution.
2. The refusal is a silent shape-shift. Same command, same flags: success
   yields `experiment-result` JSON with "cases"; refusal yields a ~9 MB
   `compilation-result` JSON with no "cases" and no stderr text. A
   harness must sniff the SHAPE to distinguish "backend cannot do this"
   from "backend is broken". There is no machine-readable refusal code
   (searched the 9 MB body for refus*/cannot realize/unsupported effect:
   zero hits).
3. Effect refusal is whole-program. One `host_read` in an otherwise pure
   program poisons the entire run on compiled backends — there is no
   partial execution of the pure cases in the same corpus.

Minimal reproducer: `tests/fixtures/effects_probe.mncs` +
`tests/corpora/effects-corpus.json`, run with `--grant-crypto
probe_crypto --grant-read probe_reader=tests/fixtures/read_grant.bin`
on any compiled backend. Pinned in-suite by
`test_effects_probe_per_backend` with EXPECTED_REFUSALS allowlist
(stale-entry discipline: a backend that starts executing effects fails
loudly, forcing suite widening).

Required semantics: per-backend effect-capability declarations in the
experiment matrix (or capability manifest), a machine-readable refusal
(status/code, not shape-sniffing), and — for storage credibility —
effect realization on at least the natively-executed backends
(LLVM/Cranelift/C11), so integrity proofs are not interpreter-only.

Why the current behavior/API is insufficient: the store's content-
identity story (SHA-256 over canonical frames, cross-checked against
hashlib) executes on exactly ONE backend. Cross-backend agreement — the
mechanism that would catch lowering bugs like P1-B01 in hash paths — is
impossible for every hash-dependent case. A lowering bug in a compiled
backend's (future) effect path would have no cross-check.

Safety/correctness implications: MEDIUM-HIGH. Interpreter-only integrity
means production-shaped backends never verify a digest in this run; any
future effect lowering there ships without differential coverage unless
this probe forces it.

Performance implications: none (capability gap, not speed).

Workaround used: crypto suites scoped to `EFFECT_BACKENDS =
["mncs-research-bytecode"]`; per-backend support pinned cheaply by the
tiny probe program (fast compile) instead of recompiling full store
modules per backend per run; lifecycle driver runs on bytecode.

What the language/stdlib/runtime should ideally provide: (1) declared
effect support per backend in `experiment matrix`; (2) a coded refusal
(`EFFECT_UNSUPPORTED` with backend/effect names, nonzero exit, stderr
line); (3) effect realization on LLVM/Cranelift/C11 (host calls from
compiled code with the same grant model); (4) per-case partial execution
or per-case refusal reporting instead of whole-run shape-shift.

Affected backend(s) / target(s), if known: portable-WASM, C11, LLVM-IR,
Cranelift refuse; research-bytecode realizes. (x86_64 Linux; other hosts
unprobed.)

Severity: major (backend-specific capability gap)

Status: workaround (bytecode-scoped crypto suites) + pinned (probe +
EXPECTED_REFUSALS allowlist)
