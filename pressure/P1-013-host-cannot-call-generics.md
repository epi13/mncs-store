# P1-013 — Host cannot invoke generic functions (no type arguments at the process boundary)

Storage feature being implemented: test/harness invocation of
width-polymorphic validators (first attempted for `frame_valid<N: Nat>`).

Observed language/runtime/compiler behavior: an execution request names
(module, function, arguments) with NO type-argument channel. Targeting a
generic function yields `"execution target SSA function does not exist"`
(the generic template is not an executable symbol; only in-language
monomorphizations via explicit turbofish call sites executable). Probed
2026-09-08 with:

```mncs
fn vlen<N: Nat>(view: [byte; up_to N]) -> (result: u64) { return view.len; }
```

called from a corpus with a 5-byte sequence argument: status FAIL,
`failure_reason: "execution target SSA function does not exist"`. (The
stdlib's generic corpus cases all target NON-generic wrapper functions in
consumer modules — e.g. `examples.status.generic_consumer` — confirming
this is the ecosystem's own pattern, not a misunderstanding.)

Required semantics: either host-nameable type arguments in execution
requests, or argument-driven instantiation of generic entry
points, so polymorphic validators are directly testable.

Why the current behavior/API is insufficient: it forces a non-generic
wrapper layer over every polymorphic function the harness must call
(`verify_frame4/8/16/32` instead of one `verify_frame<N>`; concrete
`frame_valid` over `[byte; up_to 64]` instead of generic). The wrappers
are pure boilerplate that must be maintained per width (compounds
P1-006).

Safety/correctness implications: LOW (wrappers are thin and tested).
The cost is surface area, not soundness.

Performance implications: none.

Workaround used: non-generic boundary functions throughout
(`store.chunk`, `store.descriptor`, `store.manifest` expose only concrete
signatures); generics used solely inside in-language call graphs (none
needed in Phase 1a beyond the abandoned `frame_valid<N>` attempt).

What the language/stdlib/runtime should ideally provide: a `type_arguments`
field on execution-request targets with elaboration-time checking, so a
host can invoke `frame_valid<8>` exactly as source does.

Affected backend(s) / target(s), if known: all (execution-request shape).

Severity: moderate

Status: workaround (non-generic boundary layer)
