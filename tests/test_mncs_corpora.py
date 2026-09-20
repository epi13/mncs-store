"""Phase 1 MNCS corpus tests: checked-in semantic vectors across backends.

Each corpus pins deterministic behavior of one store module with
independent oracle values (see tests/corpora/README.md for provenance).
A case passes only when the backend returns the expected value
(status == "returned" and expectation_met).

Backend selection: MNCS_BACKENDS="a,b,c" narrows the matrix (default: all
five executable backends). Backends that cannot run in an environment
(no JSON result while others work) SKIP with a reason instead of failing;
see the final report for the environment matrix.
"""

import os

import pytest

from mncs_exec import (
    MNCS_BIN,
    backends_to_test,
    run_corpus,
    source_study,
)

# Only research-bytecode realizes host effects (sha256_digest, host_read);
# every compiled backend refuses effect-bearing programs (see P1-B02 and
# the effects-probe suite below). Crypto suites are therefore scoped to
# EFFECT_BACKENDS; if a backend gains effect support, add it here AND
# remove its EXPECTED_REFUSALS entries so the probe suite forces the
# wiring update.
EFFECT_BACKENDS = ["mncs-research-bytecode"]

# (src, corpus, grants, backends); backends=None means all backends under
# test. Crypto suites are bytecode-only until P1-B02 is fixed upstream.
CORPUS_SUITES = [
    ("src/store/identity.mncs", "tests/corpora/identity-corpus.json", [], None),
    ("src/store/descriptor.mncs", "tests/corpora/descriptor-corpus.json", [], None),
    (
        "src/store/chunk.mncs",
        "tests/corpora/chunk-corpus.json",
        ["--grant-crypto", "store_chunk"],
        EFFECT_BACKENDS,
    ),
    (
        "src/store/manifest.mncs",
        "tests/corpora/manifest-corpus.json",
        ["--grant-crypto", "store_manifest"],
        EFFECT_BACKENDS,
    ),
    ("tests/fixtures/checked_arith_canary.mncs", "tests/corpora/canary-corpus.json", [], None),
    # Phase-2 suites. Generation/recovery (pure-only modules) run on every
    # backend under test. chunk-v2/manifest-v2 cases are pure functions,
    # but their modules also host sha256 effects, and effect refusal is
    # whole-program (P1-B02 gap 3) — so they ride the bytecode-scoped
    # crypto discipline like the chain cases appended to
    # manifest-corpus.json. The descriptor v2 cases ride the existing pure
    # descriptor suite on all backends.
    ("src/store/chunk.mncs", "tests/corpora/chunk-v2-corpus.json", [],
     EFFECT_BACKENDS),
    ("src/store/manifest.mncs", "tests/corpora/manifest-v2-corpus.json",
     [], EFFECT_BACKENDS),
    ("src/store/generation.mncs", "tests/corpora/generation-corpus.json", [], None),
    ("src/store/recovery.mncs", "tests/corpora/recovery-corpus.json", [], None),
]

PROBE_SRC = "tests/fixtures/effects_probe.mncs"
PROBE_CORPUS = "tests/corpora/effects-corpus.json"
_PROBE_GRANT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "fixtures", "read_grant.bin")
PROBE_GRANTS = [
    "--grant-crypto", "probe_crypto",
    "--grant-read", f"probe_reader={_PROBE_GRANT_FILE}",
]
PROBE_CASES = ["sha256-empty-frame", "hostread-len", "hostread-first"]

ELABORATION_SOURCES = [
    "src/store/identity.mncs",
    "src/store/descriptor.mncs",
    "src/store/chunk.mncs",
    "src/store/manifest.mncs",
    "src/store/read_verify.mncs",
    "src/store/generation.mncs",
    "src/store/recovery.mncs",
    "src/store/relationship/v2.mncs",
    "tests/fixtures/checked_arith_canary.mncs",
    "tests/fixtures/effects_probe.mncs",
]

# (backend, corpus_file, case_id) -> pressure ID. A listed cell that FAILS
# is reported as a known divergence (suite stays green). A listed cell that
# PASSES fails the suite: the allowlist is stale and must be removed.
# An unlisted failure always fails the suite.
KNOWN_DIVERGENCES = {
    ("mncs-c11", "canary-corpus.json", "decode-plain-max"): "P1-B01",
}

# Compiled backends refuse effect-bearing programs outright (P1-B02). These
# are EXPECTED REFUSALS, not passes: the probe suite asserts the refusal
# shape, and a backend that starts executing effects fails loudly so its
# entries get removed and the crypto suites get widened.
EXPECTED_REFUSALS = {
    (b, "effects-corpus.json", c): "P1-B02"
    for b in (
        "mncs-portable-wasm-mvp",
        "mncs-c11",
        "mncs-llvm-ir",
        "mncs-cranelift",
    )
    for c in PROBE_CASES
}


def suite_backends(suite_backends):
    if suite_backends is None:
        return backends_to_test()
    return [b for b in backends_to_test() if b in suite_backends]


PARAMS = [
    (src, corpus, grants, backend)
    for (src, corpus, grants, backends) in CORPUS_SUITES
    for backend in suite_backends(backends)
]

PARAM_IDS = [f"{c.split('/')[-1]}@{b}" for (s, c, g, b) in PARAMS]


def is_refusal_shape(result):
    """A compilation-result where an experiment-result was expected.

    Compiled backends refuse effect-bearing programs with exit 1, a JSON
    body shaped like a compilation result (has "emissions", no "cases"),
    and empty stderr (see P1-B02). This predicate names that shape so the
    suite can pin it instead of misreading it as an environment flake.
    """
    return (
        result is not None
        and "cases" not in result
        and "emissions" in result
    )


def test_compiler_available():
    assert os.path.exists(MNCS_BIN), f"mncs compiler CLI missing: {MNCS_BIN}"


@pytest.mark.parametrize("path", ELABORATION_SOURCES)
def test_modules_elaborate_without_errors(path):
    study = source_study(path)
    errors = [
        d for d in study.get("diagnostics", []) if d.get("severity") == "error"
    ]
    assert errors == [], f"{path}: elaboration errors: {errors[:2]}"


def test_type_confusion_rejected():
    """Logical-identity bytes must not elaborate in a content position."""
    study = source_study("tests/fixtures/type_confusion.mncs")
    codes = [
        d.get("code")
        for d in study.get("diagnostics", [])
        if d.get("severity") == "error"
    ]
    assert "MNE133" in codes, (
        "expected call-argument type rejection (MNE133) for "
        f"[byte; 12] in a [byte; 32] position, got: {codes}"
    )


@pytest.mark.parametrize(
    "src,corpus,grants,backend", PARAMS, ids=PARAM_IDS
)
def test_corpus_on_backend(src, corpus, grants, backend):
    code, result, stderr = run_corpus(src, None, backend, grants=grants,
                                      corpus_path=corpus)
    if result is None:
        pytest.skip(f"{backend}: no result JSON (exit={code}): {stderr[-500:]}")
    if is_refusal_shape(result):
        # Suites in this test are scoped to backends that must execute
        # them (crypto suites to EFFECT_BACKENDS). A refusal here is a
        # REGRESSION, not an environment flake: fail, do not skip.
        raise AssertionError(
            f"{corpus}@{backend}: refused with compilation-result shape "
            f"(exit={code}); a backend in scope must execute its suites"
        )
    if "cases" not in result:
        pytest.skip(f"{backend}: unrecognized output (exit={code})")
    corpus_file = corpus.split("/")[-1]
    failures = []
    for c in result["cases"]:
        key = (backend, corpus_file, c["case_id"])
        ok = c.get("status") == "returned" and c.get("expectation_met") is True
        if key in KNOWN_DIVERGENCES:
            if ok:
                failures.append(
                    f"{c['case_id']}: STALE allowlist "
                    f"({KNOWN_DIVERGENCES[key]} now passes on {backend}); "
                    "remove the entry"
                )
            continue
        if not ok:
            failures.append(
                f"{c['case_id']}: status={c.get('status')} "
                f"met={c.get('expectation_met')} "
                f"failure={c.get('failure_reason')}"
            )
    assert failures == [], (
        f"{corpus_file}@{backend}:\n" + "\n".join(failures)
    )


def _suite_cases(src, corpus, grants, backend):
    code, result, stderr = run_corpus(src, None, backend, grants=grants,
                                      corpus_path=corpus)
    if result is None or "cases" not in result:
        pytest.skip(f"{backend}: no result JSON (exit={code})")
    return {c["case_id"]: c for c in result["cases"]}


def test_effects_probe_per_backend():
    """Pin per-backend host-effect support cheaply (P1-B02).

    The tiny probe program asks each backend to realize `sha256_digest`
    and `host_read`. research-bytecode must execute all three cases;
    compiled backends must refuse in the documented shape. A compiled
    backend that STARTS executing effects fails loudly: remove its
    EXPECTED_REFUSALS entries and widen the crypto suites to cover it.
    """
    problems = []
    for backend in backends_to_test():
        code, result, stderr = run_corpus(
            PROBE_SRC, None, backend, grants=PROBE_GRANTS,
            corpus_path=PROBE_CORPUS)
        if result is not None and "cases" in result:
            executed = True
        elif is_refusal_shape(result):
            executed = False
            # P1-B02 reframe: refusals must carry a machine-readable
            # host-call diagnostic (CG?302 family: CGC302 on C11,
            # CGN302 on wasm-MVP, CGL302 on LLVM, CGF302 on Cranelift),
            # not just the body shape.
            diags = (result or {}).get("diagnostics", [])
            host_call_marks = [
                d.get("code") for d in diags
                if (d.get("code") or "").endswith("302")
                and "host calls are unsupported" in (d.get("message") or "")
            ]
            if not host_call_marks:
                problems.append(
                    f"{backend}: refusal lacks a CG?302 host-call "
                    "diagnostic "
                    f"(got {[d.get('code') for d in diags][:6]}); P1-B02 "
                    "refusal semantics regressed"
                )
        else:
            pytest.skip(f"{backend}: no result JSON (exit={code})")
            continue
        for cid in PROBE_CASES:
            key = (backend, "effects-corpus.json", cid)
            if key in EXPECTED_REFUSALS:
                if executed:
                    problems.append(
                        f"{backend}:{cid}: STALE refusal "
                        f"({EXPECTED_REFUSALS[key]} may be fixed); remove the "
                        "entry and widen EFFECT_BACKENDS"
                    )
                continue
            if not executed:
                problems.append(
                    f"{backend}:{cid}: unexpected refusal; "
                    "research-bytecode must realize host effects"
                )
                continue
            case = {c["case_id"]: c for c in result["cases"]}.get(cid)
            if case is None or case.get("status") != "returned" \
                    or case.get("expectation_met") is not True:
                problems.append(
                    f"{backend}:{cid}: status={case.get('status') if case else None} "
                    f"met={case.get('expectation_met') if case else None}"
                )
    assert problems == [], "effect-probe problems:\n" + "\n".join(problems)


def test_cross_backend_agreement():
    """All backends must return identical values for identical cases."""
    backends = backends_to_test()
    if len(backends) < 2:
        pytest.skip("need at least two backends for agreement check")
    mismatches = []
    # Crypto suites run on EFFECT_BACKENDS only (P1-B02); agreement across
    # backends is asserted over the universally-executed suites.
    for src, corpus, grants, backends_scope in CORPUS_SUITES:
        if backends_scope is not None:
            continue
        corpus_file = corpus.split("/")[-1]
        try:
            runs = {
                b: _suite_cases(src, corpus, grants, b) for b in backends
            }
        except pytest.skip.Exception:
            raise
        # A case known-divergent for ANY participant is excluded from
        # agreement no matter which backend is the reference: the corpus
        # test's allowlist already pins that divergence (P1-B01). Without
        # this, a divergent reference (e.g. c11 first) would report the
        # agreeing backends as mismatched — the failure mode this fix
        # corrects (found live on the c11/llvm/cranelift matrix).
        divergent = {
            cid for b in backends
            for (bb, cf, cid) in KNOWN_DIVERGENCES
            if bb == b and cf == corpus_file
        }
        ref = backends[0]
        for cid, refcase in runs[ref].items():
            if cid in divergent:
                continue
            for b in backends[1:]:
                other = runs[b].get(cid)
                if other is None:
                    mismatches.append(f"{corpus_file}:{cid} missing on {b}")
                elif other.get("returned") != refcase.get("returned"):
                    mismatches.append(
                        f"{corpus_file}:{cid} {ref}={refcase.get('returned')} "
                        f"{b}={other.get('returned')}"
                    )
    assert mismatches == [], "cross-backend disagreement:\n" + "\n".join(mismatches)


def test_repeat_run_determinism():
    """Two identical runs must produce identical case outcomes."""
    src, corpus, grants, _scope = CORPUS_SUITES[2]  # chunk: hashing + framing
    first = _suite_cases(src, corpus, grants, "mncs-research-bytecode")
    second = _suite_cases(src, corpus, grants, "mncs-research-bytecode")
    assert set(first) == set(second)
    for cid in first:
        assert first[cid].get("returned") == second[cid].get("returned"), (
            f"nondeterministic result for {cid}"
        )
