"""Shared MNCS execution helpers for mncs-store Phase 1 tests.

Every semantic byte asserted by this suite is produced by executing
mncs-language programs (src/store/*.mncs) through the reference compiler
CLI. This module only transports values across the process boundary and
parses results; it never reimplements storage semantics. The one exception
is hashlib, used strictly as an INDEPENDENT oracle for SHA-256 agreement
(the store must never depend on it for canonical bytes).
"""

import json
import os
import subprocess

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
CORPORA_DIR = os.path.join(REPO_ROOT, "tests", "corpora")
FIXTURES_DIR = os.path.join(REPO_ROOT, "tests", "fixtures")

MNCS_BIN = os.environ.get(
    "MNCS_BIN",
    "/home/epi13/Documents/Projects/mncs-language/target/debug/mncs",
)
MNCS_LANG_LIB = os.environ.get(
    "MNCS_LANG_LIB", "/home/epi13/Documents/Projects/mncs-language/library"
)

BACKENDS = [
    "mncs-research-bytecode",
    "mncs-portable-wasm-mvp",
    "mncs-c11",
    "mncs-llvm-ir",
    "mncs-cranelift",
]

# Backends that run without a C toolchain / external runtime on this host.
FAST_BACKENDS = ["mncs-research-bytecode", "mncs-portable-wasm-mvp"]

STEP_BUDGET = 32768
RUN_TIMEOUT_S = 280


def backends_to_test():
    raw = os.environ.get("MNCS_BACKENDS", "")
    if raw.strip():
        wanted = [b.strip() for b in raw.split(",") if b.strip()]
        unknown = [b for b in wanted if b not in BACKENDS]
        if unknown:
            raise StoreHarnessError(f"unknown backends in MNCS_BACKENDS: {unknown}")
        return wanted
    return list(BACKENDS)


def library_path():
    return MNCS_LANG_LIB + ":" + os.path.join(REPO_ROOT, "src")


def I(value, bits, signed):
    return {"integer": {"value": value, "type": {"bits": bits, "signed": signed}}}


def U32(value):
    return I(value, 32, False)


def U64(value):
    return I(value, 64, False)


def U16(value):
    return I(value, 16, False)


def B(value):
    return {"boolean": {"value": bool(value)}}


def BY(value):
    return {"byte": {"value": value}}


def BYTES(data):
    return {"sequence": {"values": [BY(b) for b in bytes(data)]}}


def req(module, function, args, budget=STEP_BUDGET):
    return {
        "schema_version": "0.1",
        "target": {"module": module, "function": function},
        "arguments": args,
        "step_budget": budget,
    }


def run_corpus(source, corpus, backend, grants=(), corpus_path=None):
    """Run `experiment run` and return (returncode, result_dict, stderr).

    `source` is repo-relative (e.g. "src/store/chunk.mncs") or absolute.
    `corpus` is a corpus dict, or None when `corpus_path` points at a
    checked-in corpus file. `grants` are extra CLI flags
    (e.g. ["--grant-crypto", "store_chunk"]).
    """
    src = source if os.path.isabs(source) else os.path.join(REPO_ROOT, source)
    if corpus_path is not None:
        cpath = (
            corpus_path
            if os.path.isabs(corpus_path)
            else os.path.join(REPO_ROOT, corpus_path)
        )
        tmp = None
    else:
        import tempfile

        tmp = tempfile.NamedTemporaryFile(
            suffix=".json", prefix="mncs-store-case-", delete=False
        )
        tmp.write(json.dumps(corpus).encode())
        tmp.close()
        cpath = tmp.name
    try:
        cmd = [MNCS_BIN, "experiment", "run", src, "--backend", backend,
               "--corpus", cpath] + list(grants)
        env = dict(os.environ)
        env["MNCS_LIBRARY_PATH"] = library_path()
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=RUN_TIMEOUT_S, env=env
        )
        try:
            result = json.loads(proc.stdout) if proc.stdout.strip() else None
        except json.JSONDecodeError:
            result = None
        return proc.returncode, result, proc.stderr
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass


def call_many(source, module, calls, backend, grants=()):
    """Batch N (case_id, function, args) calls into one CLI invocation.

    Returns {case_id: case_result}. Raises StoreHarnessError on transport
    failure (missing CLI output, missing case).
    """
    corpus = {
        "schema_version": "0.1",
        "name": "mncs-store-batch",
        "cases": [
            {"id": cid, "request": req(module, fn, args)}
            for cid, fn, args in calls
        ],
    }
    code, result, stderr = run_corpus(source, corpus, backend, grants=grants)
    if result is None or "cases" not in result:
        raise StoreHarnessError(
            f"mncs run produced no result JSON (exit={code}): {stderr[-2000:]}"
        )
    out = {c["case_id"]: c for c in result["cases"]}
    missing = [cid for cid, _, _ in calls if cid not in out]
    if missing:
        raise StoreHarnessError(f"mncs run dropped cases: {missing}")
    return out


def require_returned(case, what):
    if case.get("status") != "returned" or not case.get("returned"):
        raise StoreHarnessError(
            f"{what}: expected a returned value, got status="
            f"{case.get('status')} failure={case.get('failure_reason')}"
        )
    if len(case["returned"]) != 1:
        raise StoreHarnessError(
            f"{what}: expected exactly one returned value, got "
            f"{len(case['returned'])}"
        )
    return case["returned"][0]


def as_bytes(value):
    """Convert a `sequence of byte` result value to Python bytes."""
    return bytes(item["byte"]["value"] for item in value["sequence"]["values"])


def as_int(value):
    return int(value["integer"]["value"])


def as_bool(value):
    return bool(value["boolean"]["value"])


def source_study(path):
    """Run source-study on a repo-relative path; return parsed JSON."""
    full = path if os.path.isabs(path) else os.path.join(REPO_ROOT, path)
    env = dict(os.environ)
    env["MNCS_LIBRARY_PATH"] = library_path()
    proc = subprocess.run(
        [MNCS_BIN, "source-study", full],
        capture_output=True, text=True, timeout=RUN_TIMEOUT_S, env=env,
    )
    return json.loads(proc.stdout)


class StoreHarnessError(Exception):
    pass
