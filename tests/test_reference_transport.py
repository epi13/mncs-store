"""Real reference-oracle equivalence, admission invalidation and cancellation."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
import mncs_exec as execution
import retained_session as retained


@pytest.fixture
def reference(tmp_path, monkeypatch):
    retained.close_reference_sessions()
    monkeypatch.setenv("MNCS_STORE_ARTIFACT_CACHE", str(tmp_path / "artifacts"))
    monkeypatch.delenv("MNCS_TEST_TRANSPORT", raising=False)
    if not hasattr(retained.RetainedSession._load_library(), "mncs_session_bind_reference"):
        pytest.skip("reference comparison unavailable: build the selected mncs-embed toolchain")
    yield
    retained.close_reference_sessions()


def test_reference_reuse_matches_full_cli_for_new_inputs_and_budgets(reference):
    before = dict(retained.METRICS)
    for n, budget in [(0, 32768), (2**32 - 1, 32768), (4, 1), (7, 32768)]:
        request = execution.req("store.identity.v1", "object_encode",
                                [execution.U32(n), execution.U64(n)], budget)
        corpus = {"schema_version": "0.1", "name": "equivalence",
                  "cases": [{"id": "case", "request": request}]}
        code, result, stderr = execution.run_corpus("src/store/identity.mncs", corpus, retained.BACKEND)
        assert result and "cases" in result, (code, stderr)
        expected = result["cases"][0]
        actual = execution.call_many("src/store/identity.mncs", "store.identity.v1",
                    [("case", "object_encode", request["arguments"], budget)], retained.BACKEND)["case"]
        for field in ("case_id", "status", "returned", "steps", "failure_reason"):
            assert actual[field] == expected[field]
        assert actual.get("effects", []) == expected.get("effects", [])
    assert retained.METRICS["compilations"] - before["compilations"] == 1
    assert retained.METRICS["opens"] - before["opens"] == 1
    assert retained.METRICS["batches"] - before["batches"] == 4


def test_grants_remain_request_specific(reference):
    calls = [("hash", "chain_init", [execution.BYTES(bytes(28))])]
    def run(grants):
        return execution.call_many("src/store/manifest.mncs", "store.manifest.v1",
                                   calls, retained.BACKEND, grants)["hash"]
    granted = run(["--grant-crypto", "store_manifest"])
    with pytest.raises(execution.StoreHarnessError, match="unsupported"):
        run([])
    granted_again = run(["--grant-crypto", "store_manifest"])
    assert granted["status"] == granted_again["status"] == "returned"
    assert granted["returned"] == granted_again["returned"]


def test_source_invalidation_and_corrupt_disposable_cache(reference, tmp_path, monkeypatch):
    root = tmp_path / "project"
    (root / "src").mkdir(parents=True)
    source = root / "src/probe.mncs"
    def write(offset):
        source.write_text(f"mncs 0.6;\nmodule probe.reference;\nfn add(x: u32) -> (y: u32) {{ return x +% {offset}; }}\n")
    monkeypatch.setattr(execution, "REPO_ROOT", str(root))
    def run():
        out = execution.call_many("src/probe.mncs", "probe.reference",
                                   [("one", "add", [execution.U32(8)])], retained.BACKEND)
        return execution.as_int(execution.require_returned(out["one"], "changed source"))
    write(1)
    assert run() == 9
    write(3)
    assert run() == 11
    retained.close_reference_sessions()
    for artifact in (tmp_path / "artifacts").glob("*/backend.json"):
        artifact.write_bytes(b"corrupt cache")
    assert run() == 11  # checksum failure recompiles, never executes stale bytes
    retained.close_reference_sessions()
    for manifest in (tmp_path / "artifacts").glob("*/manifest.json"):
        record = json.loads(manifest.read_text())
        record["material"]["reference_evidence"]["input"]["fingerprint"] = "0" * 64
        manifest.write_text(json.dumps(record))
    before = retained.METRICS["compilations"]
    assert run() == 11  # intact backend bytes cannot stand in for independent evidence
    assert retained.METRICS["compilations"] == before + 1
    retained.close_reference_sessions()
    for manifest in (tmp_path / "artifacts").glob("*/manifest.json"):
        record = json.loads(manifest.read_text())
        record["material"] = []
        manifest.write_text(json.dumps(record))
    assert run() == 11


@pytest.mark.parametrize("interrupt", [False, True])
def test_compiler_timeout_and_interrupt_reap_backend_children(tmp_path, interrupt):
    child_pid = tmp_path / "child.pid"
    # The grandchild ignores TERM, forcing the whole-group KILL fallback.
    child = "import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)"
    parent = ("import subprocess,sys,time;from pathlib import Path;"
              f"p=subprocess.Popen([sys.executable,'-c',{child!r}]);"
              f"Path({str(child_pid)!r}).write_text(str(p.pid));time.sleep(60)")
    runner = ("import os,sys;sys.path.insert(0,'tests');import mncs_exec as m;"
              f"m.RUN_TIMEOUT_S={30 if interrupt else 0.5};"
              f"m._run([sys.executable,'-c',{parent!r}],dict(os.environ))")
    process = subprocess.Popen([sys.executable, "-c", runner], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    try:
        deadline = time.monotonic() + 5
        while not child_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert child_pid.exists()
        if interrupt:
            process.send_signal(signal.SIGINT)
        _, stderr = process.communicate(timeout=8)
        assert process.returncode != 0
        assert (b"KeyboardInterrupt" if interrupt else b"TimeoutExpired") in stderr
        status = Path(f"/proc/{child_pid.read_text()}/stat")
        assert not status.exists() or status.read_text().split()[2] == "Z"
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
        if child_pid.exists():
            try:
                os.kill(int(child_pid.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass
