"""Observable provider transport and authoritative selected runtime contracts."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LANGUAGE = ROOT.parent / "mncs-language"
SCRIPT = ROOT / "scripts/provider.py"
spec = importlib.util.spec_from_file_location("store_provider", SCRIPT)
provider = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provider)


def invoke(operation, request=None, extra=None, env=None):
    selected = {
        **os.environ,
        "MNCS_STORE_ROOT": str(ROOT),
        "MNCS_LANGUAGE_ROOT": str(LANGUAGE),
        "MNCS_BIN": str(LANGUAGE / "target/release/mncs"),
        "MNCS_EMBED_LIB": str(LANGUAGE / "target/release/libmncs_embed.so"),
        "MNCS_CLI": "/invalid-ambient/compiler",
        "MNCS_STORE_ARTIFACT": "/invalid-ambient/artifact",
        **(env or {}),
    }
    argv = [sys.executable, str(SCRIPT), operation, *(extra or [])]
    if request is not None:
        argv += ["--request", str(request)]
    result = subprocess.run(
        argv,
        cwd="/tmp",
        env=selected,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    return result, json.loads(result.stdout) if result.stdout else None


def test_manifest_operations_have_owned_entrypoints_effects_and_selected_toolchains():
    manifest = json.loads((ROOT / ".mncs/project.json").read_text())
    entries = {entry["contract"]: entry for entry in manifest["contracts"]["provides"]}
    for operation in provider.OPERATIONS:
        name = (
            "adaptive-representations"
            if operation == "describe"
            else "adaptive-" + operation
        )
        entry = entries[name]
        assert entry["effects"] == (
            ["write"] if operation in ("admit", "add-representation") else ["read"]
        )
        assert entry["invocation"]["fixed_argv"] == [operation]
        assert entry["invocation"]["path"] == "scripts/provider.py"
        assert entry["invocation"]["toolchain"]["repository"] == "mncs-language"


def test_manifest_tests_select_runtime_and_suppress_ambient_store_artifact():
    manifest = json.loads((ROOT / ".mncs/project.json").read_text())
    for test in manifest["contracts"]["tests"]:
        command = test["command"]
        assert command["toolchain"] == {
            "repository": "mncs-language",
            "path": "target/release/mncs",
        }
        assert command["toolchain_env"] == "MNCS_BIN"
        assert command["environment"]["MNCS_STORE_ARTIFACT"] == ""


def test_describe_is_cwd_independent_and_pins_runtime():
    process, result = invoke("describe")
    assert process.returncode == 0
    assert result["selected"]["MNCS_BIN"] == str(LANGUAGE / "target/release/mncs")
    assert "add-representation" in result["result"]["operations"]
    assert result["result"]["plan_authority"] == "proposal provenance, never permission"
    assert result["result"]["satisfied"] == "required fidelity only"


@pytest.mark.parametrize(
    "override,code",
    [
        ({"MNCS_STORE_ROOT": "/tmp/another-store"}, "SELECTED_PROVIDER_MISMATCH"),
        ({"MNCS_BIN": "/tmp/ambient-mncs"}, "SELECTED_TOOLCHAIN_UNAVAILABLE"),
        ({"MNCS_EMBED_LIB": "/tmp/ambient-embed"}, "SELECTED_TOOLCHAIN_UNAVAILABLE"),
        ({"MNCS_LANGUAGE_ROOT": ""}, "PATH_NOT_ABSOLUTE"),
    ],
)
def test_runtime_selection_fails_closed(override, code):
    process, result = invoke("describe", env=override)
    assert process.returncode == 2 and result["diagnostic"]["code"] == code


def test_fixed_operation_cannot_be_overridden():
    process, result = invoke("describe", extra=["admit"])
    assert process.returncode == 2 and result is None


def test_provider_fresh_process_selective_read_and_plan_refusal(tmp_path):
    source = tmp_path / "source"
    first, other = b"A" * 65536, b"B" * 65536
    source.write_bytes(first + other)
    request = tmp_path / "request.json"
    root = tmp_path / "store"
    common = {
        "store": str(root),
        "domain_schema": {"utf8": "provider-contract"},
        "domain_identity": {"utf8": "one"},
    }
    request.write_text(
        json.dumps(
            {
                **common,
                "expected_generation": 0,
                "descriptor": {"utf8": "test"},
                "payload": {"file": str(source)},
                "synopsis": {"utf8": "two regions"},
                "blocks": [
                    {"index": 0, "tag": 1, "start": 0, "length": 65536},
                    {"index": 1, "tag": 2, "start": 65536, "length": 65536},
                ],
            }
        )
    )
    assert invoke("admit", request)[1]["result"]["code"] == "COMMITTED"
    import hashlib

    unrelated = root / "chunks" / (hashlib.sha256(other).hexdigest() + ".chunk")
    unrelated.unlink()
    request.write_text(json.dumps(common))
    result = invoke("inspect-envelope", request)[1]
    assert result["status"] == "ok"
    assert result["metrics"]["retained_calls"] > 0
    assert result["metrics"]["request_transport_bytes"] > 0
    request.write_text(json.dumps({**common, "tag": 1}))
    process, result = invoke(
        "materialize", request, extra=["--artifact-dir", str(tmp_path / "artifacts")]
    )
    assert process.returncode == 0
    assert Path(result["result"]["payload"]["file"]).read_bytes() == first
    request.write_text(json.dumps({**common, "mask": 4}))
    result = invoke("read-blocks", request)[1]
    assert result["diagnostic"]["code"] == "DENIED"
    assert result["diagnostic"]["detail_code"] == "INVALID_BLOCK_MASK"
    request.write_text(json.dumps({**common, "intent": {"fidelity": 9}}))
    assert invoke("select", request)[1]["diagnostic"]["code"] == "MALFORMED_INTENT"


def test_transport_bounds_and_absolute_paths_fail_explicitly(tmp_path):
    with pytest.raises(provider.RequestError, match="absolute"):
        provider.absolute("relative")
    path = tmp_path / "large"
    path.write_bytes(b"x" * 10)
    with pytest.raises(provider.RequestError, match="exceeds"):
        provider.bounded_file(path, 9)
    with pytest.raises(provider.RequestError, match="exactly one"):
        provider.bytes_input({"hex": "00", "utf8": "x"})
