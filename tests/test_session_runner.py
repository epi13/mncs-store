from __future__ import annotations

import gc
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import mncs_store.embedded as embedded_module
import mncs_store.session as session_module
from mncs_store.embedded import EmbeddedStore
from mncs_store.session import (
    STORE_ARTIFACT_COMPILE_TIMEOUT_SECONDS,
    STORE_ARTIFACT_OUTPUT_BYTES,
    StoreSession,
)


def test_store_artifact_compilation_uses_injected_bounded_runner(
    tmp_path: Path, monkeypatch
) -> None:
    store_root = tmp_path / "store"
    source = store_root / "src/store/application.mncs"
    source.parent.mkdir(parents=True)
    source.write_text("module store.application;\n", encoding="utf-8")
    language_root = tmp_path / "language"
    library = language_root / "library/standard.mncs"
    library.parent.mkdir(parents=True)
    library.write_text("module std.standard;\n", encoding="utf-8")
    compiler = language_root / "target/mncs"
    compiler.parent.mkdir(parents=True)
    compiler.write_bytes(b"compiler fixture")
    cache = tmp_path / "artifact-cache"

    monkeypatch.setattr(session_module, "_store_root", lambda: store_root)
    monkeypatch.setattr(session_module, "_language_root", lambda: language_root)
    monkeypatch.setenv("MNCS_BIN", str(compiler))
    monkeypatch.setenv("MNCS_STORE_ARTIFACT_CACHE", str(cache))
    monkeypatch.setattr(StoreSession, "_artifact", None)
    monkeypatch.setattr(StoreSession, "_artifact_key", None)

    calls: list[dict[str, object]] = []

    class Runner:
        @staticmethod
        def execute(command, **kwargs):
            calls.append({"command": list(command), **kwargs})
            output_dir = Path(command[command.index("--output-dir") + 1])
            (output_dir / "backend.json").write_bytes(b"bounded-store-artifact")
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    session = object.__new__(StoreSession)
    session._command_executor = Runner()
    artifact = session._compile_artifact()

    assert artifact == b"bounded-store-artifact"
    assert len(calls) == 1
    call = calls[0]
    assert call["cwd"] == store_root
    assert call["timeout"] == STORE_ARTIFACT_COMPILE_TIMEOUT_SECONDS == 300
    assert call["output_cap"] == STORE_ARTIFACT_OUTPUT_BYTES == 128 * 1024 * 1024
    assert call["stderr_cap"] == STORE_ARTIFACT_OUTPUT_BYTES


def test_embedded_store_closes_owned_session_when_collected(
    tmp_path: Path, monkeypatch
) -> None:
    class Session:
        closed = 0
        command_executor: object | None = None

        def close(self) -> None:
            self.closed += 1

    session = Session()
    def create_session(*, command_executor=None):
        session.command_executor = command_executor
        return session

    executor = object()
    monkeypatch.setattr(embedded_module, "StoreSession", create_session)
    monkeypatch.setattr(EmbeddedStore, "_recover", lambda _self: None)

    store = EmbeddedStore(
        tmp_path / "state", command_executor=executor, verify_on_open=False
    )
    assert session.command_executor is executor
    del store
    gc.collect()

    assert session.closed == 1


def test_store_cache_artifact_and_manifest_reads_have_byte_bounds(
    tmp_path: Path, monkeypatch
) -> None:
    cache = tmp_path / "artifact-cache"
    key = "a" * 64
    entry = cache / key
    entry.mkdir(parents=True)
    artifact = b"x" * 17
    (entry / "backend.json").write_bytes(artifact)
    (entry / "manifest.json").write_text(
        json.dumps(
            {
                "schema": "mncs-store-artifact-cache/1",
                "key": key,
                "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MNCS_STORE_ARTIFACT_CACHE", str(cache))
    monkeypatch.setattr(session_module, "STORE_ARTIFACT_MAX_BYTES", 16)
    assert StoreSession._read_cached_artifact(key) is None

    (entry / "backend.json").write_bytes(b"x")
    monkeypatch.setattr(session_module, "STORE_ARTIFACT_MANIFEST_MAX_BYTES", 16)
    assert StoreSession._read_cached_artifact(key) is None
