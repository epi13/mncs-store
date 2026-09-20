"""Retained mncs-embed session adapter for Store semantic calls.

The Store driver still owns filesystem mechanics, but this adapter removes
the per-batch compiler/CLI process from its semantic boundary.  The C ABI is
only the existing typed transport surface of ``mncs-embed``; canonical Store
bytes continue to be produced by the retained MNCS artifact.
"""

from __future__ import annotations

import ctypes
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

from store_phase1a import Engine, StoreHarnessError


STORE_ROOT = Path(__file__).resolve().parents[1]
LANGUAGE_ROOT = Path(
    os.environ.get("MNCS_LANGUAGE_ROOT", "/home/epi13/Documents/Projects/mncs-language")
)
MNCS_BIN = Path(
    os.environ.get("MNCS_BIN", str(LANGUAGE_ROOT / "target" / "debug" / "mncs"))
)
EMBED_LIB = Path(
    os.environ.get(
        "MNCS_EMBED_LIB", str(LANGUAGE_ROOT / "target" / "debug" / "libmncs_embed.so")
    )
)
BACKEND = "mncs-research-bytecode"


class RetainedSession:
    """One compiled artifact retained across typed calls."""

    _library = None

    @classmethod
    def _load_library(cls):
        if cls._library is not None:
            return cls._library
        if not EMBED_LIB.is_file():
            raise StoreHarnessError(f"mncs-embed library not found: {EMBED_LIB}")
        library = ctypes.CDLL(str(EMBED_LIB))
        library.mncs_session_open.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        library.mncs_session_open.restype = ctypes.c_void_p
        library.mncs_session_call_batch.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        library.mncs_session_call_batch.restype = ctypes.c_void_p
        library.mncs_session_close.argtypes = [ctypes.c_void_p]
        library.mncs_session_close.restype = None
        library.mncs_response_text.argtypes = [ctypes.c_void_p]
        library.mncs_response_text.restype = ctypes.c_char_p
        library.mncs_response_free.argtypes = [ctypes.c_void_p]
        library.mncs_response_free.restype = None
        library.mncs_last_error.argtypes = []
        library.mncs_last_error.restype = ctypes.c_char_p
        cls._library = library
        return library

    def __init__(self, source: str):
        self.source = source
        self._temporary = tempfile.TemporaryDirectory(prefix="mncs-store-artifact-")
        output_dir = Path(self._temporary.name)
        source_path = STORE_ROOT / source
        if not source_path.is_file():
            self.close()
            raise StoreHarnessError(f"MNCS source not found: {source_path}")
        environment = dict(os.environ)
        environment["MNCS_LIBRARY_PATH"] = os.pathsep.join(
            [str(LANGUAGE_ROOT / "library"), str(STORE_ROOT / "src")]
        )
        compile_started = time.perf_counter()
        completed = subprocess.run(
            [
                str(MNCS_BIN),
                "compile",
                str(source_path),
                "--emit",
                "backend",
                "--output-dir",
                str(output_dir),
                "--target",
                BACKEND,
            ],
            capture_output=True,
            text=True,
            timeout=280,
            env=environment,
            check=False,
        )
        self.compile_seconds = time.perf_counter() - compile_started
        artifact_path = output_dir / "backend.json"
        if completed.returncode != 0 or not artifact_path.is_file():
            detail = (completed.stderr or completed.stdout)[-3000:]
            self.close()
            raise StoreHarnessError(f"retained artifact admission failed for {source}: {detail}")
        self._artifact = artifact_path.read_bytes()
        self._library_handle = self._load_library()
        open_started = time.perf_counter()
        self._handle = self._library_handle.mncs_session_open(
            self._artifact, len(self._artifact)
        )
        self.session_open_seconds = time.perf_counter() - open_started
        if not self._handle:
            error = self._library_handle.mncs_last_error()
            detail = error.decode() if error else "unknown session-open failure"
            self.close()
            raise StoreHarnessError(f"retained session open failed for {source}: {detail}")
        self.call_seconds: list[float] = []
        self.batch_seconds: list[float] = []
        self.batch_sizes: list[int] = []
        self.closed = False

    @staticmethod
    def _grants(values):
        values = list(values or ())
        if not values:
            return []
        if len(values) % 2:
            raise StoreHarnessError(f"unpaired grant arguments: {values!r}")
        grants = []
        for flag, capability in zip(values[::2], values[1::2]):
            if flag == "--grant-crypto":
                grants.append(
                    {
                        "capability": capability,
                        "locator": "crypto-verify",
                        "bytes": [],
                    }
                )
                continue
            if flag == "--grant-fs":
                try:
                    name, path = capability.split("=", 1)
                except ValueError as error:
                    raise StoreHarnessError(f"invalid filesystem grant: {capability!r}") from error
                if not name or not path:
                    raise StoreHarnessError(f"invalid filesystem grant: {capability!r}")
                grants.append(
                    {
                        "capability": name,
                        "locator": path,
                        "bytes": [],
                    }
                )
                continue
            if flag:
                raise StoreHarnessError(f"retained adapter does not support grant {flag!r}")
        return grants

    def call_batch(self, module, calls, grants=()):
        if self.closed or not self._handle:
            raise StoreHarnessError("retained session is closed")
        requests = [
            {
                "module": module,
                "function": function,
                "args": arguments,
                "grants": self._grants(grants),
                "step_budget": 32768,
            }
            for _case_id, function, arguments in calls
        ]
        started = time.perf_counter()
        response = self._library_handle.mncs_session_call_batch(
            self._handle, json.dumps(requests, separators=(",", ":")).encode()
        )
        elapsed = time.perf_counter() - started
        self.batch_seconds.append(elapsed)
        self.batch_sizes.append(len(calls))
        if not response:
            error = self._library_handle.mncs_last_error()
            detail = error.decode() if error else "unknown retained call failure"
            raise StoreHarnessError(detail)
        try:
            raw = self._library_handle.mncs_response_text(response)
            outputs = json.loads(raw.decode())
        finally:
            self._library_handle.mncs_response_free(response)
        if len(outputs) != len(calls):
            raise StoreHarnessError("retained batch returned the wrong number of outputs")
        per_call = elapsed / len(calls) if calls else 0.0
        self.call_seconds.extend([per_call] * len(calls))
        return {
            case_id: output for (case_id, _function, _arguments), output in zip(calls, outputs)
        }

    def close(self):
        if getattr(self, "closed", False):
            return
        started = time.perf_counter()
        handle = getattr(self, "_handle", None)
        library = getattr(self, "_library_handle", None)
        if handle and library is not None:
            library.mncs_session_close(handle)
        self._handle = None
        temporary = getattr(self, "_temporary", None)
        if temporary is not None:
            temporary.cleanup()
        self.close_seconds = time.perf_counter() - started
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        self.close()


class RetainedEngine(Engine):
    """Store ``Engine`` compatibility backed by retained source sessions."""

    def __init__(self, backend=BACKEND):
        super().__init__(backend=backend)
        if backend != BACKEND:
            raise StoreHarnessError("retained adapter currently supports research-bytecode only")
        self.sessions: dict[str, RetainedSession] = {}
        self.cold_admission_seconds = 0.0
        self.session_reopen_seconds = []
        self._closed = False

    def run(self, source, module, calls, grants=()):
        session = self.sessions.get(source)
        if session is None:
            started = time.perf_counter()
            session = RetainedSession(source)
            self.cold_admission_seconds += time.perf_counter() - started
            self.sessions[source] = session
        self.invocations += 1
        output = session.call_batch(module, calls, grants=grants)
        self.bytes_out += len(str(calls))
        self.bytes_in += len(str(output))
        return output

    def close(self):
        if self._closed:
            return
        started = time.perf_counter()
        for session in self.sessions.values():
            session.close()
        self.session_reopen_seconds.append(time.perf_counter() - started)
        self._closed = True

    def metrics(self):
        sessions = list(self.sessions.values())
        calls = [value for session in sessions for value in session.call_seconds]
        batches = [value for session in sessions for value in session.batch_seconds]
        sizes = [value for session in sessions for value in session.batch_sizes]
        return {
            "cold_admission_seconds": self.cold_admission_seconds,
            "session_count": len(sessions),
            "session_open_seconds": sum(
                getattr(session, "session_open_seconds", 0.0) for session in sessions
            ),
            "semantic_batch_count": len(batches),
            "semantic_call_count": len(calls),
            "mean_per_operation_seconds": (sum(calls) / len(calls)) if calls else 0.0,
            "mean_batched_operation_seconds": (sum(batches) / len(batches)) if batches else 0.0,
            "max_batch_size": max(sizes, default=0),
            "close_reopen_seconds": sum(self.session_reopen_seconds),
        }
