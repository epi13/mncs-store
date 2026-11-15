"""Retained mncs-embed session adapter for Store semantic calls.

The Store driver still owns filesystem mechanics, but this adapter removes
the per-batch compiler/CLI process from its semantic boundary.  The C ABI is
only the existing typed transport surface of ``mncs-embed``; canonical Store
bytes continue to be produced by the retained MNCS artifact.
"""

from __future__ import annotations

import ctypes
import atexit
from collections import OrderedDict
import json
import os
import re
import tempfile
import time
import sys
import threading
from pathlib import Path

from store_phase1a import Engine, StoreHarnessError
from mncs_exec import MNCS_BIN as SELECTED_MNCS_BIN, _run


STORE_ROOT = Path(
    os.environ.get("MNCS_STORE_ROOT", str(Path(__file__).resolve().parents[1]))
)
LANGUAGE_ROOT = Path(
    os.environ.get("MNCS_LANGUAGE_ROOT", "/home/epi13/Documents/Projects/mncs-language")
)
MNCS_BIN = Path(SELECTED_MNCS_BIN)
EMBED_LIB = Path(
    os.environ.get(
        "MNCS_EMBED_LIB", str(MNCS_BIN.with_name("libmncs_embed.so"))
    )
)
BACKEND = "mncs-research-bytecode"
APPLICATION_SOURCE = "src/store/application.mncs"


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
        if hasattr(library, "mncs_session_compare_reference"):
            library.mncs_session_compare_reference.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
            library.mncs_session_compare_reference.restype = ctypes.c_void_p
        if hasattr(library, "mncs_session_bind_reference"):
            library.mncs_session_bind_reference.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            library.mncs_session_bind_reference.restype = ctypes.c_void_p
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

    def __init__(self, source: str, *, artifact: bytes | None = None, environment=None,
                 reference=False, evidence=None):
        self.source = source
        self._temporary = None
        self.compile_seconds = 0.0
        self.compiler_timings = {}
        self.compiler_stderr = ""
        self.reference_evidence = evidence
        if artifact is None:
            artifact = self._compile(source, environment, reference)
        self._artifact = artifact
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
        if reference:
            response = self._library_handle.mncs_session_bind_reference(
                self._handle, json.dumps(self.reference_evidence).encode())
            if not response:
                error = self._library_handle.mncs_last_error()
                self.close()
                raise StoreHarnessError(f"independent reference admission failed: {error}")
            try:
                identity = json.loads(self._library_handle.mncs_response_text(response))
                if identity != self.reference_evidence["identity"]:
                    self.close()
                    raise StoreHarnessError("reference admission returned a different evidence identity")
            finally:
                self._library_handle.mncs_response_free(response)

    def _compile(self, source, environment=None, reference=False):
        from mncs_store.session import _library_roots
        self._temporary = tempfile.TemporaryDirectory(prefix="mncs-store-artifact-")
        output_dir = Path(self._temporary.name)
        source_path = STORE_ROOT / source
        if not source_path.is_file():
            self.close()
            raise StoreHarnessError(f"MNCS source not found: {source_path}")
        if environment is None:
            environment = dict(os.environ)
            environment["MNCS_LIBRARY_PATH"] = os.pathsep.join(
                [str(root) for root in _library_roots(LANGUAGE_ROOT, environment)]
                + [str(STORE_ROOT / "src")]
            )
        else:
            environment = dict(environment)
        environment["MNCS_TIMINGS"] = "1"
        compile_started = time.perf_counter()
        try:
            completed = _run(
                [str(MNCS_BIN), "compile", str(source_path), "--emit",
                 "backend,evidence" if reference else "backend", "--quiet", "--omit-result",
                 "--output-dir", str(output_dir), "--target", BACKEND],
                env=environment,
            )
        except BaseException:
            self.close()
            raise
        self.compile_seconds = time.perf_counter() - compile_started
        self.compiler_timings = self._parse_timings(completed.stderr)
        self.compiler_stderr = completed.stderr
        artifact_path = output_dir / "backend.json"
        if completed.returncode != 0 or not artifact_path.is_file():
            detail = (completed.stderr or completed.stdout)[-3000:]
            self.close()
            raise StoreHarnessError(f"retained artifact admission failed for {source}: {detail}")
        try:
            artifact = artifact_path.read_bytes()
            if reference:
                self.reference_evidence = json.loads((output_dir / "evidence.json").read_text())
        finally:
            self._temporary.cleanup()
            self._temporary = None
        return artifact

    def compare_reference(self, corpus, grants=()):
        if self.closed or not self._handle:
            raise StoreHarnessError("retained session is closed")
        response = self._library_handle.mncs_session_compare_reference(
            self._handle, json.dumps(corpus).encode(), json.dumps(self._grants(grants)).encode())
        if not response:
            raise StoreHarnessError(f"reference comparison failed: {self._library_handle.mncs_last_error()}")
        try:
            return json.loads(self._library_handle.mncs_response_text(response))
        finally:
            self._library_handle.mncs_response_free(response)

    @staticmethod
    def _parse_timings(stderr):
        timings = {}
        for line in stderr.splitlines():
            match = re.search(r"mncs-timing stage=([^ ]+)(?: .*?exclusive_elapsed_ms=([0-9]+)| .*?elapsed_ms=([0-9]+))", line)
            if match:
                value = match.group(2) or match.group(3)
                timings[match.group(1)] = int(value)
        return timings

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
                "function": call[1],
                "args": call[2],
                "grants": self._grants(grants),
                "step_budget": call[3] if len(call) > 3 else 32768,
            }
            for call in calls
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
            call[0]: output for call, output in zip(calls, outputs)
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

    def __init__(self, backend=BACKEND, application_source=APPLICATION_SOURCE):
        super().__init__(backend=backend)
        if backend != BACKEND:
            raise StoreHarnessError("retained adapter currently supports research-bytecode only")
        self.application_source = application_source
        self.sessions: dict[str, RetainedSession] = {}
        self.source_aliases: dict[str, str] = {}
        self.cold_admission_seconds = 0.0
        self.session_reopen_seconds = []
        self._closed = False

    def run(self, source, module, calls, grants=()):
        admitted_source = self.application_source or source
        session = self.sessions.get(admitted_source)
        if session is None:
            started = time.perf_counter()
            session = RetainedSession(admitted_source)
            self.cold_admission_seconds += time.perf_counter() - started
            self.sessions[admitted_source] = session
        self.source_aliases[source] = admitted_source
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
            "compile_seconds": sum(
                getattr(session, "compile_seconds", 0.0) for session in sessions
            ),
            "artifact_bytes": sum(len(getattr(session, "_artifact", b"")) for session in sessions),
            "source_aliases": dict(sorted(self.source_aliases.items())),
            "admission_by_source": [
                {
                    "source": session.source,
                    "compile_seconds": session.compile_seconds,
                    "session_open_seconds": session.session_open_seconds,
                    "artifact_bytes": len(session._artifact),
                    "compiler_timings_ms": dict(session.compiler_timings),
                }
                for session in sorted(sessions, key=lambda item: item.source)
            ],
            "semantic_batch_count": len(batches),
            "semantic_call_count": len(calls),
            "mean_per_operation_seconds": (sum(calls) / len(calls)) if calls else 0.0,
            "mean_batched_operation_seconds": (sum(batches) / len(batches)) if batches else 0.0,
            "max_batch_size": max(sizes, default=0),
            "close_reopen_seconds": sum(self.session_reopen_seconds),
        }


# The frozen lifecycle oracle uses the same retained transport as native
# substrate tests, with reference comparison on EVERY new corpus. This is
# compilation/session reuse, never result reuse or a changed storage driver.
_reference_sessions = OrderedDict()
_reference_lock = threading.RLock()
_reference_library_stamp = None
METRICS = {"compilations": 0, "cache_hits": 0, "opens": 0, "batches": 0}


def _stamp(path):
    s = path.stat()
    return (str(path), s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def _closure_stamp(roots, source, compiler, bundle):
    files = {p.resolve() for root in roots for p in root.rglob("*.mncs")}
    if bundle:
        files.add(Path(bundle).expanduser().resolve())
    return tuple(_stamp(p) for p in sorted(files | {source.resolve(), compiler}))


def _cached_reference(cache, key, material):
    """Read one existing bounded cache entry with independent compiler evidence."""
    from mncs_store import artifact_cache
    root = cache._artifact_cache_root()
    try:
        with artifact_cache.lease(root, exclusive=False):
            artifact = cache._read_cached_artifact_locked(root, key)
            if artifact is None:
                return None
            manifest = json.loads((root / key / "manifest.json").read_text())
            recorded = manifest.get("material", {})
            if not isinstance(recorded, dict):
                return None
            if any(recorded.get(k) != v for k, v in material.items()):
                return None
            evidence = recorded.get("reference_evidence")
            return (artifact, evidence) if isinstance(evidence, dict) else None
    except (OSError, ValueError, TypeError):
        return None


def close_reference_sessions():
    with _reference_lock:
        for _, session in _reference_sessions.values():
            session.close()
        _reference_sessions.clear()


atexit.register(close_reference_sessions)


def run_reference_calls(source, corpus, backend, grants):
    from mncs_exec import MNCS_BIN, REPO_ROOT, library_path
    global _reference_library_stamp
    if backend != BACKEND or os.environ.get("MNCS_TEST_TRANSPORT") == "cli":
        return None
    if len(grants) % 2 or any(grants[i] != "--grant-crypto" for i in range(0, len(grants), 2)):
        return None
    if not corpus["cases"] or not EMBED_LIB.is_file():
        return None
    with _reference_lock:
        library = RetainedSession._load_library()
        if not all(hasattr(library, name) for name in
                   ("mncs_session_compare_reference", "mncs_session_bind_reference")):
            return None  # Older toolchain retains full reference CLI checks.
        stamp = _stamp(EMBED_LIB)
        if _reference_library_stamp is not None and stamp != _reference_library_stamp:
            raise StoreHarnessError("embed library changed during tests; restart pytest")
        _reference_library_stamp = stamp
        compiler = Path(MNCS_BIN).resolve()
        root = Path(REPO_ROOT)
        if str(Path(__file__).resolve().parents[1] / "python") not in sys.path:
            sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
        from mncs_store.session import StoreSession
        src = Path(source) if os.path.isabs(source) else root / source
        env = dict(os.environ, MNCS_LIBRARY_PATH=library_path())
        language = compiler.parents[2]
        # Stat closure includes added/removed imports and in-place writes.
        # Content is hashed by the existing cache admission on every change.
        roots = [root / "src", *(Path(p) for p in library_path().split(os.pathsep))]
        bundle = env.get("MNCS_STDLIB_BUNDLE")
        fingerprint = _closure_stamp(roots, src, compiler, bundle)
        selection = (str(src.resolve()), backend, str(compiler), tuple(sorted(
            (k, v) for k, v in env.items() if k.startswith("MNCS_"))))
        previous = _reference_sessions.get(selection)
        if previous is None or previous[0] != fingerprint:
            if previous is not None:
                previous[1].close()
                del _reference_sessions[selection]
            key, material = StoreSession._artifact_identity_material(
                source=src, compiler=compiler, language=language, store_root=root,
                target=backend, environment=env)
            cached = _cached_reference(StoreSession, key, material)
            session = None
            if cached is not None:
                try:
                    session = RetainedSession(source, artifact=cached[0],
                                              evidence=cached[1], reference=True)
                    METRICS["cache_hits"] += 1
                except StoreHarnessError:
                    # Disposable cache admission failed. Recompile once from
                    # the selected source/toolchain, retaining every check.
                    pass
            cache_miss = session is None
            if cache_miss:
                METRICS["compilations"] += 1
                session = RetainedSession(str(src), environment=env, reference=True)
            if _closure_stamp(roots, src, compiler, bundle) != fingerprint:
                session.close()
                raise StoreHarnessError("source closure changed during reference admission; retry")
            if cache_miss:
                StoreSession._write_cached_artifact(key,
                    dict(material, reference_evidence=session.reference_evidence), session._artifact)
            _reference_sessions[selection] = (fingerprint, session)
            METRICS["opens"] += 1
            while len(_reference_sessions) > 8:
                _, (_, old) = _reference_sessions.popitem(last=False)
                old.close()
        _reference_sessions.move_to_end(selection)
        session = _reference_sessions[selection][1]
        comparison = session.compare_reference(corpus, grants)
        if (comparison["status"] != "consistent_over_corpus" or comparison["mismatching_cases"]
                or comparison["matching_cases"] != len(corpus["cases"])):
            raise StoreHarnessError(f"reference layers did not agree: {comparison}")
        calls = [(c["id"], c["request"]["target"]["function"],
                  c["request"]["arguments"], c["request"]["step_budget"])
                 for c in corpus["cases"]]
        module = corpus["cases"][0]["request"]["target"]["module"]
        METRICS["batches"] += 1
        return {case_id: dict(output, case_id=case_id, failure_reason=output.get("failure_reason")) for case_id, output in
                session.call_batch(module, calls, grants).items()}
