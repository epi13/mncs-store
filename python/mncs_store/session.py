"""Retained typed MNCS session used by the embedded Store boundary.

This is host transport only.  The Store source artifact owns the semantic
encoding, comparison, recovery, and integrity decisions executed through it.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from .errors import StoreError, StoreResultCode

STORE_ARTIFACT_COMPILE_TIMEOUT_SECONDS = 300
STORE_ARTIFACT_OUTPUT_BYTES = 128 * 1024 * 1024
STORE_ARTIFACT_MAX_BYTES = 128 * 1024 * 1024
STORE_ARTIFACT_MANIFEST_MAX_BYTES = 8 * 1024 * 1024


def _store_root() -> Path:
    configured = os.environ.get("MNCS_STORE_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


def _language_root() -> Path:
    configured = os.environ.get("MNCS_LANGUAGE_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path("/home/epi13/Documents/Projects/mncs-language")


def _language_target(filename: str) -> Path:
    """Choose the fastest locally built language binary, with debug fallback.

    Store still accepts explicit ``MNCS_BIN``/``MNCS_EMBED_LIB`` overrides.
    When no override is supplied, a completed release build is the normal
    resident-development realization; a clean checkout naturally falls back
    to the debug artifact used by the existing test workflow.
    """

    language = _language_root()
    for profile in ("release", "debug"):
        candidate = language / "target" / profile / filename
        if candidate.is_file():
            return candidate
    return language / "target" / "debug" / filename


def u16(value: int) -> dict[str, Any]:
    return {"integer": {"value": int(value), "type": {"bits": 16, "signed": False}}}


def u32(value: int) -> dict[str, Any]:
    return {"integer": {"value": int(value), "type": {"bits": 32, "signed": False}}}


def u64(value: int) -> dict[str, Any]:
    return {"integer": {"value": int(value), "type": {"bits": 64, "signed": False}}}


def byte(value: int) -> dict[str, Any]:
    return {"byte": {"value": int(value)}}


def bytes_value(value: bytes) -> dict[str, Any]:
    return {"sequence": {"values": [byte(item) for item in value]}}


def u32_values(values: list[int] | tuple[int, ...]) -> dict[str, Any]:
    return {"sequence": {"values": [u32(item) for item in values]}}


def as_int(value: dict[str, Any]) -> int:
    return int(value["integer"]["value"])


def as_bytes(value: dict[str, Any]) -> bytes:
    return bytes(item["byte"]["value"] for item in value["sequence"]["values"])


def _returned(output: dict[str, Any]) -> dict[str, Any]:
    if output.get("status") != "returned" or len(output.get("returned", [])) != 1:
        raise StoreError(
            StoreResultCode.INTEGRITY_FAILURE,
            f"MNCS Store call did not return one value: {output.get('failure_reason')}",
        )
    return output["returned"][0]


def _record_fields(value: dict[str, Any]) -> dict[str, dict[str, Any]]:
    record = value.get("record")
    if not isinstance(record, dict) or not isinstance(record.get("fields"), list):
        raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store digest state is not a record")
    return {str(name): field for name, field in record["fields"]}


class StoreSession:
    """One retained Store application artifact and MNCS application session."""

    _library: Any = None
    _library_lock = threading.Lock()
    _artifact: bytes | None = None
    _artifact_key: str | None = None
    _last_artifact_cache_hit = False
    _artifact_lock = threading.Lock()

    @classmethod
    def _load_library(cls) -> Any:
        with cls._library_lock:
            if cls._library is not None:
                return cls._library
            embed = Path(os.environ.get("MNCS_EMBED_LIB", str(_language_target("libmncs_embed.so"))))
            if not embed.is_file():
                raise StoreError(
                    StoreResultCode.PLATFORM_UNSUPPORTED,
                    f"retained MNCS embed library is unavailable: {embed}",
                )
            library = ctypes.CDLL(str(embed))
            library.mncs_session_open.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            library.mncs_session_open.restype = ctypes.c_void_p
            library.mncs_session_call_batch.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            library.mncs_session_call_batch.restype = ctypes.c_void_p
            library.mncs_session_close.argtypes = [ctypes.c_void_p]
            library.mncs_session_close.restype = None
            library.mncs_session_info.argtypes = [ctypes.c_void_p]
            library.mncs_session_info.restype = ctypes.c_void_p
            library.mncs_response_text.argtypes = [ctypes.c_void_p]
            library.mncs_response_text.restype = ctypes.c_char_p
            library.mncs_response_free.argtypes = [ctypes.c_void_p]
            library.mncs_response_free.restype = None
            library.mncs_last_error.argtypes = []
            library.mncs_last_error.restype = ctypes.c_char_p
            cls._library = library
            return library

    @classmethod
    def _artifact_cache_root(cls) -> Path:
        configured = os.environ.get("MNCS_STORE_ARTIFACT_CACHE")
        if configured:
            return Path(configured).expanduser().resolve()
        return Path(tempfile.gettempdir()) / "mncs-store-artifact-cache"

    @staticmethod
    def _source_manifest(root: Path) -> list[dict[str, str]]:
        entries: list[dict[str, str]] = []
        if not root.is_dir():
            return entries
        for path in sorted(root.rglob("*.mncs")):
            if not path.is_file():
                continue
            entries.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
        return entries

    @classmethod
    def _artifact_identity_material(
        cls,
        *,
        source: Path,
        compiler: Path,
        language: Path,
        store_root: Path,
        target: str,
        environment: dict[str, str],
    ) -> tuple[str, dict[str, Any]]:
        relevant_environment = {
            name: environment.get(name, "")
            for name in (
                "MNCS_LIBRARY_PATH",
                "MNCS_STDLIB_BUNDLE",
                "MNCS_PROFILE",
                "MNCS_COMPILER_PROFILE",
                "MNCS_TARGET_PROFILE",
                "MNCS_BACKEND_CONFIG",
            )
        }
        bundle = environment.get("MNCS_STDLIB_BUNDLE", "")
        bundle_path = Path(bundle).expanduser().resolve() if bundle else None
        material: dict[str, Any] = {
            "schema": "mncs-store-artifact-cache/1",
            "source": {
                "path": source.relative_to(store_root).as_posix(),
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            },
            "source_roots": {
                "store": cls._source_manifest(store_root / "src"),
                "language": cls._source_manifest(language / "library"),
            },
            "compiler": {
                "path": str(compiler),
                "sha256": hashlib.sha256(compiler.read_bytes()).hexdigest(),
            },
            "target": target,
            "profile": relevant_environment,
            "bundle": {
                "path": str(bundle_path) if bundle_path else "",
                "sha256": (
                    hashlib.sha256(bundle_path.read_bytes()).hexdigest()
                    if bundle_path and bundle_path.is_file()
                    else ""
                ),
            },
        }
        encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest(), material

    @classmethod
    def _read_cached_artifact(cls, key: str) -> bytes | None:
        entry = cls._artifact_cache_root() / key
        manifest_path = entry / "manifest.json"
        artifact_path = entry / "backend.json"
        try:
            if manifest_path.stat().st_size > STORE_ARTIFACT_MANIFEST_MAX_BYTES:
                return None
            if artifact_path.stat().st_size > STORE_ARTIFACT_MAX_BYTES:
                return None
            manifest = json.loads(manifest_path.read_text())
            artifact = artifact_path.read_bytes()
        except (OSError, ValueError, TypeError):
            return None
        if manifest.get("schema") != "mncs-store-artifact-cache/1":
            return None
        if manifest.get("key") != key:
            return None
        if manifest.get("artifact_sha256") != hashlib.sha256(artifact).hexdigest():
            return None
        return artifact

    @classmethod
    def _write_cached_artifact(
        cls,
        key: str,
        material: dict[str, Any],
        artifact: bytes,
    ) -> None:
        root = cls._artifact_cache_root()
        entry = root / key
        try:
            root.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f"{key}-", dir=root))
            (temporary / "backend.json").write_bytes(artifact)
            (temporary / "manifest.json").write_text(
                json.dumps(
                    {
                        "schema": "mncs-store-artifact-cache/1",
                        "key": key,
                        "material": material,
                        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            if entry.exists():
                for child in (temporary / "backend.json", temporary / "manifest.json"):
                    os.replace(child, entry / child.name)
                temporary.rmdir()
            else:
                os.replace(temporary, entry)
        except OSError:
            # Caching is an acceleration only. A read-only or unavailable
            # cache must never change Store admission semantics.
            return

    def _compile_artifact(self) -> bytes:
        cls = type(self)
        with cls._artifact_lock:
            configured_artifact = os.environ.get("MNCS_STORE_ARTIFACT")
            if configured_artifact:
                artifact_path = Path(configured_artifact).expanduser().resolve()
                if not artifact_path.is_file():
                    raise StoreError(
                        StoreResultCode.PLATFORM_UNSUPPORTED,
                        f"configured Store artifact is unavailable: {artifact_path}",
                    )
                if artifact_path.stat().st_size > STORE_ARTIFACT_MAX_BYTES:
                    raise StoreError(
                        StoreResultCode.PLATFORM_UNSUPPORTED,
                        f"configured Store artifact exceeds the {STORE_ARTIFACT_MAX_BYTES}-byte resident bound",
                    )
                cls._artifact = artifact_path.read_bytes()
                cls._artifact_key = None
                cls._last_artifact_cache_hit = False
                return cls._artifact
            store_root = _store_root()
            language = _language_root()
            source = store_root / "src" / "store" / "application.mncs"
            compiler = Path(
                os.environ.get("MNCS_BIN", str(_language_target("mncs")))
            )
            if not source.is_file() or not compiler.is_file():
                raise StoreError(
                    StoreResultCode.PLATFORM_UNSUPPORTED,
                    f"Store compiler/source unavailable: {compiler}, {source}",
                )
            environment = dict(os.environ)
            environment["MNCS_LIBRARY_PATH"] = os.pathsep.join(
                [str(language / "library"), str(store_root / "src")]
            )
            target = "mncs-research-bytecode"
            key, material = cls._artifact_identity_material(
                source=source,
                compiler=compiler,
                language=language,
                store_root=store_root,
                target=target,
                environment=environment,
            )
            if cls._artifact is not None and cls._artifact_key == key:
                cls._last_artifact_cache_hit = True
                return cls._artifact
            cached = cls._read_cached_artifact(key)
            if cached is not None:
                cls._artifact = cached
                cls._artifact_key = key
                cls._last_artifact_cache_hit = True
                return cached
            with tempfile.TemporaryDirectory(prefix="mncs-store-artifact-") as output:
                command = [
                    str(compiler),
                    "compile",
                    str(source),
                    "--emit",
                    "backend",
                    "--output-dir",
                    output,
                    "--target",
                    target,
                ]
                if self._command_executor is not None:
                    execute = getattr(self._command_executor, "execute", None)
                    if not callable(execute):
                        raise StoreError(
                            StoreResultCode.PLATFORM_UNSUPPORTED,
                            "Store artifact command executor has no execute method",
                        )
                    completed = execute(
                        command,
                        cwd=store_root,
                        timeout=STORE_ARTIFACT_COMPILE_TIMEOUT_SECONDS,
                        output_cap=STORE_ARTIFACT_OUTPUT_BYTES,
                        stderr_cap=STORE_ARTIFACT_OUTPUT_BYTES,
                        environment=environment,
                    )
                else:
                    # Standalone Store clients retain the local path, but use
                    # a bounded wall timeout. Continuous Forge injects its
                    # Runner to enforce aggregate cgroup and output policy.
                    completed = subprocess.run(
                        command,
                        capture_output=True,
                        text=False,
                        timeout=STORE_ARTIFACT_COMPILE_TIMEOUT_SECONDS,
                        env=environment,
                        check=False,
                    )
                artifact = Path(output) / "backend.json"
                if completed.returncode != 0 or not artifact.is_file():
                    diagnostic = completed.stderr or completed.stdout
                    detail = (
                        diagnostic[-4000:].decode("utf-8", errors="replace")
                        if isinstance(diagnostic, bytes)
                        else diagnostic[-4000:]
                    )
                    raise StoreError(
                        StoreResultCode.PLATFORM_UNSUPPORTED,
                        f"Store artifact admission failed: {detail}",
                    )
                if artifact.stat().st_size > STORE_ARTIFACT_MAX_BYTES:
                    raise StoreError(
                        StoreResultCode.PLATFORM_UNSUPPORTED,
                        f"compiled Store artifact exceeds the {STORE_ARTIFACT_MAX_BYTES}-byte resident bound",
                    )
                cls._artifact = artifact.read_bytes()
                cls._artifact_key = key
                cls._last_artifact_cache_hit = False
                cls._write_cached_artifact(key, material, cls._artifact)
                return cls._artifact

    def __init__(self, *, command_executor: object | None = None) -> None:
        started = time.perf_counter()
        self._command_executor = command_executor
        library_started = time.perf_counter()
        self._library = self._load_library()
        self.library_load_seconds = time.perf_counter() - library_started
        artifact_started = time.perf_counter()
        artifact = self._compile_artifact()
        self.artifact_prepare_seconds = time.perf_counter() - artifact_started
        self.artifact_cache_hit = self._last_artifact_cache_hit
        self.artifact_sha256 = hashlib.sha256(artifact).hexdigest()
        self.toolchain = os.environ.get("MNCS_BIN", str(_language_target("mncs")))
        self.call_count = 0
        self.semantic_seconds = 0.0
        open_started = time.perf_counter()
        # This is the artifact admission boundary for both freshly compiled
        # and cross-process cached bytes. No cached artifact reaches a call
        # until the embed library has accepted its identity and payload here.
        self._handle = self._library.mncs_session_open(artifact, len(artifact))
        self.session_open_seconds = time.perf_counter() - open_started
        if not self._handle:
            raw = self._library.mncs_last_error()
            detail = raw.decode() if raw else "unknown session-open failure"
            raise StoreError(StoreResultCode.PLATFORM_UNSUPPORTED, detail)
        self.backend = "unknown"
        self.reused_session = False
        info = self._library.mncs_session_info(self._handle)
        if info:
            try:
                raw_info = self._library.mncs_response_text(info)
                details = json.loads(raw_info.decode())
                self.backend = str(details.get("backend", self.backend))
                self.reused_session = bool(details.get("reused_session", False))
            finally:
                self._library.mncs_response_free(info)
        self.construction_seconds = time.perf_counter() - started
        self._closed = False

    def call(
        self,
        module: str,
        function: str,
        arguments: list[dict[str, Any]],
        *,
        step_budget: int = 600_000,
        grants: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store session is closed")
        started = time.perf_counter()
        self.call_count += 1
        request = [
            {
                "module": module,
                "function": function,
                "args": arguments,
                "grants": grants or [],
                "step_budget": step_budget,
            }
        ]
        profile = os.environ.get("MNCS_RUNTIME_PROFILE") is not None
        encode_started = time.perf_counter()
        request_bytes = json.dumps(request, separators=(",", ":")).encode()
        if profile:
            print(
                "mncs-store-profile phase=host_argument_encode elapsed_ns="
                f"{int((time.perf_counter() - encode_started) * 1_000_000_000)}",
                file=sys.stderr,
                flush=True,
            )
        abi_started = time.perf_counter()
        response = self._library.mncs_session_call_batch(self._handle, request_bytes)
        if profile:
            print(
                "mncs-store-profile phase=c_abi_call elapsed_ns="
                f"{int((time.perf_counter() - abi_started) * 1_000_000_000)}",
                file=sys.stderr,
                flush=True,
            )
        self.semantic_seconds += time.perf_counter() - started
        if not response:
            raw = self._library.mncs_last_error()
            detail = raw.decode() if raw else "unknown Store call failure"
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, detail)
        try:
            decode_started = time.perf_counter()
            text = self._library.mncs_response_text(response)
            values = json.loads(text.decode())
            if profile:
                print(
                    "mncs-store-profile phase=host_result_decode elapsed_ns="
                    f"{int((time.perf_counter() - decode_started) * 1_000_000_000)}",
                    file=sys.stderr,
                    flush=True,
                )
        finally:
            self._library.mncs_response_free(response)
        if not isinstance(values, list) or len(values) != 1:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "invalid retained Store response")
        return values[0]

    def sha256(self, payload: bytes) -> bytes:
        """Hash arbitrary bytes through ``mncs.std.sha256.v1`` via Store."""

        payload = bytes(payload)
        # Transporting each 64-byte SHA view through the retained ABI is
        # useful for small values and proves the direct typed path. Larger
        # values use the same MNCS DigestState through bounded host transport
        # windows: the host stages/reads only platform bytes, while MNCS owns
        # every update and the final digest decision. No source value grows
        # with the object.
        if len(payload) > 1024:
            with tempfile.TemporaryDirectory(prefix="mncs-store-hash-") as directory:
                path = Path(directory) / "payload.bin"
                with path.open("wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                return self.sha256_file(path)

        constants = (
            1116352408, 1899447441, 3049323471, 3921009573, 961987163, 1508970993,
            2453635748, 2870763221, 3624381080, 310598401, 607225278, 1426881987,
            1925078388, 2162078206, 2614888103, 3248222580, 3835390401, 4022224774,
            264347078, 604807628, 770255983, 1249150122, 1555081692, 1996064986,
            2554220882, 2821834349, 2952996808, 3210313671, 3336571891, 3584528711,
            113926993, 338241895, 666307205, 773529912, 1294757372, 1396182291,
            1695183700, 1986661051, 2177026350, 2456956037, 2730485921, 2820302411,
            3259730800, 3345764771, 3516065817, 3600352804, 4094571909, 275423344,
            430227734, 506948616, 659060556, 883997877, 958139571, 1322822218,
            1537002063, 1747873779, 1955562222, 2024104815, 2227730452, 2361852424,
            2428436474, 2756734187, 3204031479, 3329325298,
        )
        state_value = _returned(self.call("store.content.v1", "digest_init", []))
        state = _record_fields(state_value)
        for start in range(0, len(payload), 1024):
            window = payload[start : start + 1024]
            updated = _returned(
                self.call(
                    "store.content.v1",
                    "digest_update_window_fields",
                    [
                        state["h"],
                        state["total"],
                        state["buf"],
                        state["buffered"],
                        bytes_value(window),
                        u64(len(window)),
                        u32_values(constants),
                    ],
                )
            )
            state = _record_fields(updated)
        final = _returned(
            self.call(
                "store.content.v1",
                "digest_finalize_fields",
                [state["h"], state["total"], state["buf"], state["buffered"], u32_values(constants)],
            )
        )
        return as_bytes(final)

    def sha256_file(self, path: Path) -> bytes:
        """Hash a host file through bounded native SHA windows.

        File reads are platform transport.  Every byte still crosses the
        current native ``DigestState`` through ``digest_update_window_fields``
        and the final digest is produced by ``mncs.std.sha256.v1``.  Keeping
        the filesystem loop at the host boundary avoids asking the research
        bytecode interpreter to realize hundreds of filesystem effects in one
        call while preserving one streaming hash state for the whole file.
        """

        path = Path(path).expanduser().resolve()
        if not path.is_file():
            raise StoreError(StoreResultCode.DENIED, f"hash input is not a file: {path}")
        length = path.stat().st_size
        constants = (
            1116352408, 1899447441, 3049323471, 3921009573, 961987163, 1508970993,
            2453635748, 2870763221, 3624381080, 310598401, 607225278, 1426881987,
            1925078388, 2162078206, 2614888103, 3248222580, 3835390401, 4022224774,
            264347078, 604807628, 770255983, 1249150122, 1555081692, 1996064986,
            2554220882, 2821834349, 2952996808, 3210313671, 3336571891, 3584528711,
            113926993, 338241895, 666307205, 773529912, 1294757372, 1396182291,
            1695183700, 1986661051, 2177026350, 2456956037, 2730485921, 2820302411,
            3259730800, 3345764771, 3516065817, 3600352804, 4094571909, 275423344,
            430227734, 506948616, 659060556, 883997877, 958139571, 1322822218,
            1537002063, 1747873779, 1955562222, 2024104815, 2227730452, 2361852424,
            2428436474, 2756734187, 3204031479, 3329325298,
        )
        state = _record_fields(_returned(self.call("store.content.v1", "digest_init", [])))
        with path.open("rb") as stream:
            for start in range(0, length, 1024):
                expected = min(1024, length - start)
                window = stream.read(expected)
                if len(window) != expected:
                    raise StoreError(StoreResultCode.INTEGRITY_FAILURE, f"hash input truncated: {path}")
                updated = _returned(
                    self.call(
                        "store.content.v1",
                        "digest_update_window_fields",
                        [
                            state["h"],
                            state["total"],
                            state["buf"],
                            state["buffered"],
                            bytes_value(window),
                            u64(expected),
                            u32_values(constants),
                        ],
                    )
                )
                state = _record_fields(updated)
        final = _returned(
            self.call(
                "store.content.v1",
                "digest_finalize_fields",
                [state["h"], state["total"], state["buf"], state["buffered"], u32_values(constants)],
            )
        )
        return as_bytes(final)

    def encode_relation(
        self,
        relation_type: bytes,
        source: bytes,
        target: bytes,
        generation: int,
        provenance: bytes,
        ordinal: int,
        metadata_type: bytes | None = None,
        metadata_root: bytes | None = None,
    ) -> bytes:
        """Encode one current generic Store relation through MNCS."""

        relation_type = bytes(relation_type)
        source = bytes(source)
        target = bytes(target)
        provenance = bytes(provenance)
        metadata_type = bytes(metadata_type or bytes(32))
        metadata_root = bytes(metadata_root or bytes(32))
        if len(relation_type) != 32 or len(source) != 12 or len(target) != 12:
            raise StoreError(StoreResultCode.DENIED, "Store relation identity width is invalid")
        if len(provenance) != 32 or len(metadata_type) != 32 or len(metadata_root) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store relation metadata width is invalid")
        output = self.call(
            "store.relationship",
            "encode_fields",
            [
                bytes_value(relation_type),
                bytes_value(source),
                bytes_value(target),
                u64(generation),
                bytes_value(provenance),
                u64(ordinal),
                bytes_value(metadata_type),
                bytes_value(metadata_root),
            ],
        )
        return as_bytes(_returned(output))

    def validate_relation(self, raw: bytes, *, expected_generation: int | None = None) -> int:
        """Fail closed on malformed or future generic relation bytes."""

        raw = bytes(raw)
        if len(raw) != 172:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store relation width is invalid")
        output = self.call(
            "store.relationship",
            "validate",
            [bytes_value(raw)],
        )
        if as_int(_returned(output)) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store relation structural validation failed")
        output = self.call(
            "store.relationship",
            "validate_exact",
            [bytes_value(raw)],
        )
        if as_int(_returned(output)) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store relation exact validation failed")
        output = self.call(
            "store.relationship",
            "generation",
            [bytes_value(raw)],
        )
        generation = as_int(_returned(output))
        if expected_generation is not None and generation > expected_generation:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store relation points into a future generation")
        return generation

    def encode_provenance(
        self,
        source: bytes,
        producer: bytes,
        transformation: bytes,
        generation: int,
        evidence: bytes,
        ancestry: bytes,
    ) -> bytes:
        """Encode one current typed Store provenance record through MNCS."""

        values = [bytes(source), bytes(producer), bytes(transformation), bytes(evidence), bytes(ancestry)]
        if len(values[0]) != 12 or len(values[1]) != 12 or len(values[2]) != 12:
            raise StoreError(StoreResultCode.DENIED, "Store provenance object identity width is invalid")
        if len(values[3]) != 32 or len(values[4]) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store provenance digest width is invalid")
        output = self.call(
            "store.provenance.v1",
            "encode_fields",
            [
                bytes_value(values[0]),
                bytes_value(values[1]),
                bytes_value(values[2]),
                u64(generation),
                bytes_value(values[3]),
                bytes_value(values[4]),
            ],
        )
        return as_bytes(_returned(output))

    def validate_provenance(self, raw: bytes, *, expected_generation: int | None = None) -> int:
        """Fail closed on malformed or future typed provenance bytes."""

        raw = bytes(raw)
        if len(raw) != 112:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store provenance width is invalid")
        output = self.call(
            "store.provenance.v1",
            "validate",
            [bytes_value(raw)],
        )
        if as_int(_returned(output)) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store provenance validation failed")
        output = self.call(
            "store.provenance.v1",
            "generation",
            [bytes_value(raw)],
        )
        generation = as_int(_returned(output))
        if expected_generation is not None and generation > expected_generation:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store provenance points into a future generation")
        return generation

    def encode_commit_feed(
        self,
        generation: int,
        objects: int,
        relations: int,
        provenance: int,
        root: bytes,
    ) -> bytes:
        """Encode and validate the deterministic Store-to-Index feed."""

        root = bytes(root)
        if len(root) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store feed root width is invalid")
        output = self.call(
            "store.commit_feed.v1",
            "encode_fields",
            [u64(generation), u64(objects), u64(relations), u64(provenance), bytes_value(root)],
        )
        raw = as_bytes(_returned(output))
        output = self.call("store.commit_feed.v1", "validate", [bytes_value(raw)])
        if as_int(_returned(output)) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store commit feed validation failed")
        return raw

    def recovery_decide(self, previous_valid: bool, candidate_class: int) -> int:
        """Ask native Store whether a durable candidate stays or promotes."""

        output = self.call(
            "store.recovery.v1",
            "recover_decide",
            [
                {"boolean": {"value": bool(previous_valid)}},
                u64(candidate_class),
            ],
            step_budget=32_768,
        )
        return as_int(_returned(output))

    def close(self) -> None:
        if self._closed:
            return
        self._library.mncs_session_close(self._handle)
        self._handle = None
        self._closed = True

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> "StoreSession":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()
