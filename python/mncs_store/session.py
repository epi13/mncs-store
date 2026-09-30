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
STORE_CALL_BATCH_MAX = 4096


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
        cache_home = os.environ.get("XDG_CACHE_HOME")
        base = Path(cache_home).expanduser() if cache_home else Path.home() / ".cache"
        return base / "mncs-store" / "artifacts"

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
        self.batch_count = 0
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
        return self.call_batch([{
            "module": module,
            "function": function,
            "args": arguments,
            "step_budget": step_budget,
            "grants": grants or [],
        }])[0]

    def call_batch(self, calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Invoke several typed Store calls through one retained ABI crossing.

        Each request is validated before serialization. Results preserve
        request order and the whole batch is one call against this retained
        artifact/session; callers must not use this for dependent requests.
        """
        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store session is closed")
        if not isinstance(calls, list) or not calls:
            raise StoreError(StoreResultCode.DENIED, "Store call batch must be a non-empty list")
        if len(calls) > STORE_CALL_BATCH_MAX:
            raise StoreError(
                StoreResultCode.DENIED,
                f"Store call batch exceeds {STORE_CALL_BATCH_MAX} requests",
            )
        request: list[dict[str, Any]] = []
        for index, call in enumerate(calls):
            if not isinstance(call, dict):
                raise StoreError(StoreResultCode.DENIED, f"Store batch request {index} must be an object")
            module = call.get("module")
            function = call.get("function")
            arguments = call.get("args", [])
            grants = call.get("grants", [])
            step_budget = call.get("step_budget", 600_000)
            if not isinstance(module, str) or not module or not isinstance(function, str) or not function:
                raise StoreError(StoreResultCode.DENIED, f"Store batch request {index} needs module and function")
            if not isinstance(arguments, list) or not isinstance(grants, list):
                raise StoreError(StoreResultCode.DENIED, f"Store batch request {index} has invalid args or grants")
            if not isinstance(step_budget, int) or step_budget < 1:
                raise StoreError(StoreResultCode.DENIED, f"Store batch request {index} has invalid step_budget")
            request.append({
                "module": module,
                "function": function,
                "args": arguments,
                "grants": grants,
                "step_budget": step_budget,
            })

        started = time.perf_counter()
        self.call_count += len(request)
        self.batch_count += 1
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
        if not isinstance(values, list) or len(values) != len(request):
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "invalid retained Store batch response")
        if not all(isinstance(value, dict) for value in values):
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "retained Store batch returned a non-object")
        return values

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

    @staticmethod
    def sha256_buffer(payload: bytes) -> bytes:
        """Hash already resident bytes with the canonical SHA-256 primitive.

        EmbeddedStore uses this for read-side integrity checks after loading a
        node, descriptor, chunk, or object payload. New identities and staged
        content continue through :meth:`sha256`; this avoids routing thousands
        of tiny verification reads through the MNCS interpreter one at a time.
        """

        return hashlib.sha256(bytes(payload)).digest()

    def sha256_file(self, path: Path) -> bytes:
        """Hash a host file with the platform's optimized SHA-256 primitive.

        SHA-256 is the Store's canonical content identity algorithm. The
        standard-library implementation computes the same digest as
        ``mncs.std.sha256.v1`` while keeping bulk filesystem verification out
        of the research-bytecode interpreter. Small semantic calls continue
        to use the MNCS digest module directly.
        """

        path = Path(path).expanduser().resolve()
        if not path.is_file():
            raise StoreError(StoreResultCode.DENIED, f"hash input is not a file: {path}")
        try:
            with path.open("rb") as stream:
                digest = hashlib.sha256()
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                return digest.digest()
        except OSError as exc:
            raise StoreError(
                StoreResultCode.INTEGRITY_FAILURE,
                f"cannot read Store hash input: {path}",
            ) from exc

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
        outputs = self.call_batch([
            {"module": "store.relationship", "function": "validate", "args": [bytes_value(raw)]},
            {"module": "store.relationship", "function": "validate_exact", "args": [bytes_value(raw)]},
            {"module": "store.relationship", "function": "generation", "args": [bytes_value(raw)]},
        ])
        if as_int(_returned(outputs[0])) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store relation structural validation failed")
        if as_int(_returned(outputs[1])) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store relation exact validation failed")
        generation = as_int(_returned(outputs[2]))
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
        outputs = self.call_batch([
            {"module": "store.provenance.v1", "function": "validate", "args": [bytes_value(raw)]},
            {"module": "store.provenance.v1", "function": "generation", "args": [bytes_value(raw)]},
        ])
        if as_int(_returned(outputs[0])) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store provenance validation failed")
        generation = as_int(_returned(outputs[1]))
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

    # ---- adaptive representations: MNCS-decided, host-transported ----
    #
    # Every function below transports values across the retained ABI and
    # parses results. Record bytes, validation verdicts, cost estimates,
    # selection ranks, closure masks, and codec windows are all computed by
    # the Store MNCS modules; the host only moves bytes, folds total
    # pairwise verdicts, and iterates monotone fixpoints to MNCS-verified
    # closure.

    def encode_intent(
        self,
        fidelity: int,
        latency: int = 0,
        compute: int = 0,
        memory: int = 0,
        transfer: int = 0,
        frequency: int = 0,
        lifetime: int = 0,
        locality: int = 0,
    ) -> bytes:
        """Encode one generic access intent through MNCS."""

        output = self.call(
            "store.intent.v1",
            "encode_fields",
            [
                u64(fidelity),
                u64(latency),
                u64(compute),
                u64(memory),
                u64(transfer),
                u64(frequency),
                u64(lifetime),
                u64(locality),
            ],
        )
        return as_bytes(_returned(output))

    def default_intent(self) -> bytes:
        """Return the MNCS-owned default intent (exact, unconstrained)."""

        output = self.call("store.intent.v1", "default_intent", [])
        return as_bytes(_returned(output))

    def normalize_intent(self, raw: bytes) -> bytes:
        """Normalize advisory intent codes onto their known domains."""

        raw = bytes(raw)
        if len(raw) != 64:
            raise StoreError(StoreResultCode.DENIED, "Store intent width is invalid")
        output = self.call("store.intent.v1", "normalize", [bytes_value(raw)])
        return as_bytes(_returned(output))

    def validate_intent(self, raw: bytes) -> None:
        """Fail closed on structurally malformed intent bytes."""

        raw = bytes(raw)
        if len(raw) != 64:
            raise StoreError(StoreResultCode.DENIED, "Store intent width is invalid")
        output = self.call("store.intent.v1", "validate", [bytes_value(raw)])
        if as_int(_returned(output)) != 0:
            raise StoreError(StoreResultCode.DENIED, "Store intent structural validation failed")

    def intent_fidelity(self, raw: bytes) -> int:
        output = self.call("store.intent.v1", "fidelity", [bytes_value(bytes(raw))])
        return as_int(_returned(output))

    def encode_envelope(
        self,
        *,
        logical: bytes,
        type_identity: bytes,
        synopsis: bytes,
        synopsis_bytes: int,
        rep_count: int,
        block_count: int,
        stored_bytes: int,
        plain_bytes: int,
        default_root: bytes,
        provenance: bytes,
        block_table: bytes,
        generation: int,
        fidelity_bits: int,
        flags: int,
    ) -> bytes:
        """Encode one semantic envelope through MNCS."""

        fields = [bytes(logical), bytes(type_identity), bytes(synopsis)]
        digests = [bytes(default_root), bytes(provenance), bytes(block_table)]
        if len(fields[0]) != 12 or len(fields[1]) != 32 or len(fields[2]) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store envelope identity width is invalid")
        if any(len(item) != 32 for item in digests):
            raise StoreError(StoreResultCode.DENIED, "Store envelope digest width is invalid")
        output = self.call(
            "store.envelope.v1",
            "encode_fields",
            [
                bytes_value(fields[0]),
                bytes_value(fields[1]),
                bytes_value(fields[2]),
                u64(synopsis_bytes),
                u64(rep_count),
                u64(block_count),
                u64(stored_bytes),
                u64(plain_bytes),
                bytes_value(digests[0]),
                bytes_value(digests[1]),
                bytes_value(digests[2]),
                u64(generation),
                u64(fidelity_bits),
                u64(flags),
            ],
        )
        return as_bytes(_returned(output))

    def validate_envelope(self, raw: bytes) -> None:
        """Fail closed on malformed or future envelope bytes."""

        raw = bytes(raw)
        if len(raw) != 256:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store envelope width is invalid")
        outputs = self.call_batch([
            {"module": "store.envelope.v1", "function": "validate", "args": [bytes_value(raw)]},
            {"module": "store.envelope.v1", "function": "validate_exact", "args": [bytes_value(raw)]},
        ])
        if as_int(_returned(outputs[0])) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store envelope structural validation failed")
        if as_int(_returned(outputs[1])) != 0:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store envelope exact validation failed")

    def default_envelope_for(
        self,
        logical: bytes,
        content: bytes,
        total: int,
        generation: int,
    ) -> bytes:
        """Synthesize the deterministic legacy envelope through MNCS."""

        logical = bytes(logical)
        content = bytes(content)
        if len(logical) != 12 or len(content) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store envelope synthesis width is invalid")
        output = self.call(
            "store.envelope.v1",
            "envelope_default_for",
            [bytes_value(logical), bytes_value(content), u64(total), u64(generation)],
        )
        return as_bytes(_returned(output))

    def envelope_consistent(
        self,
        envelope: bytes,
        logical: bytes,
        default_root: bytes,
        total: int,
        generation: int,
    ) -> bool:
        output = self.call(
            "store.envelope.v1",
            "envelope_consistent",
            [
                bytes_value(bytes(envelope)),
                bytes_value(bytes(logical)),
                bytes_value(bytes(default_root)),
                u64(total),
                u64(generation),
            ],
        )
        return bool(_returned(output)["boolean"]["value"])

    def envelope_offers(self, envelope: bytes, level: int) -> bool:
        output = self.call(
            "store.envelope.v1", "offers", [bytes_value(bytes(envelope)), u64(level)]
        )
        return bool(_returned(output)["boolean"]["value"])

    def fidelity_bitmap_with(self, bits: int, level: int) -> int:
        """Set one fidelity bit through the MNCS bitmap fold helper."""

        output = self.call("store.envelope.v1", "bitmap_with", [u64(bits), u64(level)])
        return as_int(_returned(output))

    def envelope_fields(self, envelope: bytes) -> dict[str, int | bytes]:
        """Project the scalar envelope fields through one batched MNCS call."""

        envelope = bytes(envelope)
        if len(envelope) != 256:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store envelope width is invalid")
        names = [
            "rep_count", "block_count", "stored_bytes", "plain_bytes",
            "synopsis_bytes", "generation", "fidelity_bits", "flags",
        ]
        outputs = self.call_batch([
            {"module": "store.envelope.v1", "function": name, "args": [bytes_value(envelope)]}
            for name in names
        ] + [
            {"module": "store.envelope.v1", "function": name, "args": [bytes_value(envelope)]}
            for name in ("logical", "type_identity", "synopsis", "default_root", "provenance", "block_table")
        ])
        fields: dict[str, int | bytes] = {
            name: as_int(_returned(output)) for name, output in zip(names, outputs)
        }
        for name, output in zip(
            ("logical", "type_identity", "synopsis", "default_root", "provenance", "block_table"),
            outputs[len(names):],
        ):
            fields[name] = as_bytes(_returned(output))
        return fields

    def encode_representation(
        self,
        *,
        fidelity: int,
        codec: int,
        codec_identity: bytes,
        stored: int,
        plain: int,
        decode_class: int,
        first_block: int,
        block_count: int,
        root: bytes,
        flags: int,
    ) -> bytes:
        """Encode one representation descriptor through MNCS."""

        codec_identity = bytes(codec_identity)
        root = bytes(root)
        if len(codec_identity) != 32 or len(root) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store representation identity width is invalid")
        output = self.call(
            "store.representation.v1",
            "encode_fields",
            [
                u64(fidelity),
                u64(codec),
                bytes_value(codec_identity),
                u64(stored),
                u64(plain),
                u64(decode_class),
                u64(first_block),
                u64(block_count),
                bytes_value(root),
                u64(flags),
            ],
        )
        return as_bytes(_returned(output))

    def validate_representation(self, raw: bytes) -> None:
        """Fail closed on malformed or future representation bytes."""

        raw = bytes(raw)
        if len(raw) != 128:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store representation width is invalid")
        outputs = self.call_batch([
            {"module": "store.representation.v1", "function": "validate", "args": [bytes_value(raw)]},
            {"module": "store.representation.v1", "function": "validate_exact", "args": [bytes_value(raw)]},
        ])
        if as_int(_returned(outputs[0])) != 0:
            raise StoreError(
                StoreResultCode.INTEGRITY_FAILURE, "Store representation structural validation failed"
            )
        if as_int(_returned(outputs[1])) != 0:
            raise StoreError(
                StoreResultCode.INTEGRITY_FAILURE, "Store representation exact validation failed"
            )

    def representation_fields(self, raw: bytes) -> dict[str, int | bytes]:
        """Project the scalar representation fields through one batched call."""

        raw = bytes(raw)
        if len(raw) != 128:
            raise StoreError(StoreResultCode.INTEGRITY_FAILURE, "Store representation width is invalid")
        names = ["fidelity", "codec", "stored", "plain", "decode_class", "first_block", "block_count", "flags"]
        outputs = self.call_batch([
            {"module": "store.representation.v1", "function": name, "args": [bytes_value(raw)]}
            for name in names
        ] + [
            {"module": "store.representation.v1", "function": name, "args": [bytes_value(raw)]}
            for name in ("codec_identity", "root")
        ])
        fields: dict[str, int | bytes] = {
            name: as_int(_returned(output)) for name, output in zip(names, outputs)
        }
        fields["codec_identity"] = as_bytes(_returned(outputs[len(names)]))
        fields["root"] = as_bytes(_returned(outputs[len(names) + 1]))
        output = self.call("store.representation.v1", "is_exact", [bytes_value(raw)])
        fields["exact"] = int(bool(_returned(output)["boolean"]["value"]))
        return fields

    def representation_satisfies(self, raw: bytes, required: int) -> bool:
        output = self.call(
            "store.representation.v1", "satisfies", [bytes_value(bytes(raw)), u64(required)]
        )
        return bool(_returned(output)["boolean"]["value"])

    def estimate_cost(self, representation: bytes, intent: bytes) -> int:
        output = self.call(
            "store.representation.v1",
            "estimate_cost",
            [bytes_value(bytes(representation)), bytes_value(bytes(intent))],
        )
        return as_int(_returned(output))

    def rank_representations(self, left: bytes, right: bytes, intent: bytes) -> int:
        """Return the MNCS pairwise tournament verdict (0/1/2)."""

        output = self.call(
            "store.representation.v1",
            "rank",
            [bytes_value(bytes(left)), bytes_value(bytes(right)), bytes_value(bytes(intent))],
        )
        return as_int(_returned(output))

    def select_representation(
        self, records: list[bytes], intent: bytes
    ) -> tuple[int, bool, int]:
        """Fold the MNCS tournament over validated records.

        Returns (winning index, satisfies required fidelity, estimated
        cost). Every comparison verdict is MNCS-computed; the host only
        folds the total pairwise order.
        """

        records = [bytes(item) for item in records]
        intent = bytes(intent)
        if not records:
            raise StoreError(StoreResultCode.DENIED, "Store representation table is empty")
        for record in records:
            self.validate_representation(record)
        self.validate_intent(intent)
        winner = 0
        for index in range(1, len(records)):
            verdict = self.rank_representations(records[winner], records[index], intent)
            if verdict == 1:
                winner = index
            elif verdict != 0:
                raise StoreError(
                    StoreResultCode.INTEGRITY_FAILURE,
                    "Store representation tournament reached no verdict",
                )
        required = self.intent_fidelity(intent)
        satisfied = self.representation_satisfies(records[winner], required)
        return winner, satisfied, self.estimate_cost(records[winner], intent)

    def encode_block(
        self,
        *,
        index: int,
        digest: bytes,
        stored: int,
        plain: int,
        tag: int,
        start: int,
        length: int,
        depcount: int = 0,
        deps: tuple[int, int, int, int] = (0, 0, 0, 0),
        flags: int = 0,
    ) -> bytes:
        """Encode one block descriptor through MNCS."""

        digest = bytes(digest)
        if len(digest) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store block digest width is invalid")
        if len(deps) != 4:
            raise StoreError(StoreResultCode.DENIED, "Store block dependencies must number four")
        output = self.call(
            "store.block.v1",
            "encode_fields",
            [
                u64(index),
                bytes_value(digest),
                u64(stored),
                u64(plain),
                u64(tag),
                u64(start),
                u64(length),
                u64(depcount),
                u64(deps[0]),
                u64(deps[1]),
                u64(deps[2]),
                u64(deps[3]),
                u64(flags),
            ],
        )
        return as_bytes(_returned(output))

    def validate_block_table(self, table: bytes, count: int) -> None:
        """Fail closed on a malformed block table."""

        table = bytes(table)
        output = self.call(
            "store.block.v1", "table_validate", [bytes_value(table), u64(count)]
        )
        code = as_int(_returned(output))
        if code != 0:
            raise StoreError(
                StoreResultCode.INTEGRITY_FAILURE,
                f"Store block table validation failed with code {code}",
            )

    def block_spans_within(self, table: bytes, count: int, total: int) -> bool:
        output = self.call(
            "store.block.v1",
            "spans_within",
            [bytes_value(bytes(table)), u64(count), u64(total)],
        )
        return bool(_returned(output)["boolean"]["value"])

    def closure_step(self, table: bytes, count: int, mask: int) -> int:
        output = self.call(
            "store.block.v1",
            "closure_step",
            [bytes_value(bytes(table)), u64(count), u64(mask)],
        )
        return as_int(_returned(output))

    def closure_closed(self, table: bytes, count: int, mask: int) -> bool:
        output = self.call(
            "store.block.v1",
            "closure_closed",
            [bytes_value(bytes(table)), u64(count), u64(mask)],
        )
        return bool(_returned(output)["boolean"]["value"])

    def closure_fixpoint(self, tables: list[tuple[bytes, int]], mask: int) -> int:
        """Iterate MNCS closure rounds across tables to a verified fixpoint.

        Closure is monotone (rounds only add bits over a u64 domain), so at
        most 64 rounds can change the mask; the host then verifies closure
        through MNCS before returning.
        """

        tables = [(bytes(table), int(count)) for table, count in tables]
        grown = int(mask)
        for _ in range(65):
            advanced = grown
            for table, count in tables:
                advanced = self.closure_step(table, count, advanced)
            if advanced == grown:
                break
            grown = advanced
        for table, count in tables:
            if not self.closure_closed(table, count, grown):
                raise StoreError(
                    StoreResultCode.INTEGRITY_FAILURE,
                    "Store block closure did not reach a verified fixpoint",
                )
        return grown

    def blocks_with_tag(self, table: bytes, count: int, tag: int) -> int:
        output = self.call(
            "store.block.v1",
            "blocks_with_tag",
            [bytes_value(bytes(table)), u64(count), u64(tag)],
        )
        return as_int(_returned(output))

    def mask_bytes(self, table: bytes, count: int, mask: int) -> tuple[int, int]:
        """Return MNCS-computed (stored, plain) byte sums for a mask."""

        outputs = self.call_batch([
            {
                "module": "store.block.v1",
                "function": "mask_stored",
                "args": [bytes_value(bytes(table)), u64(count), u64(mask)],
            },
            {
                "module": "store.block.v1",
                "function": "mask_plain",
                "args": [bytes_value(bytes(table)), u64(count), u64(mask)],
            },
        ])
        return as_int(_returned(outputs[0])), as_int(_returned(outputs[1]))

    def mask_count(self, mask: int) -> int:
        output = self.call("store.block.v1", "mask_count", [u64(mask)])
        return as_int(_returned(output))

    def range_mask(self, first: int, count: int) -> int:
        """Return the MNCS mask for a contiguous block range."""

        output = self.call("store.block.v1", "range_mask", [u64(first), u64(count)])
        return as_int(_returned(output))

    def mask_within(self, mask: int, first: int, count: int) -> bool:
        """Ask MNCS whether a mask lies within a representation range."""

        output = self.call(
            "store.block.v1", "mask_within", [u64(mask), u64(first), u64(count)]
        )
        return bool(_returned(output)["boolean"]["value"])

    def plan_fields(self, plan: bytes) -> dict[str, int | bytes]:
        """Project a materialization plan's scalar fields through MNCS."""

        plan = bytes(plan)
        if len(plan) != 128:
            raise StoreError(StoreResultCode.DENIED, "Store plan width is invalid")
        outputs = self.call_batch([
            {"module": "store.plan.v1", "function": name, "args": [bytes_value(plan)]}
            for name in ("fidelity", "mask", "cap")
        ] + [
            {"module": "store.plan.v1", "function": name, "args": [bytes_value(plan)]}
            for name in ("root", "authority")
        ])
        return {
            "fidelity": as_int(_returned(outputs[0])),
            "mask": as_int(_returned(outputs[1])),
            "cap": as_int(_returned(outputs[2])),
            "root": as_bytes(_returned(outputs[3])),
            "authority": as_bytes(_returned(outputs[4])),
        }

    def slot_span(self, table: bytes, count: int, slot: int) -> dict[str, int | bytes]:
        """Project one table slot's span fields through one batched call."""

        table = bytes(table)
        outputs = self.call_batch([
            {"module": "store.block.v1", "function": name, "args": [bytes_value(table), u64(count), u64(slot)]}
            for name in ("slot_index", "slot_tag", "slot_start", "slot_length", "slot_stored", "slot_plain")
        ] + [
            {"module": "store.block.v1", "function": "slot_digest",
             "args": [bytes_value(table), u64(count), u64(slot)]}
        ])
        names = ("index", "tag", "start", "length", "stored", "plain")
        fields: dict[str, int | bytes] = {
            name: as_int(_returned(output)) for name, output in zip(names, outputs)
        }
        fields["digest"] = as_bytes(_returned(outputs[len(names)]))
        return fields

    def encode_plan(
        self,
        *,
        fidelity: int,
        root: bytes,
        mask: int,
        cap: int,
        authority: bytes,
    ) -> bytes:
        """Encode one materialization plan through MNCS."""

        root = bytes(root)
        authority = bytes(authority)
        if len(root) != 32 or len(authority) != 32:
            raise StoreError(StoreResultCode.DENIED, "Store plan identity width is invalid")
        output = self.call(
            "store.plan.v1",
            "encode_fields",
            [u64(fidelity), bytes_value(root), u64(mask), u64(cap), bytes_value(authority)],
        )
        return as_bytes(_returned(output))

    def plan_validate(self, plan: bytes, envelope: bytes, representation: bytes) -> int:
        """Return the MNCS plan-admission code (0 admits)."""

        output = self.call(
            "store.plan.v1",
            "plan_validate",
            [bytes_value(bytes(plan)), bytes_value(bytes(envelope)), bytes_value(bytes(representation))],
        )
        return as_int(_returned(output))

    def plan_blocks_validate(self, plan: bytes, table: bytes, count: int) -> int:
        """Return the MNCS plan block-admission code (0 admits)."""

        output = self.call(
            "store.plan.v1",
            "plan_blocks_validate",
            [bytes_value(bytes(plan)), bytes_value(bytes(table)), u64(count)],
        )
        return as_int(_returned(output))

    def codec_identity_for(self, code: int) -> bytes:
        output = self.call("store.codec.v1", "codec_identity_for", [u64(code)])
        return as_bytes(_returned(output))

    def codec_is_supported(self, code: int) -> bool:
        output = self.call("store.codec.v1", "codec_is_supported", [u64(code)])
        return bool(_returned(output)["boolean"]["value"])

    def rle_encode_windows(self, payload: bytes) -> list[bytes]:
        """Encode each 64-byte window through MNCS; return encoded windows."""

        payload = bytes(payload)
        windows = [payload[start : start + 64] for start in range(0, len(payload), 64)]
        if not windows:
            return []
        # Two requests per window; chunk so no crossing exceeds the batch cap.
        encoded: list[bytes] = []
        for base in range(0, len(windows), STORE_CALL_BATCH_MAX // 2):
            group = windows[base : base + STORE_CALL_BATCH_MAX // 2]
            outputs = self.call_batch([
                {"module": "store.codec.v1", "function": "rle_encode_block", "args": [bytes_value(window)]}
                for window in group
            ] + [
                {"module": "store.codec.v1", "function": "rle_encode_block_len", "args": [bytes_value(window)]}
                for window in group
            ])
            raws = [as_bytes(_returned(output)) for output in outputs[: len(group)]]
            lengths = [as_int(_returned(output)) for output in outputs[len(group):]]
            encoded.extend(raw[:length] for raw, length in zip(raws, lengths))
        return encoded

    def rle_decode_windows(self, windows: list[tuple[bytes, int]]) -> bytes:
        """Decode framed windows through MNCS; fail closed on any bad window."""

        windows = [(bytes(data), int(plain)) for data, plain in windows]
        if not windows:
            return b""
        outputs: list[dict[str, Any]] = []
        for base in range(0, len(windows), STORE_CALL_BATCH_MAX):
            group = windows[base : base + STORE_CALL_BATCH_MAX]
            outputs.extend(self.call_batch([
                {
                    "module": "store.codec.v1",
                    "function": "rle_decode_block",
                    "args": [bytes_value(data), u64(len(data)), u64(plain)],
                }
                for data, plain in group
            ]))
        plain_parts: list[bytes] = []
        for (data, plain), output in zip(windows, outputs):
            raw = as_bytes(_returned(output))
            if raw[0] != 0:
                raise StoreError(
                    StoreResultCode.INTEGRITY_FAILURE,
                    f"Store RLE window decode failed with status {raw[0]}",
                )
            plain_parts.append(raw[1 : 1 + plain])
        return b"".join(plain_parts)

    def canonical_sort(self, table: bytes, width: int, count: int) -> bytes:
        """Return the MNCS canonical byte order for a fixed-record table."""

        output = self.call(
            "store.canonical.v1",
            "canonical_sort",
            [bytes_value(bytes(table)), u64(width), u64(count)],
        )
        return as_bytes(_returned(output))

    def rows_sorted(self, table: bytes, width: int, count: int) -> int:
        output = self.call(
            "store.canonical.v1",
            "rows_sorted",
            [bytes_value(bytes(table)), u64(width), u64(count)],
        )
        return as_int(_returned(output))

    def canonical_equal(self, left: bytes, right: bytes, width: int, count: int) -> bool:
        output = self.call(
            "store.canonical.v1",
            "canonical_equal",
            [bytes_value(bytes(left)), bytes_value(bytes(right)), u64(width), u64(count)],
        )
        return bool(_returned(output)["boolean"]["value"])

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
