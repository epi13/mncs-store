"""Resumable, resource-bounded reachability, reclamation, backup, and restore.

EmbeddedStore retains authority over generation headers, locks, native
recovery/floor verdicts, and digest verification. This module encapsulates
the incremental state machine, compact binary progress storage, consistent
backup manifests, and crash-safe restoration staging so ``embedded.py``
stays clean without creating a second source of truth.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import stat
import struct
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .errors import StoreError, StoreIntegrityError, StoreResultCode


DIR_ENTRY_SCAN_BATCH = 2048
_MARK_FORMAT = "mncs-store-reachability-mark/2"
_BACKUP_FORMAT = "mncs-store-backup/1"

_RETAINED_GEN_STRUCT = struct.Struct(">QQ")
# logical_id(12), binding_id(32), rep_root(32), content_id(32), total_length(Q), ordinal(Q), first_gen(Q)
_ROOT_RECORD_STRUCT = struct.Struct(">12s32s32s32sQQQ")
# node_digest(32), level(B), start(Q), expected_count(Q)
_NODE_RECORD_STRUCT = struct.Struct(">32sBQQ")
# chunk_digest(32), chunk_length(I)
_CHUNK_RECORD_STRUCT = struct.Struct(">32sI")

_STORE_DATA_SUBDIRS = (
    "generations",
    "bindings",
    "descriptors",
    "objects",
    "nodes",
    "chunks",
    "envelopes",
    "representations",
    "blocks",
    "reps",
)


class _ReclamationIncomplete(RuntimeError):
    pass


class _ReclamationBudget:
    """Hard per-invocation I/O, visit, and wall-time ceiling."""

    def __init__(self, root: Path, files: int, size: int, seconds: float) -> None:
        self.root = root
        self.max_files = files
        self.max_bytes = size
        self.seconds = seconds
        self.files = 0
        self.bytes = 0
        self.start = time.monotonic()
        self.chunks: set[str] = set()

    def tick(self, count: int = 1) -> None:
        self.files += count
        if self.files > self.max_files or time.monotonic() - self.start > self.seconds:
            raise _ReclamationIncomplete("reclamation resource budget exhausted")

    def read(self, path: Path) -> bytes:
        self.tick()
        try:
            st = path.lstat()
        except FileNotFoundError as exc:
            raise StoreIntegrityError(f"committed Store artifact is missing: {path.name}") from exc
        size = st.st_size
        if size > self.max_bytes - self.bytes:
            raise _ReclamationIncomplete("reclamation byte budget exhausted")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise StoreIntegrityError("reclamation encountered a non-regular artifact")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                value = stream.read(size + 1)
            if len(value) != size:
                raise _ReclamationIncomplete("artifact changed during analysis")
        finally:
            os.close(fd)
        self.bytes += size
        if path.parent == self.root / "chunks":
            self.chunks.add(path.name)
        return value


def _validate_retained_generation_inventory(
    generations: dict[int, Path],
    head: int,
    floor: int,
    pins: Sequence[int],
) -> None:
    """Validate O(len(generations)) coverage for genesis, pins, and [floor, head]."""
    if head not in generations or 0 not in generations:
        raise StoreIntegrityError("head or genesis generation is missing")
    if floor < 0 or floor > head or floor not in generations:
        raise StoreIntegrityError("retention checkpoint is missing")
    for n in pins:
        if n not in generations:
            raise StoreIntegrityError(f"protected generation is missing: {n}")
    if sum(1 for n in generations if n >= floor) != head - floor + 1:
        raise StoreIntegrityError("retained generation history is incomplete in [floor, head]")


def _sha256_file(path: Path) -> tuple[int, str]:
    hasher = hashlib.sha256()
    total = 0
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise StoreIntegrityError(f"non-regular file encountered: {path}")
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            hasher.update(chunk)
    finally:
        os.close(fd)
    return total, hasher.hexdigest()


def _sha256_prefix(path: Path, expected_bytes: int) -> str:
    hasher = hashlib.sha256()
    remaining = expected_bytes
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise StoreIntegrityError(f"non-regular file encountered: {path}")
        if st.st_size < expected_bytes:
            raise ValueError("mark data file shorter than committed size")
        if st.st_size > expected_bytes:
            os.ftruncate(fd, expected_bytes)
        while remaining > 0:
            chunk = os.read(fd, min(remaining, 1024 * 1024))
            if not chunk:
                raise ValueError("unexpected EOF in mark data file")
            hasher.update(chunk)
            remaining -= len(chunk)
    finally:
        os.close(fd)
    return hasher.hexdigest()


def _sync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_bytes(path: Path, raw: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.stage")
    try:
        fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            view = memoryview(raw)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short write")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _sync_dir(path.parent)


def _sweep_stage_artifacts(directory: Path) -> None:
    if not directory.is_dir():
        return
    for child in directory.iterdir():
        if child.name.startswith(".") and child.name.endswith(".stage"):
            if child.is_file(follow_symlinks=False) or child.is_symlink():
                child.unlink(missing_ok=True)
            elif child.is_dir():
                shutil.rmtree(child, ignore_errors=True)


def _validate_external_dir_target(target: Path, store_path: Path, label: str) -> Path:
    if target.is_symlink():
        raise StoreError(StoreResultCode.DENIED, f"{label} cannot be a symlink")
    abs_target = Path(target).absolute()
    resolved_target = abs_target.resolve()
    resolved_store = store_path.resolve()
    if resolved_target.is_relative_to(resolved_store) or resolved_store.is_relative_to(resolved_target):
        raise StoreError(StoreResultCode.DENIED, f"{label} must live outside the target Store")
    return resolved_target


def _entry_digest(entry: Any) -> bytes:
    packed = struct.pack(
        ">12s32s32s32sQQ",
        entry.logical_id,
        entry.binding_id,
        entry.representation_root,
        entry.content_id,
        entry.total_length,
        entry.ordinal,
    )
    return hashlib.sha256(packed).digest()


def _canonical_json_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _load_mark_state(mark_dir: Path) -> tuple[dict[str, Any] | None, str | None]:
    state_path = mark_dir / "state.json"
    if not state_path.exists():
        return None, None
    if state_path.is_symlink() or not state_path.is_file():
        return None, "mark-state-corrupted"
    try:
        data = json.loads(state_path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None, "mark-state-corrupted"
    if not isinstance(data, dict) or data.get("format") != _MARK_FORMAT:
        return None, "mark-state-incompatible"
    digest = data.get("state_digest")
    body = {k: v for k, v in data.items() if k != "state_digest"}
    if not isinstance(digest, str) or digest != _canonical_json_digest(body):
        return None, "mark-state-corrupted"
    return data, None


def _reset_mark_dir(mark_dir: Path) -> None:
    _sweep_stage_artifacts(mark_dir)
    for child in mark_dir.iterdir():
        if child.name == "mark.lock":
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)


def _save_mark_state(
    mark_dir: Path,
    state: dict[str, Any],
    dirty_files: set[str],
) -> None:
    data_files = state.setdefault("data_files", {})
    for name in sorted(dirty_files):
        fpath = mark_dir / name
        if fpath.exists():
            size, digest = _sha256_file(fpath)
            data_files[name] = {"bytes": size, "sha256": digest}
        else:
            data_files.pop(name, None)
    body = {k: v for k, v in state.items() if k != "state_digest"}
    body["state_digest"] = _canonical_json_digest(body)
    _atomic_write_bytes(
        mark_dir / "state.json",
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
    )
    dirty_files.clear()


def _append_binary_records(path: Path, data: bytes) -> None:
    if not data:
        return
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short mark append")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _dir_mtime_ns(path: Path) -> int:
    return path.stat(follow_symlinks=False).st_mtime_ns


def _list_dir_sorted(path: Path, budget: _ReclamationBudget) -> list[str]:
    names: list[str] = []
    with os.scandir(path) as it:
        for idx, de in enumerate(it):
            if idx % DIR_ENTRY_SCAN_BATCH == 0 and time.monotonic() - budget.start > budget.seconds:
                raise _ReclamationIncomplete("directory listing exceeded reclamation time budget")
            names.append(de.name)
    names.sort()
    return names


def _verify_object_dependencies(
    store: Any,
    entry: Any,
    commit_generation: int,
    budget: _ReclamationBudget,
    *,
    verified_descriptors: set[bytes],
    verified_nodes: set[bytes],
    verified_chunks: dict[bytes, int],
    node_append_buf: bytearray | None,
    chunk_append_buf: bytearray | None,
    embedded_module: Any,
) -> None:
    """Verify binding, manifest, descriptor, nodes, and unique chunks for one root."""
    binding_record = store._read_binding(entry.binding_id, entry.representation_root)
    _schema, _identity, logical, content, root, binding_gen = binding_record
    if logical != entry.logical_id or content != entry.content_id or root != entry.representation_root:
        raise StoreIntegrityError("Store binding does not match generation entry")
    if binding_gen > commit_generation:
        raise StoreIntegrityError("Store binding points into a future generation")

    manifest_path = store.path / "objects" / f"{entry.representation_root.hex()}.physical"
    if not manifest_path.exists():
        manifest_path = store.path / "objects" / f"{entry.logical_id.hex()}.manifest"
    manifest_raw = budget.read(manifest_path)

    adaptive: tuple[bytes, bytes] | None = None
    if len(manifest_raw) == embedded_module.MANIFEST_V2.size:
        (
            magic,
            version,
            reserved,
            total_length,
            chunk_size,
            chunk_count,
            depth,
            content_id,
            tree_root,
            descriptor_id,
            binding_id,
            logical_id,
        ) = embedded_module.MANIFEST_V2.unpack(manifest_raw)
        if magic != embedded_module.MANIFEST_MAGIC or version != embedded_module.VERSION or reserved != 0:
            raise StoreIntegrityError("Store object manifest is unknown")
    elif len(manifest_raw) == embedded_module.MANIFEST_V3.size:
        (
            magic,
            version,
            reserved,
            total_length,
            chunk_size,
            chunk_count,
            depth,
            content_id,
            tree_root,
            descriptor_id,
            binding_id,
            logical_id,
            envelope_digest,
            reps_digest,
        ) = embedded_module.MANIFEST_V3.unpack(manifest_raw)
        if magic != embedded_module.MANIFEST_MAGIC or version != embedded_module.MANIFEST_VERSION_V3 or reserved != 0:
            raise StoreIntegrityError("Store object manifest is unknown")
        adaptive = (envelope_digest, reps_digest)
    else:
        raise StoreIntegrityError("Store object manifest has an invalid size")

    if logical_id != entry.logical_id or binding_id != entry.binding_id:
        raise StoreIntegrityError("Store object manifest binding is inconsistent")
    if content_id != entry.content_id or store._verify_digest(manifest_raw) != entry.representation_root:
        raise StoreIntegrityError("Store object manifest root identity mismatch")
    if chunk_size != embedded_module.CHUNK_SIZE or chunk_count < 1 or total_length != entry.total_length:
        raise StoreIntegrityError("Store object manifest geometry is invalid")

    if descriptor_id not in verified_descriptors:
        descriptor = budget.read(store.path / "descriptors" / f"{descriptor_id.hex()}.desc")
        if store._verify_digest(descriptor) != descriptor_id:
            raise StoreIntegrityError("Store descriptor integrity failure")
        verified_descriptors.add(descriptor_id)

    if adaptive is not None:
        raise _ReclamationIncomplete("adaptive reachability requires a verified collector")

    expected_chunks = max(1, (total_length + embedded_module.CHUNK_SIZE - 1) // embedded_module.CHUNK_SIZE)
    if chunk_count != expected_chunks:
        raise StoreIntegrityError("content geometry is inconsistent")
    capacity, expected_depth = embedded_module.FANOUT, 0
    while capacity < chunk_count:
        capacity *= embedded_module.FANOUT
        expected_depth += 1
    if depth != expected_depth:
        raise StoreIntegrityError("content depth is inconsistent")

    node_struct = embedded_module.NODE_HEADER
    fanout = embedded_module.FANOUT
    chunk_size_const = embedded_module.CHUNK_SIZE

    def visit(digest: bytes, level: int, start: int) -> None:
        span = fanout**level
        remaining = chunk_count - start
        expected_count = min(fanout, (remaining + span - 1) // span)
        if expected_count < 1:
            raise StoreIntegrityError("Store content tree contains an empty branch")
        node_key = _NODE_RECORD_STRUCT.pack(digest, level, start, expected_count)
        if node_key in verified_nodes:
            # Post-order insertion guarantees all descendants are already verified
            # and present in verified_chunks; validate tail chunk lengths when leaf
            # cardinality matches single-chunk expectation cleanly.
            return

        node_raw = budget.read(store.path / "nodes" / f"{digest.hex()}.node")
        if store._verify_digest(node_raw) != digest or len(node_raw) < node_struct.size:
            raise StoreIntegrityError("Store content node integrity failure")
        magic, ver, res, actual_level, actual_start, count = node_struct.unpack_from(node_raw)
        if (
            magic != embedded_module.NODE_MAGIC
            or ver != embedded_module.VERSION
            or res != 0
            or actual_level != level
            or actual_start != start
            or count != expected_count
            or len(node_raw) != node_struct.size + count * 32
        ):
            raise StoreIntegrityError("Store content node structure is invalid")
        children = [
            node_raw[node_struct.size + i * 32 : node_struct.size + (i + 1) * 32]
            for i in range(count)
        ]
        if level == 0:
            if chunk_count == 1 and children[0] != content_id:
                raise StoreIntegrityError("Store content identity verification failed")
            for idx, child_digest in enumerate(children):
                chunk_index = start + idx
                expected_len = min(chunk_size_const, total_length - chunk_index * chunk_size_const)
                if expected_len < 0:
                    raise StoreIntegrityError("Store content chunk length binding failed")
                known_len = verified_chunks.get(child_digest)
                if known_len is not None:
                    if known_len != expected_len:
                        raise StoreIntegrityError("Store content chunk length binding failed")
                    budget.chunks.add(f"{child_digest.hex()}.chunk")
                    continue
                chunk_raw = budget.read(store.path / "chunks" / f"{child_digest.hex()}.chunk")
                if store._verify_digest(chunk_raw) != child_digest:
                    raise StoreIntegrityError(f"Store content chunk {chunk_index} failed integrity")
                if len(chunk_raw) != expected_len:
                    raise StoreIntegrityError("Store content chunk length binding failed")
                verified_chunks[child_digest] = len(chunk_raw)
                if chunk_append_buf is not None:
                    chunk_append_buf.extend(_CHUNK_RECORD_STRUCT.pack(child_digest, len(chunk_raw)))
        else:
            for idx, child_digest in enumerate(children):
                visit(child_digest, level - 1, start + idx * span)

        verified_nodes.add(node_key)
        if node_append_buf is not None:
            node_append_buf.extend(node_key)

    visit(tree_root, depth, 0)


def _prune_generations_resumable(
    store: Any,
    embedded_module: Any,
    *,
    keep_last: int,
    dry_run: bool,
    protected_generations: Sequence[int],
    policy_id: str | None,
    reclaim_chunks: bool,
    audit_path: Path | None,
    mark_path: Path,
    expected_head: int | None,
    expected_plan_digest: str | None,
    max_files: int,
    max_bytes: int,
    max_seconds: float,
) -> dict[str, object]:
    """Bounded, resumable reachability analysis and fail-closed reclamation."""
    mark_dir = _validate_external_dir_target(Path(mark_path), store.path, "mark_path")
    mark_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_fd = os.open(mark_dir / "mark.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise StoreError(
                StoreResultCode.DENIED,
                "mark state is locked by another maintenance worker",
            ) from exc

        _sweep_stage_artifacts(mark_dir)
        head = store.current_generation
        pins = tuple(protected_generations)
        floor = store.retention_floor(keep_last=keep_last, protected_generations=pins)
        sorted_pins = sorted(set(pins))

        if expected_head is not None and head != expected_head:
            raise StoreError(StoreResultCode.DENIED, "retention plan is stale: Store head changed")

        report: dict[str, object] = dict(
            head=head,
            floor=floor,
            keep_last=keep_last,
            dry_run=bool(dry_run),
            policy_id=policy_id,
            protected_generations=sorted_pins,
            policy_asserted=policy_id is not None,
            verification_scope=("all-retained-v2-payloads" if reclaim_chunks else "generation-chain-only"),
            status="unknown",
            prunable_files=0,
            prunable_bytes=0,
            removed_files=0,
            removed_bytes=0,
            candidates=[],
            removed=[],
            mark_resumable=True,
        )

        budget = _ReclamationBudget(store.path, max_files, max_bytes, max_seconds)
        store._reclamation_reader = budget.read
        store._invalidate_projection()
        store._invalidate_generation_parts_cache()
        publication_guard = None
        try:
            if not dry_run:
                guard = store._publication_lock(nonblocking=True)
                guard.__enter__()
                publication_guard = guard
                head = store.current_generation
                floor = store.retention_floor(keep_last=keep_last, protected_generations=pins)
                report["head"] = head
                report["floor"] = floor
                if expected_head is not None and head != expected_head:
                    raise StoreError(StoreResultCode.DENIED, "retention plan is stale: Store head changed")

            if any((store.path / "staging").iterdir()):
                raise _ReclamationIncomplete("pending recovery roots: reopen with recovery before maintenance")

            meta_path = store.path / "meta"
            meta_st = meta_path.stat(follow_symlinks=False)
            _meta_len, meta_sha256 = _sha256_file(meta_path)
            genesis_path = store.path / "generations" / f"{0:016x}"
            if not genesis_path.is_file():
                raise StoreIntegrityError("head or genesis generation is missing")
            _, genesis_sha256 = _sha256_file(genesis_path)
            head_gen_path = store.path / "generations" / f"{head:016x}"
            if not head_gen_path.is_file():
                raise StoreIntegrityError("head or genesis generation is missing")
            _, head_sha256 = _sha256_file(head_gen_path)
            floor_gen_path = store.path / "generations" / f"{floor:016x}"
            if not floor_gen_path.is_file():
                raise StoreIntegrityError("retention checkpoint is missing")
            _, floor_sha256 = _sha256_file(floor_gen_path)
            for pin in sorted_pins:
                if not (store.path / "generations" / f"{pin:016x}").is_file():
                    raise StoreIntegrityError(f"protected generation is missing: {pin}")

            state, invalid_reason = _load_mark_state(mark_dir)
            if state is not None:
                if (
                    state.get("store_meta_inode") != [meta_st.st_dev, meta_st.st_ino]
                    or state.get("store_meta_sha256") != meta_sha256
                ):
                    raise StoreError(StoreResultCode.DENIED, "mark state belongs to a different Store")

            policy_sig = {
                "keep_last": keep_last,
                "pins": sorted_pins,
                "reclaim_chunks": bool(reclaim_chunks),
            }
            need_reset = state is None
            if state is not None:
                if (
                    state.get("genesis_sha256") != genesis_sha256
                    or state.get("floor") != floor
                    or state.get("floor_sha256") != floor_sha256
                    or state.get("keep_last") != keep_last
                    or state.get("pins") != sorted_pins
                    or state.get("reclaim_chunks") != bool(reclaim_chunks)
                ):
                    need_reset = True
                    invalid_reason = "retention-policy-or-floor-changed"
                elif head < int(state.get("anchor_head", -1)):
                    need_reset = True
                    invalid_reason = "store-head-regressed"

            if not need_reset and state is not None:
                for fname, meta_info in state.get("data_files", {}).items():
                    fpath = mark_dir / fname
                    if not fpath.exists() or fpath.is_symlink():
                        need_reset = True
                        invalid_reason = "mark-state-corrupted"
                        break
                    try:
                        actual_digest = _sha256_prefix(fpath, int(meta_info["bytes"]))
                        if actual_digest != meta_info["sha256"]:
                            need_reset = True
                            invalid_reason = "mark-state-corrupted"
                            break
                    except (OSError, ValueError, StoreIntegrityError):
                        need_reset = True
                        invalid_reason = "mark-state-corrupted"
                        break

            if need_reset:
                _reset_mark_dir(mark_dir)
                state = {
                    "format": _MARK_FORMAT,
                    "store_meta_sha256": meta_sha256,
                    "store_meta_inode": [meta_st.st_dev, meta_st.st_ino],
                    "genesis_sha256": genesis_sha256,
                    **policy_sig,
                    "floor": floor,
                    "floor_sha256": floor_sha256,
                    "anchor_head": head,
                    "anchor_head_sha256": head_sha256,
                    "phase": "gen_dir",
                    "gen_names_built": False,
                    "gen_dir_cursor": 0,
                    "gen_dir_mtime_ns": _dir_mtime_ns(store.path / "generations"),
                    "gen_cursor": 0,
                    "root_cursor": 0,
                    "chunk_names_built": False,
                    "chunk_scan_cursor": 0,
                    "chunks_dir_mtime_ns": 0,
                    "verified_descriptors_hex": [],
                    "needs_fresh_inventory": False,
                    "apply_in_progress": False,
                    "data_files": {},
                }
            assert state is not None
            if invalid_reason:
                report["mark_invalidated_reason"] = invalid_reason

            dirty_files: set[str] = set()

            # Refresh candidate inventories cleanly after a previous apply/crash, or if
            # head advanced cleanly with identical floor/policy, or if generations
            # dir mutated externally while resuming.
            prev_anchor = int(state["anchor_head"])
            current_gens_mtime = _dir_mtime_ns(store.path / "generations")
            if state.get("needs_fresh_inventory") or state.get("apply_in_progress"):
                state["needs_fresh_inventory"] = False
                state["apply_in_progress"] = False
                state["anchor_head"] = head
                state["anchor_head_sha256"] = head_sha256
                state["phase"] = "gen_dir"
                state["gen_names_built"] = False
                state["gen_dir_cursor"] = 0
                state["gen_dir_mtime_ns"] = current_gens_mtime
                state["gen_cursor"] = 0
                state["chunk_names_built"] = False
                state["chunk_scan_cursor"] = 0
                for reset_name in (
                    "gen_names.txt",
                    "scanned_gens.bin",
                    "retained_gens.bin",
                    "prunable_gens.bin",
                    "chunk_names.txt",
                    "orphan_chunks.bin",
                ):
                    (mark_dir / reset_name).unlink(missing_ok=True)
                    state["data_files"].pop(reset_name, None)
            elif head > prev_anchor:
                old_anchor_path = store.path / "generations" / f"{prev_anchor:016x}"
                if not old_anchor_path.is_file():
                    raise StoreIntegrityError("retained generation history is incomplete in [floor, head]")
                _, observed_old_anchor_sha = _sha256_file(old_anchor_path)
                if observed_old_anchor_sha != state["anchor_head_sha256"]:
                    raise StoreIntegrityError("retained generation changed digest")
                state["anchor_head"] = head
                state["anchor_head_sha256"] = head_sha256
                state["gen_dir_mtime_ns"] = current_gens_mtime
                # Retain verified roots/nodes/chunks and extend generation inventory cleanly.
                state["phase"] = "gen_dir"
                state["gen_names_built"] = False
                state["gen_dir_cursor"] = 0
                state["chunk_names_built"] = False
                state["chunk_scan_cursor"] = 0
                for reset_name in (
                    "gen_names.txt",
                    "scanned_gens.bin",
                    "retained_gens.bin",
                    "prunable_gens.bin",
                    "chunk_names.txt",
                    "orphan_chunks.bin",
                ):
                    (mark_dir / reset_name).unlink(missing_ok=True)
                    state["data_files"].pop(reset_name, None)
            elif (
                state["phase"] == "gen_dir"
                and state.get("gen_names_built")
                and current_gens_mtime != state.get("gen_dir_mtime_ns")
            ):
                state["gen_names_built"] = False
                state["gen_dir_cursor"] = 0
                state["gen_dir_mtime_ns"] = current_gens_mtime
                for reset_name in ("gen_names.txt", "scanned_gens.bin"):
                    (mark_dir / reset_name).unlink(missing_ok=True)
                    state["data_files"].pop(reset_name, None)

            # ---- Phase "gen_dir": bounded directory scan & validation ----
            gen_names_path = mark_dir / "gen_names.txt"
            scanned_gens_path = mark_dir / "scanned_gens.bin"
            retained_path = mark_dir / "retained_gens.bin"
            prunable_path = mark_dir / "prunable_gens.bin"

            if state["phase"] == "gen_dir":
                if not state.get("gen_names_built"):
                    state["gen_dir_mtime_ns"] = current_gens_mtime
                    names = _list_dir_sorted(store.path / "generations", budget)
                    _atomic_write_bytes(gen_names_path, ("\n".join(names) + ("\n" if names else "")).encode("utf-8"))
                    dirty_files.add("gen_names.txt")
                    state["gen_names_built"] = True
                    state["gen_dir_cursor"] = 0
                    scanned_gens_path.unlink(missing_ok=True)
                    state["data_files"].pop("scanned_gens.bin", None)
                gen_names = [line for line in gen_names_path.read_text("utf-8").splitlines() if line]
                cursor = int(state["gen_dir_cursor"])
                append_buf = bytearray()
                try:
                    for idx in range(cursor, len(gen_names)):
                        budget.tick()
                        name = gen_names[idx]
                        path = store.path / "generations" / name
                        try:
                            n = int(name, 16)
                        except ValueError:
                            n = -1
                        if (
                            n < 0
                            or name != f"{n:016x}"
                            or not path.is_file(follow_symlinks=False)
                        ):
                            raise _ReclamationIncomplete("unknown generation artifact must be retained")
                        if n > head:
                            raise _ReclamationIncomplete("uncommitted generation requires recovery investigation")
                        size = path.stat(follow_symlinks=False).st_size
                        append_buf += _RETAINED_GEN_STRUCT.pack(n, size)
                        cursor = idx + 1
                except _ReclamationIncomplete as exc:
                    _append_binary_records(scanned_gens_path, bytes(append_buf))
                    if append_buf:
                        dirty_files.add("scanned_gens.bin")
                    state["gen_dir_cursor"] = cursor
                    _save_mark_state(mark_dir, state, dirty_files)
                    report.update(
                        status="unknown",
                        reason=str(exc),
                        analysis_phase="generations",
                        generations_scanned=cursor,
                        generations_total=len(gen_names),
                    )
                    return report
                _append_binary_records(scanned_gens_path, bytes(append_buf))
                if append_buf:
                    dirty_files.add("scanned_gens.bin")
                state["gen_dir_cursor"] = cursor
                if _dir_mtime_ns(store.path / "generations") != state["gen_dir_mtime_ns"]:
                    raise _ReclamationIncomplete("generations directory changed during analysis; retry")

                generations_map: dict[int, Path] = {}
                retained_buf = bytearray()
                prunable_buf = bytearray()
                if scanned_gens_path.exists():
                    raw_scanned = scanned_gens_path.read_bytes()
                    records = [
                        _RETAINED_GEN_STRUCT.unpack_from(raw_scanned, off)
                        for off in range(0, len(raw_scanned), _RETAINED_GEN_STRUCT.size)
                    ]
                else:
                    records = []
                for n, _size in records:
                    generations_map[n] = store.path / "generations" / f"{n:016x}"
                _validate_retained_generation_inventory(generations_map, head, floor, pins)
                pin_set = set(sorted_pins)
                for n, size in sorted(records):
                    if n == 0 or n >= floor or n in pin_set:
                        retained_buf += _RETAINED_GEN_STRUCT.pack(n, size)
                    else:
                        prunable_buf += _RETAINED_GEN_STRUCT.pack(n, size)
                _atomic_write_bytes(retained_path, bytes(retained_buf))
                _atomic_write_bytes(prunable_path, bytes(prunable_buf))
                dirty_files.update(("retained_gens.bin", "prunable_gens.bin"))
                state["phase"] = "gen_roots"
                _save_mark_state(mark_dir, state, dirty_files)
                del gen_names, raw_scanned, records, generations_map, retained_buf, prunable_buf

            # Load retained generations ordered list
            raw_ret = retained_path.read_bytes() if retained_path.exists() else b""
            retained_list = [
                _RETAINED_GEN_STRUCT.unpack_from(raw_ret, off)[0]
                for off in range(0, len(raw_ret), _RETAINED_GEN_STRUCT.size)
            ]
            del raw_ret
            retained_set = set(retained_list)

            roots_path = mark_dir / "roots_queue.bin"
            # ---- Phase "gen_roots": parse retained generations & extract unique roots ----
            if state["phase"] == "gen_roots":
                seen_roots_digest: dict[bytes, bytes] = {}
                if roots_path.exists():
                    raw_roots = roots_path.read_bytes()
                    for off in range(0, len(raw_roots), _ROOT_RECORD_STRUCT.size):
                        lid, bid, rroot, cid, tlen, ord_val, _fgen = _ROOT_RECORD_STRUCT.unpack_from(raw_roots, off)
                        seen_roots_digest[rroot] = hashlib.sha256(
                            struct.pack(">12s32s32s32sQQ", lid, bid, rroot, cid, tlen, ord_val)
                        ).digest()
                    del raw_roots
                gen_cursor = int(state["gen_cursor"])

                roots_append = bytearray()
                try:
                    while gen_cursor < len(retained_list):
                        n = retained_list[gen_cursor]
                        raw = budget.read(store.path / "generations" / f"{n:016x}")
                        if n == floor and floor > 0 and raw[:2] != embedded_module.GENERATION_MAGIC:
                            raise StoreIntegrityError("retention floor must be a full generation checkpoint")
                        if raw[:2] == embedded_module.GENERATION_MAGIC:
                            entries, _, _ = store._parse_generation_parts(raw, n)
                        elif raw[:2] == embedded_module.GENERATION_DELTA_MAGIC:
                            parent, entries, _, _ = store._parse_generation_delta_parts(raw, n)
                            if parent not in retained_set:
                                raise StoreIntegrityError(f"retained delta parent is missing: {parent}")
                        else:
                            raise StoreIntegrityError("unknown retained generation encoding")
                        if reclaim_chunks:
                            for entry in entries:
                                edig = _entry_digest(entry)
                                prev_edig = seen_roots_digest.get(entry.representation_root)
                                if prev_edig is not None:
                                    if prev_edig != edig:
                                        raise StoreIntegrityError("physical root has conflicting generation entries")
                                    continue
                                seen_roots_digest[entry.representation_root] = edig
                                roots_append += _ROOT_RECORD_STRUCT.pack(
                                    entry.logical_id,
                                    entry.binding_id,
                                    entry.representation_root,
                                    entry.content_id,
                                    entry.total_length,
                                    entry.ordinal,
                                    n,
                                )
                        gen_cursor += 1
                        if len(roots_append) >= 4 * 1024 * 1024:
                            _append_binary_records(roots_path, bytes(roots_append))
                            roots_append.clear()
                            dirty_files.add("roots_queue.bin")
                            state["gen_cursor"] = gen_cursor
                            _save_mark_state(mark_dir, state, dirty_files)
                except _ReclamationIncomplete as exc:
                    _append_binary_records(roots_path, bytes(roots_append))
                    if roots_append:
                        dirty_files.add("roots_queue.bin")
                    state["gen_cursor"] = gen_cursor
                    _save_mark_state(mark_dir, state, dirty_files)
                    report.update(
                        status="unknown",
                        reason=str(exc),
                        analysis_phase="gen_roots",
                        generations_verified=gen_cursor,
                        generations_total=len(retained_list),
                        unique_roots_discovered=len(seen_roots_digest),
                    )
                    return report
                _append_binary_records(roots_path, bytes(roots_append))
                if roots_append:
                    dirty_files.add("roots_queue.bin")
                state["gen_cursor"] = gen_cursor
                state["phase"] = "verify_roots" if reclaim_chunks else "complete"
                _save_mark_state(mark_dir, state, dirty_files)

            nodes_path = mark_dir / "verified_nodes.bin"
            chunks_path = mark_dir / "verified_chunks.bin"
            verified_chunks: dict[bytes, int] = {}
            if reclaim_chunks and chunks_path.exists():
                raw_chunks = chunks_path.read_bytes()
                for off in range(0, len(raw_chunks), _CHUNK_RECORD_STRUCT.size):
                    cdig, clen = _CHUNK_RECORD_STRUCT.unpack_from(raw_chunks, off)
                    verified_chunks[cdig] = clen

            # ---- Phase "verify_roots": verify unique roots & dependencies ----
            total_roots = (roots_path.stat().st_size // _ROOT_RECORD_STRUCT.size) if roots_path.exists() else 0
            if state["phase"] == "verify_roots":
                verified_nodes: set[bytes] = set()
                if nodes_path.exists():
                    raw_nodes = nodes_path.read_bytes()
                    verified_nodes = {
                        raw_nodes[off : off + _NODE_RECORD_STRUCT.size]
                        for off in range(0, len(raw_nodes), _NODE_RECORD_STRUCT.size)
                    }
                verified_descriptors = {bytes.fromhex(h) for h in state.get("verified_descriptors_hex", [])}
                root_cursor = int(state["root_cursor"])
                node_append_buf = bytearray()
                chunk_append_buf = bytearray()
                EntryCls = embedded_module._Entry
                try:
                    if roots_path.exists() and root_cursor < total_roots:
                        with roots_path.open("rb") as rf:
                            rf.seek(root_cursor * _ROOT_RECORD_STRUCT.size)
                            while root_cursor < total_roots:
                                rec_raw = rf.read(_ROOT_RECORD_STRUCT.size)
                                if len(rec_raw) < _ROOT_RECORD_STRUCT.size:
                                    break
                                lid, bid, rroot, cid, tlen, ord_val, first_gen = _ROOT_RECORD_STRUCT.unpack(rec_raw)
                                budget.tick()
                                _verify_object_dependencies(
                                    store,
                                    EntryCls(lid, bid, rroot, cid, tlen, ord_val),
                                    first_gen,
                                    budget,
                                    verified_descriptors=verified_descriptors,
                                    verified_nodes=verified_nodes,
                                    verified_chunks=verified_chunks,
                                    node_append_buf=node_append_buf,
                                    chunk_append_buf=chunk_append_buf,
                                    embedded_module=embedded_module,
                                )
                                root_cursor += 1
                                if len(chunk_append_buf) >= 2 * 1024 * 1024 or root_cursor % 4096 == 0:
                                    _append_binary_records(nodes_path, bytes(node_append_buf))
                                    _append_binary_records(chunks_path, bytes(chunk_append_buf))
                                    if node_append_buf:
                                        dirty_files.add("verified_nodes.bin")
                                    if chunk_append_buf:
                                        dirty_files.add("verified_chunks.bin")
                                    node_append_buf.clear()
                                    chunk_append_buf.clear()
                                    state["root_cursor"] = root_cursor
                                    state["verified_descriptors_hex"] = sorted(d.hex() for d in verified_descriptors)
                                    _save_mark_state(mark_dir, state, dirty_files)
                except _ReclamationIncomplete as exc:
                    _append_binary_records(nodes_path, bytes(node_append_buf))
                    _append_binary_records(chunks_path, bytes(chunk_append_buf))
                    if node_append_buf:
                        dirty_files.add("verified_nodes.bin")
                    if chunk_append_buf:
                        dirty_files.add("verified_chunks.bin")
                    state["root_cursor"] = root_cursor
                    state["verified_descriptors_hex"] = sorted(d.hex() for d in verified_descriptors)
                    _save_mark_state(mark_dir, state, dirty_files)
                    report.update(
                        status="unknown",
                        reason=str(exc),
                        analysis_phase="verify_roots",
                        roots_verified=root_cursor,
                        roots_total=total_roots,
                        chunks_verified=len(verified_chunks),
                    )
                    return report
                _append_binary_records(nodes_path, bytes(node_append_buf))
                _append_binary_records(chunks_path, bytes(chunk_append_buf))
                if node_append_buf:
                    dirty_files.add("verified_nodes.bin")
                if chunk_append_buf:
                    dirty_files.add("verified_chunks.bin")
                state["root_cursor"] = root_cursor
                state["verified_descriptors_hex"] = sorted(d.hex() for d in verified_descriptors)
                state["phase"] = "chunks_scan"
                _save_mark_state(mark_dir, state, dirty_files)

            # ---- Phase "chunks_scan": bounded chunks/ classification ----
            chunk_names_path = mark_dir / "chunk_names.txt"
            orphan_path = mark_dir / "orphan_chunks.bin"
            if state["phase"] == "chunks_scan":
                current_chunks_mtime = _dir_mtime_ns(store.path / "chunks")
                if (
                    not state.get("chunk_names_built")
                    or current_chunks_mtime != state.get("chunks_dir_mtime_ns")
                ):
                    state["chunks_dir_mtime_ns"] = current_chunks_mtime
                    cnames = _list_dir_sorted(store.path / "chunks", budget)
                    _atomic_write_bytes(chunk_names_path, ("\n".join(cnames) + ("\n" if cnames else "")).encode("utf-8"))
                    orphan_path.unlink(missing_ok=True)
                    state["data_files"].pop("orphan_chunks.bin", None)
                    dirty_files.add("chunk_names.txt")
                    state["chunk_names_built"] = True
                    state["chunk_scan_cursor"] = 0

                scan_cursor = int(state["chunk_scan_cursor"])
                chunk_names = [line for line in chunk_names_path.read_text("utf-8").splitlines() if line]
                orphan_append = bytearray()
                try:
                    for idx in range(scan_cursor, len(chunk_names)):
                        budget.tick()
                        name = chunk_names[idx]
                        cpath = store.path / "chunks" / name
                        if (
                            len(name) != 70
                            or not name.endswith(".chunk")
                            or any(c not in "0123456789abcdef" for c in name[:64])
                            or not cpath.is_file(follow_symlinks=False)
                        ):
                            raise _ReclamationIncomplete("unknown chunk artifact must be retained")
                        cdig = bytes.fromhex(name[:64])
                        in_set = cdig in verified_chunks
                        if not in_set and store.session.prune_decide(False) == 0:
                            csize = cpath.stat(follow_symlinks=False).st_size
                            name_bytes = name.encode("utf-8")
                            orphan_append += struct.pack(">HQ", len(name_bytes), csize) + name_bytes
                        scan_cursor = idx + 1
                except _ReclamationIncomplete as exc:
                    _append_binary_records(orphan_path, bytes(orphan_append))
                    if orphan_append:
                        dirty_files.add("orphan_chunks.bin")
                    state["chunk_scan_cursor"] = scan_cursor
                    _save_mark_state(mark_dir, state, dirty_files)
                    report.update(
                        status="unknown",
                        reason=str(exc),
                        analysis_phase="chunks_scan",
                        chunks_scanned=scan_cursor,
                        chunks_total=len(chunk_names),
                        chunks_verified=len(verified_chunks),
                    )
                    return report
                _append_binary_records(orphan_path, bytes(orphan_append))
                if orphan_append:
                    dirty_files.add("orphan_chunks.bin")
                state["chunk_scan_cursor"] = scan_cursor
                if _dir_mtime_ns(store.path / "chunks") != state["chunks_dir_mtime_ns"]:
                    state["chunk_names_built"] = False
                    state["chunk_scan_cursor"] = 0
                    _save_mark_state(mark_dir, state, dirty_files)
                    raise _ReclamationIncomplete("chunks directory changed during analysis; retry")
                state["phase"] = "complete"
                _save_mark_state(mark_dir, state, dirty_files)

            # ---- Phase "complete": revalidate head/dirs & apply if authorized ----
            if store.current_generation != int(state["anchor_head"]):
                state["phase"] = "gen_dir"
                state["needs_fresh_inventory"] = True
                _save_mark_state(mark_dir, state, dirty_files)
                raise _ReclamationIncomplete("head changed during analysis; retry")
            if reclaim_chunks and _dir_mtime_ns(store.path / "chunks") != state.get("chunks_dir_mtime_ns"):
                state["phase"] = "chunks_scan"
                state["chunk_names_built"] = False
                state["chunk_scan_cursor"] = 0
                _save_mark_state(mark_dir, state, dirty_files)
                raise _ReclamationIncomplete("chunks directory changed after analysis; retry")
            if _dir_mtime_ns(store.path / "generations") != state.get("gen_dir_mtime_ns"):
                state["phase"] = "gen_dir"
                state["needs_fresh_inventory"] = True
                _save_mark_state(mark_dir, state, dirty_files)
                raise _ReclamationIncomplete("generations directory changed after analysis; retry")

            candidate_paths: list[tuple[Path, int]] = []
            if prunable_path.exists():
                raw_prun = prunable_path.read_bytes()
                for off in range(0, len(raw_prun), _RETAINED_GEN_STRUCT.size):
                    gn, gsize = _RETAINED_GEN_STRUCT.unpack_from(raw_prun, off)
                    candidate_paths.append((store.path / "generations" / f"{gn:016x}", gsize))
            if reclaim_chunks and orphan_path.exists():
                raw_orph = orphan_path.read_bytes()
                pos = 0
                while pos < len(raw_orph):
                    nlen, osize = struct.unpack_from(">HQ", raw_orph, pos)
                    pos += 10
                    oname = raw_orph[pos : pos + nlen].decode("utf-8")
                    pos += nlen
                    candidate_paths.append((store.path / "chunks" / oname, osize))

            candidate_paths.sort(key=lambda item: (item[0].parent.name != "generations", str(item[0])))
            items = [{"path": str(path.relative_to(store.path)), "bytes": size} for path, size in candidate_paths]
            report.update(
                status="complete",
                analysis_phase="complete",
                candidates=items,
                prunable_files=len(items),
                prunable_bytes=sum(size for _, size in candidate_paths),
                verified_physical_roots=total_roots if reclaim_chunks else 0,
                referenced_chunks=len(verified_chunks) if reclaim_chunks else None,
            )
            plan_bytes = json.dumps(
                {k: report[k] for k in ("head", "floor", "keep_last", "policy_id", "protected_generations", "candidates")},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            report["plan_digest"] = store._verify_digest(plan_bytes).hex()

            if expected_head is not None and head != expected_head:
                raise StoreError(StoreResultCode.DENIED, "retention plan is stale: Store head changed")
            if expected_plan_digest is not None and report["plan_digest"] != expected_plan_digest:
                raise StoreError(StoreResultCode.DENIED, "retention plan is stale: plan digest changed")

            if dry_run or not candidate_paths:
                return report

            if audit_path is None:
                raise StoreError(StoreResultCode.DENIED, "destructive maintenance requires an external audit_path")
            resolved_audit = Path(audit_path).absolute()
            if (
                resolved_audit.resolve().is_relative_to(store.path)
                or resolved_audit.resolve().is_relative_to(mark_dir)
            ):
                raise StoreError(StoreResultCode.DENIED, "audit must live outside the reclaimed Store and mark directory")
            fd = os.open(resolved_audit, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            state["apply_in_progress"] = True
            _save_mark_state(mark_dir, state, dirty_files)

            def audit(value: object) -> None:
                data = memoryview(json.dumps(value, sort_keys=True).encode() + b"\n")
                while data:
                    written = os.write(fd, data)
                    if written <= 0:
                        raise OSError("short reclamation audit write")
                    data = data[written:]
                os.fsync(fd)

            try:
                audit({"plan_digest": report["plan_digest"], "plan": json.loads(plan_bytes)})
                _sync_dir(resolved_audit.parent)
                for (cpath, csize), item in zip(candidate_paths, items, strict=True):
                    try:
                        budget.tick(0)
                    except _ReclamationIncomplete as error:
                        report.update(status="partial", reason=str(error))
                        break
                    audit({"attempt": item["path"]})
                    store._trip("before_reclamation_unlink")
                    cpath.unlink()
                    store._trip("after_reclamation_unlink_before_receipt")
                    _sync_dir(cpath.parent)
                    report["removed"].append(item)
                    report["removed_files"] += 1
                    report["removed_bytes"] += csize
                    audit({"removed": item})
                audit({
                    "status": report["status"],
                    "removed_files": report["removed_files"],
                    "removed_bytes": report["removed_bytes"],
                })
            finally:
                os.close(fd)
                # Force fresh generation/chunk candidate discovery on next call, while
                # preserving already-verified root/chunk marks for partial continuation.
                state["needs_fresh_inventory"] = True
                state["apply_in_progress"] = False
                _save_mark_state(mark_dir, state, dirty_files)
            return report
        except _ReclamationIncomplete as error:
            report.update(status="unknown", reason=str(error))
            return report
        except KeyError as error:
            raise StoreIntegrityError("retention checkpoint is missing") from error
        finally:
            store._reclamation_reader = None
            store._invalidate_projection()
            store._invalidate_generation_parts_cache()
            store._entry_index_generation = None
            store._entries_by_binding = {}
            store._entry_logical_ids = set()
            if publication_guard is not None:
                publication_guard.__exit__(None, None, None)
            report.update(
                scanned_files=budget.files,
                read_bytes=budget.bytes,
                elapsed_seconds=time.monotonic() - budget.start,
            )
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


def _validate_backup_generation_inventory(
    store: Any,
    embedded_module: Any,
    head: int,
) -> list[Path]:
    gen_dir = store.path / "generations"
    if not gen_dir.is_dir():
        raise StoreIntegrityError("Store generations directory is missing")
    by_num: dict[int, Path] = {}
    for entry in os.scandir(gen_dir):
        if not entry.is_file(follow_symlinks=False):
            raise StoreIntegrityError(f"Store backup encountered non-regular generation entry: {entry.name}")
        try:
            n = int(entry.name, 16)
        except ValueError as exc:
            raise StoreIntegrityError(f"Store backup encountered unknown generation entry: {entry.name}") from exc
        if n < 0 or entry.name != f"{n:016x}":
            raise StoreIntegrityError(f"Store backup encountered malformed generation name: {entry.name}")
        if n <= head:
            by_num[n] = Path(entry.path)
    if 0 not in by_num or head not in by_num:
        raise StoreIntegrityError("Store backup requires genesis and head generation")

    # Every delta generation present up to head must have its immediate parent, and
    # the newest checkpoint covering head must have a complete suffix to head.
    for n in sorted(by_num):
        if n == 0:
            continue
        raw = by_num[n].read_bytes()
        magic = raw[:2]
        if magic == embedded_module.GENERATION_MAGIC:
            store._parse_generation_parts(raw, n)
        elif magic == embedded_module.GENERATION_DELTA_MAGIC:
            parent, _, _, _ = store._parse_generation_delta_parts(raw, n)
            if parent not in by_num:
                raise StoreIntegrityError(f"Store generation delta {n} parent {parent} is missing")
        else:
            raise StoreIntegrityError(f"Store generation {n} has unknown encoding")
    return [by_num[n] for n in sorted(by_num)]


def create_backup(
    store: Any,
    destination: Path,
    embedded_module: Any,
    *,
    verify_payloads: bool = False,
) -> dict[str, object]:
    """Create a crash-safe, SHA-256 manifest-sealed backup at an external path."""
    if store._closed:
        raise StoreError(StoreResultCode.DENIED, "Store is closed")
    dest = _validate_external_dir_target(Path(destination), store.path, "backup destination")
    if dest.exists():
        raise StoreError(StoreResultCode.DENIED, "backup destination already exists")

    def _capture() -> dict[str, object]:
        staging = store.path / "staging"
        if staging.is_dir() and any(staging.iterdir()):
            raise StoreError(
                StoreResultCode.RECOVERY_REQUIRED,
                "pending recovery roots: recover Store before backup",
            )
        head = store.current_generation
        gen_paths = _validate_backup_generation_inventory(store, embedded_module, head)
        commit_feed_hex = store.commit_feed(head).hex()
        if verify_payloads:
            store.verify()

        dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        stage_dir = dest.parent / f".{dest.name}.{os.getpid()}.{time.time_ns()}.stage"
        stage_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
        files_meta: list[dict[str, object]] = []
        total_bytes = 0
        try:
            for req in ("meta", "head"):
                src = store.path / req
                raw = src.read_bytes()
                dst = stage_dir / req
                _atomic_write_bytes(dst, raw)
                files_meta.append({
                    "path": req,
                    "bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                })
                total_bytes += len(raw)
            if store.current_generation != head:
                raise StoreError(StoreResultCode.DENIED, "Store head changed during backup; retry")

            # Empty coordination/staging dirs restored cleanly without live lock state.
            for empty_dir in ("staging",):
                (stage_dir / empty_dir).mkdir(mode=0o700, exist_ok=True)
                _sync_dir(stage_dir / empty_dir)

            gen_set = {p.name for p in gen_paths}
            for subdir in _STORE_DATA_SUBDIRS:
                src_sub = store.path / subdir
                dst_sub = stage_dir / subdir
                dst_sub.mkdir(mode=0o700, exist_ok=True)
                if not src_sub.is_dir():
                    _sync_dir(dst_sub)
                    continue
                entries = sorted(os.scandir(src_sub), key=lambda de: de.name)
                for de in entries:
                    if de.name.startswith(".") and de.name.endswith(".stage"):
                        continue
                    if not de.is_file(follow_symlinks=False):
                        raise StoreIntegrityError(f"backup encountered non-regular Store artifact: {subdir}/{de.name}")
                    if subdir == "generations" and de.name not in gen_set:
                        continue
                    src_file = Path(de.path)
                    dst_file = dst_sub / de.name
                    raw = src_file.read_bytes()
                    fd = os.open(dst_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    try:
                        view = memoryview(raw)
                        while view:
                            written = os.write(fd, view)
                            if written <= 0:
                                raise OSError("short backup write")
                            view = view[written:]
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    files_meta.append({
                        "path": f"{subdir}/{de.name}",
                        "bytes": len(raw),
                        "sha256": hashlib.sha256(raw).hexdigest(),
                    })
                    total_bytes += len(raw)
                _sync_dir(dst_sub)

            # Final consistency check against head & staging mutations during unlocked read-only backup.
            if store.current_generation != head or (staging.is_dir() and any(staging.iterdir())):
                raise StoreError(StoreResultCode.DENIED, "Store changed during backup; fence writers and retry")

            manifest = {
                "format": _BACKUP_FORMAT,
                "head": head,
                "commit_feed": commit_feed_hex,
                "verify_payloads": bool(verify_payloads),
                "total_files": len(files_meta),
                "total_bytes": total_bytes,
                "files": sorted(files_meta, key=lambda item: str(item["path"])),
            }
            manifest["manifest_sha256"] = _canonical_json_digest(manifest)
            _atomic_write_bytes(
                stage_dir / "backup-manifest.json",
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
            _sync_dir(stage_dir)
            os.replace(stage_dir, dest)
            _sync_dir(dest.parent)
            return {
                "status": "complete",
                "backup_dir": str(dest),
                "head": head,
                "commit_feed": commit_feed_hex,
                "total_files": len(files_meta),
                "total_bytes": total_bytes,
                "manifest_sha256": manifest["manifest_sha256"],
            }
        except BaseException:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise

    if not store.read_only:
        with store._publication_lock(nonblocking=store._maintenance):
            store._recover_pending_locked()
            return _capture()
    return _capture()


def verify_backup(
    backup_dir: Path,
    embedded_cls: type[Any],
    embedded_module: Any,
    *,
    verify_payloads: bool = True,
    session: Any = None,
) -> dict[str, object]:
    """Verify backup completeness, exact SHA-256 digests, and Store readability."""
    bdir = Path(backup_dir).expanduser().resolve()
    if bdir.is_symlink() or not bdir.is_dir():
        raise StoreIntegrityError("backup directory is missing or invalid")
    manifest_path = bdir / "backup-manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise StoreIntegrityError("incomplete backup: backup-manifest.json is missing")
    try:
        manifest = json.loads(manifest_path.read_text("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StoreIntegrityError("corrupt backup manifest") from exc
    if not isinstance(manifest, dict) or manifest.get("format") != _BACKUP_FORMAT:
        raise StoreIntegrityError("unknown backup manifest format")
    recorded_digest = manifest.get("manifest_sha256")
    body = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    if not isinstance(recorded_digest, str) or recorded_digest != _canonical_json_digest(body):
        raise StoreIntegrityError("backup manifest digest mismatch")

    expected_files: dict[str, dict[str, Any]] = {}
    for item in manifest.get("files", []):
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise StoreIntegrityError("malformed backup file entry")
        expected_files[item["path"]] = item

    seen_rel: set[str] = set()
    allowed_top = {"backup-manifest.json", "meta", "head", "staging", *_STORE_DATA_SUBDIRS}
    for root_str, dirs, files in os.walk(bdir, followlinks=False):
        root_path = Path(root_str)
        if root_path.is_symlink():
            raise StoreIntegrityError("backup contains disallowed symlink directory")
        rel_dir = root_path.relative_to(bdir)
        if rel_dir != Path(".") and dirs:
            raise StoreIntegrityError(f"unexpected subdirectory in backup: {rel_dir}/{dirs[0]}")
        for d in dirs:
            if (root_path / d).is_symlink():
                raise StoreIntegrityError("backup contains disallowed symlink")
            if rel_dir == Path(".") and d not in allowed_top:
                raise StoreIntegrityError(f"unexpected directory in backup: {d}")
        for f in files:
            fpath = root_path / f
            if fpath.is_symlink():
                raise StoreIntegrityError("backup contains disallowed symlink file")
            rel = str(fpath.relative_to(bdir))
            if rel == "backup-manifest.json":
                continue
            seen_rel.add(rel)
            entry = expected_files.get(rel)
            if entry is None:
                raise StoreIntegrityError(f"unexpected file in backup: {rel}")
            fsize, fsha = _sha256_file(fpath)
            if fsize != entry["bytes"] or fsha != entry["sha256"]:
                raise StoreIntegrityError(f"corrupt backup artifact: {rel}")

    if seen_rel != set(expected_files):
        missing = sorted(set(expected_files) - seen_rel)[:4]
        raise StoreIntegrityError(f"incomplete backup missing files: {missing}")

    with embedded_cls(bdir, read_only=True, session=session) as check_store:
        if check_store.current_generation != manifest["head"]:
            raise StoreIntegrityError("backup head mismatches manifest")
        if check_store.commit_feed().hex() != manifest["commit_feed"]:
            raise StoreIntegrityError("backup commit_feed mismatches manifest")
        _validate_backup_generation_inventory(check_store, embedded_module, manifest["head"])
        verify_summary = check_store.verify() if verify_payloads else None

    return {
        "status": "complete",
        "verified": True,
        "backup_dir": str(bdir),
        "head": manifest["head"],
        "commit_feed": manifest["commit_feed"],
        "total_files": len(expected_files),
        "total_bytes": manifest["total_bytes"],
        "manifest_sha256": recorded_digest,
        "verify": verify_summary,
    }


def restore_backup(
    backup_dir: Path,
    target_dir: Path,
    embedded_cls: type[Any],
    embedded_module: Any,
    *,
    verify_payloads: bool = True,
    session: Any = None,
) -> dict[str, object]:
    """Verify backup integrity first, then restore atomically into an isolated target."""
    target = Path(target_dir).expanduser().resolve()
    backup_resolved = Path(backup_dir).expanduser().resolve()
    if target.is_symlink():
        raise StoreError(StoreResultCode.DENIED, "restore target cannot be a symlink")
    if target == backup_resolved or target.is_relative_to(backup_resolved):
        raise StoreError(StoreResultCode.DENIED, "restore target must be separate from backup")
    if target.exists():
        if not target.is_dir() or any(target.iterdir()):
            raise StoreError(StoreResultCode.DENIED, "restore target already exists and is not empty")

    # Clean up prior interrupted staging directories for this target automatically.
    if target.parent.is_dir():
        prefix = f".{target.name}.restore."
        for child in target.parent.iterdir():
            if child.name.startswith(prefix) and child.name.endswith(".stage") and child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    verified = verify_backup(
        backup_resolved,
        embedded_cls,
        embedded_module,
        verify_payloads=verify_payloads,
        session=session,
    )
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    stage_dir = target.parent / f".{target.name}.restore.{os.getpid()}.{time.time_ns()}.stage"
    stage_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        manifest = json.loads((backup_resolved / "backup-manifest.json").read_text("utf-8"))
        for subdir in ("staging", *_STORE_DATA_SUBDIRS):
            (stage_dir / subdir).mkdir(mode=0o700, exist_ok=True)
        for item in manifest["files"]:
            rel = item["path"]
            src = backup_resolved / rel
            dst = stage_dir / rel
            raw = src.read_bytes()
            if hashlib.sha256(raw).hexdigest() != item["sha256"]:
                raise StoreIntegrityError(f"backup changed during restore: {rel}")
            fd = os.open(dst, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                view = memoryview(raw)
                while view:
                    written = os.write(fd, view)
                    if written <= 0:
                        raise OSError("short restore write")
                    view = view[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
        for subdir in ("staging", *_STORE_DATA_SUBDIRS):
            _sync_dir(stage_dir / subdir)
        _sync_dir(stage_dir)
        os.replace(stage_dir, target)
        _sync_dir(target.parent)
    except BaseException:
        shutil.rmtree(stage_dir, ignore_errors=True)
        raise

    with embedded_cls(target, read_only=True, session=session) as restored:
        if restored.current_generation != verified["head"]:
            raise StoreIntegrityError("restored Store head mismatches backup")
        if restored.commit_feed().hex() != verified["commit_feed"]:
            raise StoreIntegrityError("restored Store commit_feed mismatches backup")
        verify_result = restored.verify() if verify_payloads else None

    return {
        "status": "complete",
        "verified": True,
        "target_dir": str(target),
        "backup_dir": str(backup_resolved),
        "head": verified["head"],
        "commit_feed": verified["commit_feed"],
        "restored_files": verified["total_files"],
        "restored_bytes": verified["total_bytes"],
        "verify": verify_result,
    }
