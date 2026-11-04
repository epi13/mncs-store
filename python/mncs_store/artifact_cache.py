"""Retention mechanics for disposable compiled Store artifacts, not Store data."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time
from typing import Iterator

SCHEMA = "mncs-store-artifact-cache/1"
HEX = re.compile(r"[0-9a-f]{64}\Z")
PENDING = re.compile(r"\.pending-[0-9]+-[0-9a-f]{32}\Z")


@dataclass(frozen=True)
class Limits:
    bytes: int = 2 * 1024**3
    entries: int = 32
    age_seconds: int = 14 * 86400

    @classmethod
    def configured(cls) -> Limits:
        defaults = cls()
        def value(name: str, default: int) -> int:
            try:
                result = int(os.environ.get(name, str(default)))
                return result if result >= 0 else default
            except ValueError:
                return default
        return cls(value("MNCS_STORE_ARTIFACT_CACHE_MAX_BYTES", defaults.bytes),
                   value("MNCS_STORE_ARTIFACT_CACHE_MAX_ENTRIES", defaults.entries),
                   value("MNCS_STORE_ARTIFACT_CACHE_MAX_AGE_SECONDS", defaults.age_seconds))


@contextmanager
def lease(root: Path, *, exclusive: bool) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(root / ".retention.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        os.close(descriptor)


def _only_cache_files(directory: Path) -> bool:
    return all(p.name in {"backend.json", "manifest.json"} and
               stat.S_ISREG(p.lstat().st_mode) for p in directory.iterdir())


def entries(root: Path) -> list[dict]:
    rows = []
    for directory in root.iterdir():
        if not HEX.fullmatch(directory.name) or not stat.S_ISDIR(directory.lstat().st_mode):
            continue
        if not _only_cache_files(directory):
            continue
        try:
            manifest_path = directory / "manifest.json"
            manifest_stat = manifest_path.stat()
            if manifest_stat.st_size > 8 * 1024**2:
                continue
            manifest = json.loads(manifest_path.read_text())
            if (not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA
                    or manifest.get("key") != directory.name
                    or not isinstance(manifest.get("artifact_sha256"), str)
                    or not HEX.fullmatch(manifest["artifact_sha256"])):
                continue
            payload = directory / "backend.json"
            metadata = payload.stat()
        except (OSError, ValueError, TypeError):
            continue
        rows.append({"directory": directory, "manifest": manifest,
                     "payload_bytes": metadata.st_size, "record_bytes": manifest_stat.st_size,
                     "payload_id": (metadata.st_dev, metadata.st_ino), "used_at": manifest_stat.st_mtime})
    return rows


def unique_bytes(rows: list[dict]) -> int:
    seen = set()
    total = 0
    for row in rows:
        total += row["record_bytes"]
        if row["payload_id"] not in seen:
            seen.add(row["payload_id"])
            total += row["payload_bytes"]
    return total


def prune(root: Path, limits: Limits | None = None) -> int:
    """Caller holds an exclusive lease; never classify durable Store objects."""
    limits = limits or Limits.configured()
    for directory in root.iterdir():
        if (PENDING.fullmatch(directory.name) and stat.S_ISDIR(directory.lstat().st_mode)
                and _only_cache_files(directory)):
            shutil.rmtree(directory)
    rows = sorted(entries(root), key=lambda r: (r["used_at"], r["directory"].name))
    removed = 0
    while rows and (len(rows) > limits.entries or unique_bytes(rows) > limits.bytes
                    or time.time() - rows[0]["used_at"] > limits.age_seconds):
        shutil.rmtree(rows.pop(0)["directory"])
        removed += 1
    return removed


def maintain(root: Path) -> dict:
    with lease(root, exclusive=True):
        before = entries(root)
        removed = prune(root)
        after = entries(root)
        return {"schema_version": "mncs.store-artifact-cache-maintenance/1",
                "directory": str(root), "removed_entries": removed,
                "entries_before": len(before), "entries_after": len(after),
                "unique_bytes_before": unique_bytes(before), "unique_bytes_after": unique_bytes(after)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    options = parser.parse_args()
    print(json.dumps(maintain(options.directory.expanduser().resolve()), sort_keys=True))
