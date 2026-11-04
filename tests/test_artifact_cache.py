"""Disposable compiled-artifact filesystem policy, separate from Store data."""
import hashlib
import json
import os
from pathlib import Path
import threading
import time

from mncs_store import artifact_cache
from mncs_store.session import StoreSession


def configure(tmp_path, monkeypatch, **limits):
    root = tmp_path / "compiled-artifacts"
    monkeypatch.setenv("MNCS_STORE_ARTIFACT_CACHE", str(root))
    for name in ["MAX_BYTES", "MAX_ENTRIES", "MAX_AGE_SECONDS"]:
        monkeypatch.delenv("MNCS_STORE_ARTIFACT_CACHE_" + name, raising=False)
    for name, value in limits.items():
        monkeypatch.setenv("MNCS_STORE_ARTIFACT_CACHE_" + name, str(value))
    return root


def put(character, payload=b"payload"):
    key = character * 64
    StoreSession._write_cached_artifact(key, {"selected": key}, payload)
    return key


def old(root, key, seconds=100):
    when = time.time() - seconds
    os.utime(root / key / "manifest.json", (when, when))


def test_full_payload_deduplication_preserves_distinct_admission_keys(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch, MAX_BYTES=4096)
    payload = b"x" * 2048
    first, second = put("a", payload), put("b", payload)
    assert StoreSession._read_cached_artifact(first) == payload
    assert StoreSession._read_cached_artifact(second) == payload
    assert (root / first / "backend.json").stat().st_ino == (root / second / "backend.json").stat().st_ino
    assert len(artifact_cache.entries(root)) == 2
    assert artifact_cache.unique_bytes(artifact_cache.entries(root)) <= 4096


def test_lru_reads_protect_entries_from_count_eviction(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch, MAX_ENTRIES=2)
    first, second = put("a"), put("b")
    old(root, first, 100)
    old(root, second, 50)
    assert StoreSession._read_cached_artifact(first) == b"payload"
    third = put("c")
    assert StoreSession._read_cached_artifact(first) == b"payload"
    assert StoreSession._read_cached_artifact(second) is None
    assert StoreSession._read_cached_artifact(third) == b"payload"


def test_byte_age_and_oversize_limits_only_affect_persistence(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch, MAX_BYTES=1200)
    first = put("a", b"a" * 700)
    old(root, first)
    second = put("b", b"b" * 700)
    assert not (root / first).exists()
    assert (root / second).exists()
    oversized = put("c", b"c" * 2048)
    assert not (root / oversized).exists()
    old(root, second, 15 * 86400)
    report = artifact_cache.maintain(root)
    assert report["entries_after"] == 0


def test_cleanup_preserves_unknown_files_symlinks_and_reaps_owned_pending(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch)
    selected = put("a")
    (root / selected / "notes").write_text("keep")
    corrupt = root / ("b" * 64)
    corrupt.mkdir()
    (corrupt / "manifest.json").write_text("{}")
    (corrupt / "backend.json").write_bytes(b"unknown")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("keep")
    (root / ("c" * 64)).symlink_to(outside, target_is_directory=True)
    pending = root / (".pending-123-" + "d" * 32)
    pending.mkdir()
    (pending / "backend.json").write_bytes(b"partial")
    monkeypatch.setenv("MNCS_STORE_ARTIFACT_CACHE_MAX_ENTRIES", "0")
    put("e")
    assert (root / selected / "notes").exists()
    assert corrupt.exists()
    assert (outside / "keep").exists()
    assert not pending.exists()
    assert not (root / ("e" * 64)).exists()


def test_eviction_waits_for_a_reader_lease(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch, MAX_ENTRIES=1)
    put("a")
    complete = threading.Event()
    with artifact_cache.lease(root, exclusive=False):
        worker = threading.Thread(target=lambda: (put("b"), complete.set()))
        worker.start()
        assert not complete.wait(0.1)
    worker.join(5)
    assert complete.is_set()
    assert not (root / ("a" * 64)).exists()


def test_concurrent_writers_readers_and_eviction_return_only_valid_payloads(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch, MAX_ENTRIES=3)
    errors = []
    def run(worker):
        try:
            for iteration in range(4):
                key = hashlib.sha256(f"{worker}:{iteration}".encode()).hexdigest()
                payload = key.encode()
                StoreSession._write_cached_artifact(key, {"worker": worker}, payload)
                assert StoreSession._read_cached_artifact(key) in (None, payload)
        except Exception as error:
            errors.append(error)
    workers = [threading.Thread(target=run, args=(i,)) for i in range(6)]
    for worker in workers: worker.start()
    for worker in workers: worker.join(10)
    assert not any(worker.is_alive() for worker in workers)
    assert not errors
    assert len(artifact_cache.entries(root)) <= 3


def test_cache_failure_and_invalid_keys_do_not_publish(tmp_path, monkeypatch):
    root = configure(tmp_path, monkeypatch)
    root.mkdir()
    (root / ".retention.lock").symlink_to(tmp_path / "missing")
    put("a")
    assert StoreSession._read_cached_artifact("a" * 64) is None
    StoreSession._write_cached_artifact("../outside", {}, b"data")
    assert StoreSession._read_cached_artifact("../outside") is None
    assert not (tmp_path / "outside").exists()


def test_invalid_settings_use_safe_defaults(tmp_path, monkeypatch):
    configure(tmp_path, monkeypatch, MAX_BYTES=-1, MAX_ENTRIES="invalid")
    assert artifact_cache.Limits.configured() == artifact_cache.Limits()
