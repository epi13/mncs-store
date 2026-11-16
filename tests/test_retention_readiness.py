"""Comprehensive lifecycle, resumability, concurrency, and backup/restore tests."""

from __future__ import annotations

import hashlib
import multiprocessing
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from mncs_store import (
    EmbeddedStore,
    RepresentationInput,
    StoreError,
    StoreIntegrityError,
    StoreSession,
    embedded,
)
from test_embedded_store import _session


def _write_multi_generation_store(
    root: Path,
    *,
    count: int = 16,
    interval: int = 4,
    monkeypatch: pytest.MonkeyPatch,
    session: StoreSession | None = None,
) -> tuple[int, Path]:
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", interval)
    shared_big = (b"SHARED-BLOCK-" * 6000)  # ~78 KiB -> 2 chunks
    with EmbeddedStore(root, session=session or _session()) as store:
        for n in range(count):
            if n % 3 == 0:
                payload = shared_big + f"-{n}".encode()
            elif n % 2 == 0:
                payload = b"shared-small"
            else:
                payload = bytes([n % 251]) * 512
            res = store.put_bound_object(
                domain_schema=b"retention/lifecycle/1",
                domain_identity=f"item:{n}".encode(),
                descriptor=b"application/octet-stream",
                payload=payload,
                expected_generation=n,
            )
            assert res.committed
        head = store.current_generation
    return head, root


def _plant_orphan_chunk(root: Path, payload: bytes) -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    path = root / "chunks" / f"{digest}.chunk"
    path.write_bytes(payload)
    return path


def _sibling_environment_root() -> Path | None:
    common = Path(
        subprocess.check_output(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            text=True,
        ).strip()
    )
    candidate = common.parent.parent / "mncs-environment"
    return candidate if (candidate / "mncs_env" / "session_store.py").is_file() else None


def test_resumable_reachability_slices_across_all_phases_and_shared_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_dir = tmp_path / "store"
    mark_dir = tmp_path / "mark"
    head, _ = _write_multi_generation_store(store_dir, count=16, interval=4, monkeypatch=monkeypatch)
    orphan_a = _plant_orphan_chunk(store_dir, b"orphan-alpha")
    orphan_b = _plant_orphan_chunk(store_dir, b"orphan-beta")

    with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
        slices = 0
        seen_phases: set[str] = set()
        # Tight per-slice file budget forces multiple steps through gen_dir, gen_roots,
        # verify_roots, and chunks_scan without ever re-scanning completed phases.
        while True:
            slices += 1
            assert slices < 64, "resumable marking failed to converge"
            res = reader.prune_generations(
                keep_last=2,
                protected_generations=[5],
                policy_id="resumable-test",
                reclaim_chunks=True,
                dry_run=True,
                mark_path=mark_dir,
                max_files=12,
                max_bytes=256 * 1024,
                max_seconds=30,
            )
            phase = str(res.get("analysis_phase"))
            seen_phases.add(phase)
            if res["status"] == "complete":
                final_dry = res
                break
            assert res["status"] == "unknown"
            assert res["mark_resumable"] is True
            assert res["removed_files"] == 0
            assert res["scanned_files"] <= 13  # max_files + 1 failing boundary tick
            assert orphan_a.exists() and orphan_b.exists()

    assert {"generations", "gen_roots", "verify_roots", "chunks_scan", "complete"}.issubset(seen_phases)
    assert final_dry["head"] == head
    assert final_dry["floor"] == 4
    prunable_names = {item["path"] for item in final_dry["candidates"]}
    assert prunable_names == {
        f"generations/{n:016x}" for n in range(1, 4)
    } | {f"chunks/{orphan_a.name}", f"chunks/{orphan_b.name}"}

    # Apply under exclusive maintenance reusing verified mark_dir and plan digest guard
    with EmbeddedStore(store_dir, session=_session(), maintenance=True, verify_on_open=False) as maint:
        with pytest.raises(StoreError, match="plan digest changed"):
            maint.prune_generations(
                keep_last=2,
                protected_generations=[5],
                policy_id="different-policy-id",
                reclaim_chunks=True,
                dry_run=False,
                mark_path=mark_dir,
                audit_path=tmp_path / "wrong-policy-audit.jsonl",
                expected_head=head,
                expected_plan_digest=str(final_dry["plan_digest"]),
                max_files=12,
                max_bytes=256 * 1024,
            )
        assert not (tmp_path / "wrong-policy-audit.jsonl").exists()
        applied = maint.prune_generations(
            keep_last=2,
            protected_generations=[5],
            policy_id="resumable-test",
            reclaim_chunks=True,
            dry_run=False,
            mark_path=mark_dir,
            audit_path=tmp_path / "audit.jsonl",
            expected_head=head,
            expected_plan_digest=str(final_dry["plan_digest"]),
            max_files=12,
            max_bytes=256 * 1024,
        )
        assert applied["status"] == "complete"
        assert applied["removed_files"] == 5
        assert not orphan_a.exists() and not orphan_b.exists()
        assert maint.verify()["ok"] is True

        # Idempotent repeat finds 0 candidates cleanly
        repeat = maint.prune_generations(
            keep_last=2,
            protected_generations=[5],
            policy_id="resumable-test",
            reclaim_chunks=True,
            dry_run=False,
            mark_path=mark_dir,
            max_files=200,
        )
        assert repeat["status"] == "complete"
        assert repeat["prunable_files"] == 0
        assert repeat["removed_files"] == 0


def test_resumable_head_advancement_preserves_newly_referenced_orphan_and_invalidates_shifted_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_dir = tmp_path / "store"
    mark_dir = tmp_path / "mark"
    head, _ = _write_multi_generation_store(store_dir, count=12, interval=4, monkeypatch=monkeypatch)
    # Plant a chunk matching a payload that will be committed mid-analysis!
    future_payload = b"becomes-live-during-sweep-" * 10
    reused_chunk = _plant_orphan_chunk(store_dir, future_payload)

    with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
        first = reader.prune_generations(
            keep_last=2,
            reclaim_chunks=True,
            dry_run=True,
            mark_path=mark_dir,
            max_files=18,
            max_bytes=512 * 1024,
        )
        assert first["status"] == "unknown"

    # Concurrent writer advances head from 12 -> 13 (floor stays 8 since 13-2=11 -> floor 8)
    with EmbeddedStore(store_dir, session=_session()) as writer:
        res = writer.put_bound_object(
            domain_schema=b"retention/lifecycle/1",
            domain_identity=b"item:reused",
            descriptor=b"application/octet-stream",
            payload=future_payload,
            expected_generation=head,
        )
        assert res.committed
        new_head = writer.current_generation
        assert new_head == 13

    # Stale expected_head fails closed before touching anything
    with EmbeddedStore(store_dir, session=_session(), maintenance=True, verify_on_open=False) as maint:
        with pytest.raises(StoreError, match="stale"):
            maint.prune_generations(
                keep_last=2,
                policy_id="stale-guard",
                reclaim_chunks=True,
                dry_run=False,
                mark_path=mark_dir,
                audit_path=tmp_path / "stale-audit.jsonl",
                expected_head=head,
            )
        assert not (tmp_path / "stale-audit.jsonl").exists()

    # Resuming without old expected_head extends over delta (13) and MUST protect reused_chunk!
    with EmbeddedStore(store_dir, session=_session(), maintenance=True, verify_on_open=False) as maint:
        while True:
            step = maint.prune_generations(
                keep_last=2,
                policy_id="head-advance",
                reclaim_chunks=True,
                dry_run=False,
                mark_path=mark_dir,
                audit_path=tmp_path / "advance-audit.jsonl",
                max_files=18,
                max_bytes=512 * 1024,
            )
            if step["status"] == "complete":
                break
            assert step["status"] == "unknown"
        assert reused_chunk.exists()
        assert maint.current_generation == 13
        assert maint.verify()["ok"] is True

    # Now start a sweep at floor=8, then commit 14..16 so keep_last=4 shifts floor 8 -> 12!
    # The mark state MUST invalidate and not wrongly preserve dead chunks only in 8..11.
    mark2 = tmp_path / "mark2"
    with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
        p1 = reader.prune_generations(
            keep_last=4,
            reclaim_chunks=True,
            dry_run=True,
            mark_path=mark2,
            max_files=20,
        )
        assert p1["floor"] == 8 and p1["status"] == "unknown"

    with EmbeddedStore(store_dir, session=_session()) as writer:
        for g in range(13, 16):
            writer.put_bound_object(
                domain_schema=b"retention/lifecycle/1",
                domain_identity=f"shift:{g}".encode(),
                descriptor=b"bytes",
                payload=f"shift-payload-{g}".encode(),
                expected_generation=g,
            )

    with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
        resumed = reader.prune_generations(
            keep_last=4,
            reclaim_chunks=True,
            dry_run=True,
            mark_path=mark2,
            max_files=200,
        )
        assert resumed["floor"] == 12
        assert resumed["mark_invalidated_reason"] == "retention-policy-or-floor-changed"


def test_resumable_policy_change_and_corrupt_mark_recovery_need_no_manual_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_dir = tmp_path / "store"
    mark_dir = tmp_path / "mark"
    _write_multi_generation_store(store_dir, count=12, interval=4, monkeypatch=monkeypatch)

    with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
        s1 = reader.prune_generations(
            keep_last=2,
            reclaim_chunks=True,
            dry_run=True,
            mark_path=mark_dir,
            max_files=14,
        )
        assert s1["status"] == "unknown"

        # Change retention policy (add historical pin 2) mid-analysis -> must invalidate & preserve 1..3
        s2 = reader.prune_generations(
            keep_last=2,
            protected_generations=[2],
            reclaim_chunks=True,
            dry_run=True,
            mark_path=mark_dir,
            max_files=200,
        )
        assert s2["mark_invalidated_reason"] == "retention-policy-or-floor-changed"
        assert s2["floor"] == 0
        assert s2["prunable_files"] == 0

        # Simulate interrupted mark temp file + corrupted state.json + truncated binary mark tail
        (mark_dir / ".state.json.9999.stage").write_text("leftover temp")
        (mark_dir / "state.json").write_text("{corrupt-json")
        recovered = reader.prune_generations(
            keep_last=2,
            protected_generations=[2],
            reclaim_chunks=True,
            dry_run=True,
            mark_path=mark_dir,
            max_files=200,
        )
        assert recovered["status"] == "complete"
        assert recovered["mark_invalidated_reason"] == "mark-state-corrupted"
        assert not (mark_dir / ".state.json.9999.stage").exists()


def _resumable_crash_worker(store_str: str, mark_str: str, audit_str: str) -> None:
    def kill(name: str) -> None:
        if name == "after_reclamation_unlink_before_receipt":
            os._exit(137)

    embedded.GENERATION_CHECKPOINT_INTERVAL = 4
    with EmbeddedStore(
        Path(store_str), session=_session(), maintenance=True, verify_on_open=False, failpoint=kill
    ) as store:
        store.prune_generations(
            keep_last=2,
            policy_id="crash-resumable",
            reclaim_chunks=True,
            dry_run=False,
            mark_path=Path(mark_str),
            audit_path=Path(audit_str),
            max_files=500,
            max_bytes=16 * 1024 * 1024,
        )


def test_resumable_interrupted_unlink_recovers_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_dir = tmp_path / "store"
    mark_dir = tmp_path / "mark"
    _write_multi_generation_store(store_dir, count=12, interval=4, monkeypatch=monkeypatch)
    orphan_chunk = _plant_orphan_chunk(store_dir, b"interrupted-orphan")

    with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
        while True:
            step = reader.prune_generations(
                keep_last=2, reclaim_chunks=True, dry_run=True, mark_path=mark_dir, max_files=20
            )
            if step["status"] == "complete":
                break

    proc = multiprocessing.get_context("spawn").Process(
        target=_resumable_crash_worker,
        args=(str(store_dir), str(mark_dir), str(tmp_path / "crash-audit.jsonl")),
    )
    proc.start()
    proc.join(20)
    assert proc.exitcode == 137

    with EmbeddedStore(store_dir, session=_session(), maintenance=True, verify_on_open=False) as maint:
        retried = maint.prune_generations(
            keep_last=2,
            policy_id="crash-resumable",
            reclaim_chunks=True,
            dry_run=False,
            mark_path=mark_dir,
            audit_path=tmp_path / "retry-audit.jsonl",
            max_files=500,
            max_bytes=16 * 1024 * 1024,
        )
        assert retried["status"] == "complete"
        assert not orphan_chunk.exists()
        assert maint.verify()["ok"] is True


def test_resumable_adaptive_and_corruption_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_dir = tmp_path / "store"
    mark_dir = tmp_path / "mark"
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with StoreSession() as session:
        with EmbeddedStore(store_dir, session=session) as st:
            st.put_bound_object(
                domain_schema=b"adaptive/1",
                domain_identity=b"obj:0",
                descriptor=b"bytes",
                payload=bytes(128),
                expected_generation=0,
            )
            st.add_representation(
                b"adaptive/1",
                b"obj:0",
                RepresentationInput(5, "rle", bytes(128)),
                expected_generation=1,
            )
        orphan = _plant_orphan_chunk(store_dir, b"keep-when-adaptive-defers")
        with EmbeddedStore(store_dir, session=session, read_only=True) as reader:
            res = reader.prune_generations(
                keep_last=100,
                reclaim_chunks=True,
                dry_run=True,
                mark_path=mark_dir,
                max_files=50,
            )
            assert res["status"] == "unknown"
            assert "adaptive" in str(res["reason"])
            assert orphan.exists()

    # Corruption inside a chunk breaks resumable verification loudly before any delete
    corrupt_dir = tmp_path / "corrupt-store"
    corrupt_mark = tmp_path / "corrupt-mark"
    _write_multi_generation_store(corrupt_dir, count=8, interval=4, monkeypatch=monkeypatch)
    target_chunk = next((corrupt_dir / "chunks").iterdir())
    target_chunk.write_bytes(b"corrupted-payload")
    with EmbeddedStore(corrupt_dir, session=_session(), read_only=True) as reader:
        with pytest.raises(StoreIntegrityError):
            reader.prune_generations(
                keep_last=2,
                reclaim_chunks=True,
                dry_run=True,
                mark_path=corrupt_mark,
                max_files=200,
            )


def _writer_during_mark_worker(store_str: str, ready: Any, go: Any) -> None:
    with EmbeddedStore(Path(store_str), session=_session()) as writer:
        ready.set()
        assert go.wait(15)
        head = writer.current_generation
        res = writer.put_bound_object(
            domain_schema=b"retention/lifecycle/1",
            domain_identity=b"concurrent:live",
            descriptor=b"bytes",
            payload=b"concurrent-writer-payload",
            expected_generation=head,
        )
        assert res.committed


def test_concurrent_writer_and_reader_exclusion_during_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_dir = tmp_path / "store"
    mark_dir = tmp_path / "mark"
    _write_multi_generation_store(store_dir, count=12, interval=4, monkeypatch=monkeypatch)
    ctx = multiprocessing.get_context("spawn")
    ready, go = ctx.Event(), ctx.Event()
    worker = ctx.Process(target=_writer_during_mark_worker, args=(str(store_dir), ready, go))
    worker.start()
    try:
        assert ready.wait(15)
        with EmbeddedStore(store_dir, session=_session(), read_only=True) as reader:
            step1 = reader.prune_generations(
                keep_last=2, reclaim_chunks=True, dry_run=True, mark_path=mark_dir, max_files=20
            )
            assert step1["status"] == "unknown"
            with pytest.raises(StoreError, match="busy"):
                EmbeddedStore(store_dir, session=_session(), maintenance=True)
        go.set()
        worker.join(15)
        assert worker.exitcode == 0
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join(5)

    with EmbeddedStore(store_dir, session=_session(), maintenance=True, verify_on_open=False) as maint:
        while True:
            step = maint.prune_generations(
                keep_last=2,
                policy_id="post-concurrent-writer",
                reclaim_chunks=True,
                dry_run=False,
                mark_path=mark_dir,
                audit_path=tmp_path / "conc-audit.jsonl",
                max_files=32,
            )
            if step["status"] == "complete":
                break
        assert maint.current_generation == 13
        assert maint.verify()["ok"] is True


def test_backup_verify_and_isolated_restore_reconstruct_environment_history_and_reject_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_root = _sibling_environment_root()
    if env_root is None:
        pytest.skip("sibling mncs-environment checkout is unavailable")
    monkeypatch.syspath_prepend(str(env_root))
    from mncs_env import sessions
    from mncs_env.session_store import open_store

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    state = tmp_path / "source-state"
    package = Path(embedded.__file__).resolve().parents[1]
    consumer = open_store(
        state,
        "store",
        store_package_dir=package,
        verify_on_open=False,
        defer_mutation=False,
    )
    try:
        owner = sessions.Session.create(
            state_dir=state,
            environment={
                "identity": "backup-fixture",
                "workspace": {"root": str(tmp_path), "repositories": []},
                "intent": {"goal": "backup restore proof", "repositories": []},
                "bindings": [],
            },
            consumer_id="backup-owner",
            authenticated_principal_id="backup-principal",
            store=consumer,
        )
        owner.transition("resolving", "fixture")
        owner.transition("ready", "fixture")
        owner.transition("active", "fixture")
        consumer.put_claim(
            {
                "claim_id": "claim:backup",
                "version": 1,
                "session_id": owner.session_id,
                "repository": "fixture",
                "status": "held",
            }
        )
        ckpt = owner.checkpoint(progress="checkpoint-preserved", remaining=["verify-restore"])
        protected_gen = consumer.generation()
        saved_events = consumer.read_events(owner.session_id)
        handoff = owner.handoff(to_consumer="successor", notes=["preserve evidence"])
        saved_claims = consumer.read_claims()
        saved_snapshot = consumer.load_snapshot(owner.session_id)
        src_store_path = consumer.backend.path
    finally:
        consumer.close()

    # Mimic historical gap strictly below floor checkpoint=4 (like production 1..34303), keeping 0 and 4..head
    for gen in range(1, 4):
        (src_store_path / "generations" / f"{gen:016x}").unlink(missing_ok=True)

    backup_dir = tmp_path / "store-backup"
    with EmbeddedStore(src_store_path, read_only=True) as ro:
        src_head = ro.current_generation
        src_feed = ro.commit_feed().hex()
        src_objects = ro.objects_at(src_head)
        src_floor_objs = ro.objects_at(4)
        src_bindings = ro.domain_bindings_at(protected_gen)
        receipt = ro.create_backup(backup_dir, verify_payloads=True)
    assert receipt["status"] == "complete"
    assert receipt["head"] == src_head
    assert receipt["commit_feed"] == src_feed

    # Create a leftover interrupted restore staging dir to verify auto-cleanup on retry
    restore_parent = tmp_path / "restore-root"
    restore_parent.mkdir()
    stale_stage = restore_parent / ".restored-store.restore.12345.stage"
    stale_stage.mkdir()
    (stale_stage / "partial").write_bytes(b"incomplete")

    restored_state = restore_parent / "restored-env"
    restored_store_path = restored_state / "store"
    # Wait: test both direct target name matching stale_stage prefix AND state/store naming
    direct_restore_target = restore_parent / "restored-store"
    r_receipt = EmbeddedStore.restore_backup(backup_dir, direct_restore_target, verify_payloads=True)
    assert r_receipt["status"] == "complete" and r_receipt["verified"] is True
    assert not stale_stage.exists()

    with EmbeddedStore(direct_restore_target, read_only=True) as check:
        assert check.current_generation == src_head
        assert check.commit_feed().hex() == src_feed
        assert check.objects_at(src_head) == src_objects
        assert check.objects_at(4) == src_floor_objs
        assert check.domain_bindings_at(protected_gen) == src_bindings

    # Restore into Environment state/store layout and verify high-level Session reconstruction
    env_restore = EmbeddedStore.restore_backup(backup_dir, restored_store_path, verify_payloads=True)
    assert env_restore["verified"] is True
    restored_consumer = open_store(
        restored_state,
        "store",
        store_package_dir=package,
        verify_on_open=False,
        defer_mutation=True,
    )
    try:
        assert restored_consumer.load_snapshot(owner.session_id) == saved_snapshot
        assert restored_consumer.read_events(owner.session_id)[: len(saved_events)] == saved_events
        assert ckpt in restored_consumer.list_checkpoints(owner.session_id)
        assert restored_consumer.load_handoff(owner.session_id, handoff["identity"]) == handoff
        assert restored_consumer.read_claims() == saved_claims
    finally:
        restored_consumer.close()

    # Refuses non-empty target
    with pytest.raises(StoreError, match="not empty"):
        EmbeddedStore.restore_backup(backup_dir, direct_restore_target)

    # Corrupt chunk, extra unexpected file, and missing manifest all fail closed before restore
    bad_chunk = next((backup_dir / "chunks").iterdir())
    original_bytes = bad_chunk.read_bytes()
    bad_chunk.write_bytes(b"corrupted-backup-bytes")
    with pytest.raises(StoreIntegrityError, match="corrupt backup"):
        EmbeddedStore.restore_backup(backup_dir, tmp_path / "never-created-1")
    assert not (tmp_path / "never-created-1").exists()
    bad_chunk.write_bytes(original_bytes)

    extra = backup_dir / "chunks" / "extra.dat"
    extra.write_bytes(b"tamper")
    with pytest.raises(StoreIntegrityError, match="unexpected file"):
        EmbeddedStore.verify_backup(backup_dir)
    extra.unlink()

    (backup_dir / "backup-manifest.json").unlink()
    with pytest.raises(StoreIntegrityError, match="incomplete backup"):
        EmbeddedStore.verify_backup(backup_dir)
