"""Bounded retention proofs; all destructive operations use isolated Stores."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path

import pytest
from mncs_store import (
    EmbeddedStore,
    StoreError,
    StoreIntegrityError,
    StoreSession,
    embedded,
)
from test_embedded_store import _session


def populate(path, count=12, session=None):
    with EmbeddedStore(path, session=session or _session()) as store:
        for n in range(count):
            store.put_bound_object(
                domain_schema=b"retention/1",
                domain_identity=str(n).encode(),
                descriptor=b"bytes",
                payload=b"shared" if n % 2 else bytes([n]) * 100,
                expected_generation=n,
            )
        return store.current_generation


def orphan(path, value=b"never committed"):
    import hashlib

    p = path / "chunks" / (hashlib.sha256(value).hexdigest() + ".chunk")
    p.write_bytes(value)
    return p


def test_policy_pin_checkpoint_history_sharing_and_idempotence(tmp_path, monkeypatch):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    head = populate(root)
    unused = orphan(root)
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False
    ) as store:
        historical = store.objects_at(4)
        current = store.objects_at(head)
        assert len({obj.content_id for obj in current}) < len(current)
        before_files = {
            str(p.relative_to(root)): p.read_bytes()
            for p in root.rglob("*")
            if p.is_file()
        }
        preview = store.prune_generations(
            keep_last=2,
            protected_generations=[5],
            policy_id="test-retain-checkpoint-4",
            reclaim_chunks=True,
        )
        assert preview["status"] == "complete" and preview["floor"] == 4
        assert preview["removed_files"] == 0
        assert {
            str(p.relative_to(root)): p.read_bytes()
            for p in root.rglob("*")
            if p.is_file()
        } == before_files
        actual = store.prune_generations(
            keep_last=2,
            protected_generations=[5],
            policy_id="test-retain-checkpoint-4",
            reclaim_chunks=True,
            dry_run=False,
            audit_path=tmp_path / "audit.jsonl",
        )
        assert actual["plan_digest"] == preview["plan_digest"]
        assert actual["removed_files"] == 4 and not unused.exists()
        assert [item["path"] for item in actual["removed"]][:3] == [
            f"generations/{n:016x}" for n in range(1, 4)
        ]
        assert actual["removed_bytes"] == sum(
            before_files[item["path"]].__len__() for item in actual["removed"]
        )
        assert store.objects_at(4) == historical and store.objects_at(head) == current
        assert store.current_generation == head
        again = store.prune_generations(
            keep_last=2,
            protected_generations=[5],
            policy_id="test-retain-checkpoint-4",
            reclaim_chunks=True,
            dry_run=False,
        )
        assert again["status"] == "complete" and again["removed_files"] == 0
        with pytest.raises(StoreIntegrityError):
            store.objects_at(1)
    audit = [
        json.loads(line) for line in (tmp_path / "audit.jsonl").read_text().splitlines()
    ]
    assert audit[0]["plan_digest"] == preview["plan_digest"]
    assert sum("removed" in row for row in audit) == 4
    with EmbeddedStore(root, session=_session(), read_only=True) as reopened:
        assert reopened.objects_at(4) == historical
        assert reopened.verify()["objects"] == head


def test_policies_do_not_invent_permission_or_historical_pins(tmp_path, monkeypatch):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    populate(root)
    with EmbeddedStore(root, session=_session()) as reader:
        with pytest.raises(StoreError, match="maintenance=True"):
            reader.prune_generations(dry_run=False, keep_last=1, policy_id="test")
        with pytest.raises(StoreError, match="busy"):
            EmbeddedStore(root, session=_session(), maintenance=True)
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False
    ) as store:
        with pytest.raises(StoreError, match="policy_id"):
            store.prune_generations(dry_run=False, keep_last=1)
        with pytest.raises(StoreError, match="audit_path"):
            store.prune_generations(dry_run=False, keep_last=1, policy_id="test")
        with pytest.raises(StoreError):
            store.prune_generations(protected_generations=[True])
        assert store.retention_floor(keep_last=1, protected_generations=[1]) == 0
        assert (
            store.prune_generations(keep_last=1, protected_generations=[1])[
                "prunable_files"
            ]
            == 0
        )
        for readonly in [True, False]:
            with pytest.raises(StoreError, match="busy"):
                EmbeddedStore(root, session=_session(), read_only=readonly)
    with EmbeddedStore(root, session=_session(), read_only=True) as reader:
        assert reader.current_generation == 12


@pytest.mark.parametrize(
    "options", [{"max_files": 2}, {"max_bytes": 1}, {"max_seconds": 1e-12}]
)
def test_exhausted_proofs_never_unlink(tmp_path, monkeypatch, options):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    populate(root)
    unused = orphan(root)
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False
    ) as store:
        report = store.prune_generations(
            keep_last=1,
            policy_id="test",
            reclaim_chunks=True,
            dry_run=False,
            audit_path=tmp_path / "audit",
            **options,
        )
        assert report["status"] == "unknown" and report["removed_files"] == 0
        assert unused.exists() and (root / "generations" / f"{1:016x}").exists()
        assert not (tmp_path / "audit").exists()


@pytest.mark.parametrize("kind", ["chunk", "checkpoint", "binding"])
def test_committed_corruption_is_failure_before_reclamation(
    tmp_path, monkeypatch, kind
):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    populate(root)
    unused = orphan(root)
    if kind == "chunk":
        target = next(p for p in (root / "chunks").iterdir() if p != unused)
    elif kind == "binding":
        target = next((root / "bindings").iterdir())
    else:
        target = root / "generations" / f"{8:016x}"
    target.write_bytes(b"corrupt")
    with EmbeddedStore(
        root, session=_session(), maintenance=True, read_only=True, verify_on_open=False
    ) as store:
        with pytest.raises((StoreIntegrityError, AssertionError)):
            store.prune_generations(keep_last=2, reclaim_chunks=True)
    assert unused.exists() and not (tmp_path / "audit").exists()
    # Constructor failures release the lifetime lock immediately, even when
    # exception tracebacks still retain the failed instance.
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False
    ):
        pass


def crash_cleanup(root_text, audit_text):
    root = Path(root_text)

    def kill(name):
        if name == "after_reclamation_unlink_before_receipt":
            os._exit(137)

    embedded.GENERATION_CHECKPOINT_INTERVAL = 4
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False, failpoint=kill
    ) as store:
        store.prune_generations(
            keep_last=2,
            policy_id="fault-test",
            reclaim_chunks=True,
            dry_run=False,
            audit_path=Path(audit_text),
        )


def test_process_death_reopens_and_reproves_without_replaying_audit(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    populate(root)
    unused = orphan(root)
    process = multiprocessing.get_context("spawn").Process(
        target=crash_cleanup, args=(str(root), str(tmp_path / "interrupted.jsonl"))
    )
    process.start()
    process.join(20)
    if process.is_alive():
        process.kill()
        process.join(5)
        pytest.fail("fault worker exceeded its budget")
    assert process.exitcode == 137
    audit = [
        json.loads(line)
        for line in (tmp_path / "interrupted.jsonl").read_text().splitlines()
    ]
    assert "attempt" in audit[-1] and "status" not in audit[-1]
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False
    ) as store:
        assert len(store.objects_at(8)) == 8 and len(store.objects_at(12)) == 12
        completed = store.prune_generations(
            keep_last=2,
            policy_id="fault-test",
            reclaim_chunks=True,
            dry_run=False,
            audit_path=tmp_path / "retry.jsonl",
        )
        assert completed["status"] == "complete" and not unused.exists()
    with EmbeddedStore(root, session=_session()) as writer:
        assert writer.put_bound_object(
            domain_schema=b"retention/1",
            domain_identity=b"after-recovery",
            descriptor=b"bytes",
            payload=b"healthy",
            expected_generation=12,
        ).committed


def try_open_worker(root_text, result):
    try:
        with EmbeddedStore(Path(root_text), session=_session()):
            result.put("opened")
    except StoreError:
        result.put("busy")


def test_other_process_admission_and_kernel_lock_release(tmp_path):
    root = tmp_path / "store"
    populate(root, 1)
    ctx = multiprocessing.get_context("spawn")
    with EmbeddedStore(root, session=_session(), maintenance=True):
        queue = ctx.Queue()
        worker = ctx.Process(target=try_open_worker, args=(str(root), queue))
        worker.start()
        worker.join(15)
        if worker.is_alive():
            worker.kill()
            worker.join(5)
            pytest.fail("admission worker exceeded budget")
        assert worker.exitcode == 0 and queue.get(timeout=2) == "busy"
        queue.close()
        queue.join_thread()


def test_native_policy_and_orphan_collector(tmp_path, monkeypatch):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    with StoreSession() as session:
        for head, window, pin, expected in [
            (12, 2, None, 8),
            (12, 2, 5, 4),
            (12, 2, 1, 0),
            (12, 20, None, 0),
        ]:
            assert session.retention_floor(head, window, 4, pin) == expected
        assert session.prune_decide(True) == 1 and session.prune_decide(False) == 0
        populate(root, 10, session=session)
        unused = orphan(root)
        with EmbeddedStore(
            root, session=session, maintenance=True, verify_on_open=False
        ) as store:
            before = store.objects_at(8)
            result = store.prune_generations(
                keep_last=2,
                policy_id="native-fixture",
                reclaim_chunks=True,
                dry_run=False,
                audit_path=tmp_path / "native-audit",
            )
            assert result["status"] == "complete" and not unused.exists()
            assert store.objects_at(8) == before


def test_native_adaptive_history_is_deferred_not_base_only_collected(tmp_path):
    from mncs_store import RepresentationInput

    root = tmp_path / "store"
    with StoreSession() as session:
        populate(root, 1, session=session)
        with EmbeddedStore(root, session=session) as store:
            old = store.objects_at(1)
            evolved = store.add_representation(
                b"retention/1",
                b"0",
                RepresentationInput(5, "rle", bytes(100)),
                expected_generation=1,
            )
            assert evolved.committed
            assert (
                old[0].representation_root
                != store.current_objects()[0].representation_root
            )
        unused = orphan(root)
        with EmbeddedStore(
            root, session=session, maintenance=True, verify_on_open=False
        ) as store:
            result = store.prune_generations(
                keep_last=100,
                protected_generations=[1],
                policy_id="preserve-all",
                reclaim_chunks=True,
                dry_run=False,
                audit_path=tmp_path / "never-written",
            )
            assert result["status"] == "unknown" and "adaptive" in result["reason"]
            assert result["removed_files"] == 0 and unused.exists()
            assert store.objects_at(1) == old
            assert not (tmp_path / "never-written").exists()


def test_pending_recovery_unknown_names_and_symlinks_defer(tmp_path, monkeypatch):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    populate(root)
    unused = orphan(root)
    with EmbeddedStore(
        root, session=_session(), maintenance=True, read_only=True
    ) as store:
        pending = root / "staging" / "000000000000000c-000000000000000d"
        pending.mkdir()
        assert "recovery" in store.prune_generations(reclaim_chunks=True)["reason"]
        pending.rmdir()
        unknown = root / "chunks" / "unclassified.data"
        unknown.write_bytes(b"preserve")
        assert "unknown chunk" in store.prune_generations(reclaim_chunks=True)["reason"]
        unknown.unlink()
        symlink = root / "chunks" / ("f" * 64 + ".chunk")
        symlink.symlink_to(unused)
        assert store.prune_generations(reclaim_chunks=True)["status"] == "unknown"
        symlink.unlink()
        assert unused.exists()


def test_environment_snapshots_events_checkpoints_handoffs_and_binding_cache(
    tmp_path, monkeypatch
):
    import subprocess

    common = Path(
        subprocess.check_output(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            text=True,
        ).strip()
    )
    projects = common.parent.parent
    environment_root = projects / "mncs-environment"
    if not environment_root.is_dir():
        pytest.skip("sibling Environment consumer is unavailable")
    monkeypatch.syspath_prepend(str(environment_root))
    from mncs_env import sessions
    from mncs_env.session_store import open_store

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    state = tmp_path / "state"
    # The already imported provider package is the tested worktree; no mutable
    # ambient production Store or environment entry is opened by this fixture.
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
                "identity": "retention-fixture",
                "workspace": {"root": str(tmp_path), "repositories": []},
                "intent": {"goal": "retention proof", "repositories": []},
                "bindings": [],
            },
            consumer_id="retention-owner",
            authenticated_principal_id="retention-test-principal",
            store=consumer,
        )
        owner.transition("resolving", "fixture")
        owner.transition("ready", "fixture")
        owner.transition("active", "fixture")
        consumer.put_claim(
            {
                "claim_id": "claim:isolated",
                "version": 1,
                "session_id": owner.session_id,
                "repository": "fixture",
                "status": "held",
            }
        )
        checkpoint = owner.checkpoint(
            progress="preserve checkpoint", remaining=["continue"]
        )
        protected = consumer.generation()
        saved_events = consumer.read_events(owner.session_id)
        handoff = owner.handoff(
            to_consumer="retention-successor", notes=["keep audit evidence"]
        )
        consumer.put_claim(
            {
                "claim_id": "claim:isolated",
                "version": 2,
                "session_id": owner.session_id,
                "repository": "fixture",
                "status": "released",
            }
        )
        saved_claims = consumer.read_claims()
        snapshot = consumer.load_snapshot(owner.session_id)
        path = consumer.backend.path
        assert consumer.backend._binding_directories
        assert consumer.load_snapshot(owner.session_id) == snapshot
        with pytest.raises(StoreError, match="busy"):
            EmbeddedStore(path, maintenance=True)
    finally:
        consumer.close()
    assert not consumer.backend._binding_directories
    with EmbeddedStore(path, maintenance=True, verify_on_open=False) as maintenance:
        previous_head = maintenance.current_generation
        old_bindings = maintenance.domain_bindings_at(protected)
        unused = orphan(path)
        result = maintenance.prune_generations(
            keep_last=1,
            protected_generations=[protected],
            policy_id="isolated-environment-history",
            reclaim_chunks=True,
            dry_run=False,
            audit_path=tmp_path / "consumer-audit",
        )
        assert result["status"] == "complete" and not unused.exists()
        assert maintenance.current_generation == previous_head
        assert maintenance.domain_bindings_at(protected) == old_bindings
    reopened = open_store(
        state,
        "store",
        store_package_dir=package,
        verify_on_open=False,
        defer_mutation=False,
    )
    try:
        assert reopened.load_snapshot(owner.session_id) == snapshot
        assert (
            reopened.read_events(owner.session_id)[: len(saved_events)] == saved_events
        )
        assert checkpoint in reopened.list_checkpoints(owner.session_id)
        assert reopened.load_handoff(owner.session_id, handoff["identity"]) == handoff
        assert reopened.read_claims() == saved_claims
    finally:
        reopened.close()


def test_partial_deletion_receipt_and_failed_audit_are_truthful(tmp_path, monkeypatch):
    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    root = tmp_path / "store"
    populate(root)
    with EmbeddedStore(
        root, session=_session(), maintenance=True, verify_on_open=False
    ) as store:
        existing = tmp_path / "existing-audit"
        existing.write_text("preserve foreign evidence")
        with pytest.raises(FileExistsError):
            store.prune_generations(
                keep_last=1, policy_id="test", dry_run=False, audit_path=existing
            )
        assert (root / "generations" / f"{1:016x}").exists()
        original = embedded._ReclamationBudget.tick
        attempts = []

        def tick(self, count=1):
            if count == 0:
                attempts.append(True)
                if len(attempts) == 2:
                    raise embedded._ReclamationIncomplete("test deadline exhausted")
            return original(self, count)

        monkeypatch.setattr(embedded._ReclamationBudget, "tick", tick)
        result = store.prune_generations(
            keep_last=1,
            policy_id="test",
            dry_run=False,
            audit_path=tmp_path / "partial-audit",
        )
        assert result["status"] == "partial" and result["removed_files"] == 1
        assert len(store.objects_at(8)) == 8
        assert (
            json.loads((tmp_path / "partial-audit").read_text().splitlines()[-1])[
                "status"
            ]
            == "partial"
        )
        assert existing.read_text() == "preserve foreign evidence"


def test_failed_constructor_does_not_pin_store_via_retained_traceback(tmp_path):
    import fcntl

    root = tmp_path / "store"
    populate(root, 1)
    (root / "generations" / f"{1:016x}").write_bytes(b"torn")
    errors = []
    try:
        EmbeddedStore(root, session=_session(), read_only=True, maintenance=True)
    except StoreIntegrityError as error:
        errors.append(error)
    assert errors
    with (root / "meta").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def held_reader_worker(root_text, ready, proceed, result):
    with EmbeddedStore(Path(root_text), session=_session(), read_only=True) as reader:
        previous = reader.objects_at(1)
        ready.set()
        if not proceed.wait(15):
            raise RuntimeError("reader coordination budget exhausted")
        result.put(reader.objects_at(1) == previous)


def test_historical_reader_survives_writer_and_blocks_maintenance(tmp_path):
    root = tmp_path / "store"
    populate(root, 1)
    ctx = multiprocessing.get_context("spawn")
    ready, proceed, result = ctx.Event(), ctx.Event(), ctx.Queue()
    worker = ctx.Process(
        target=held_reader_worker, args=(str(root), ready, proceed, result)
    )
    worker.start()
    try:
        assert ready.wait(15)
        with pytest.raises(StoreError, match="busy"):
            EmbeddedStore(root, session=_session(), maintenance=True)
        with EmbeddedStore(root, session=_session()) as writer:
            assert writer.put_bound_object(
                domain_schema=b"retention/1",
                domain_identity=b"next",
                descriptor=b"bytes",
                payload=b"next",
                expected_generation=1,
            ).committed
        proceed.set()
        assert result.get(timeout=10) is True
        worker.join(10)
        assert worker.exitcode == 0
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join(5)
        result.close()
        result.join_thread()
    with EmbeddedStore(root, session=_session(), maintenance=True):
        pass


def test_native_transaction_orphans_collected_after_automatic_recovery(tmp_path):
    root = tmp_path / "store"
    with StoreSession() as session:

        def crash(name):
            if name == "before_generation_publication":
                raise embedded.CrashInjected(name)

        with EmbeddedStore(root, session=session) as writer:
            writer.put_bound_object(
                domain_schema=b"retention/1",
                domain_identity=b"committed",
                descriptor=b"bytes",
                payload=b"S" * 65536,
                expected_generation=0,
            )
            writer._failpoint = crash
            with pytest.raises(embedded.CrashInjected):
                writer.put_bound_object(
                    domain_schema=b"retention/1",
                    domain_identity=b"interrupted",
                    descriptor=b"bytes",
                    payload=b"S" * 65536 + b"N" * 65536,
                    expected_generation=1,
                )
        with EmbeddedStore(
            root, session=session, maintenance=True, verify_on_open=False
        ) as maintenance:
            assert maintenance.current_generation == 1
            assert not list((root / "staging").iterdir())
            assert len(list((root / "chunks").iterdir())) == 2
            complete = maintenance.prune_generations(
                keep_last=100,
                policy_id="preserve-all-generations",
                reclaim_chunks=True,
                dry_run=False,
                audit_path=tmp_path / "abort-audit",
            )
            assert (
                complete["status"] == "complete" and complete["removed_bytes"] == 65536
            )
            assert len(list((root / "chunks").iterdir())) == 1
            assert maintenance.current_objects()[0].payload == b"S" * 65536


def test_maintenance_recovery_never_waits_on_a_legacy_publication_lock(
    tmp_path, monkeypatch
):
    import fcntl

    root = tmp_path / "store"
    populate(root, 1)
    original = fcntl.flock
    with (root / "publication.lock").open("rb") as held:
        original(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        publication_inode = os.fstat(held.fileno()).st_ino

        def observed(fd, operation):
            if os.fstat(fd).st_ino == publication_inode and operation & fcntl.LOCK_EX:
                assert operation & fcntl.LOCK_NB
            return original(fd, operation)

        monkeypatch.setattr(fcntl, "flock", observed)
        with pytest.raises(StoreError, match="publication is busy"):
            EmbeddedStore(
                root, session=_session(), maintenance=True, verify_on_open=False
            )
        with EmbeddedStore(root, session=_session(), read_only=True) as reader:
            assert reader.current_generation == 1


def _artifact_names(root: Path) -> tuple[set[str], set[str]]:
    generations = {path.name for path in (root / "generations").iterdir()}
    chunks = {path.name for path in (root / "chunks").iterdir()}
    return generations, chunks


def test_prune_rejects_missing_interval_before_checkpoint_both_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import EmbeddedStore, StoreIntegrityError, StoreSession, embedded

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with StoreSession() as session:
        populate(tmp_path, count=12, session=session)
        for gen in range(8, 12):
            (tmp_path / "generations" / f"{gen:016x}").unlink()
        before_generations, before_chunks = _artifact_names(tmp_path)

        with EmbeddedStore(tmp_path, session=session, maintenance=True) as store:
            assert store.retention_floor(keep_last=8) == 4
            for reclaim_chunks in (False, True):
                for dry_run in (True, False):
                    audit = tmp_path.parent / f"audit-{reclaim_chunks}-{dry_run}.jsonl"
                    with pytest.raises(StoreIntegrityError):
                        store.prune_generations(
                            keep_last=8,
                            dry_run=dry_run,
                            reclaim_chunks=reclaim_chunks,
                            policy_id="retention-completeness-regression",
                            audit_path=audit,
                        )
                    assert not audit.exists()

        after_generations, after_chunks = _artifact_names(tmp_path)
        assert after_generations == before_generations
        assert after_chunks == before_chunks
        for gen in range(1, 4):
            assert (tmp_path / "generations" / f"{gen:016x}").exists()


def test_prune_rejects_missing_interval_at_floor_zero_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import EmbeddedStore, StoreIntegrityError, StoreSession, embedded

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    for label, missing_range in (
        ("before-mid-checkpoint", range(2, 4)),
        ("before-head-checkpoint", range(8, 12)),
    ):
        store_dir = tmp_path / label
        with StoreSession() as session:
            populate(store_dir, count=12, session=session)
            for gen in missing_range:
                (store_dir / "generations" / f"{gen:016x}").unlink()
            before_generations, before_chunks = _artifact_names(store_dir)

            with EmbeddedStore(store_dir, session=session, maintenance=True) as store:
                assert store.retention_floor(keep_last=12) == 0
                for reclaim_chunks in (False, True):
                    for dry_run in (True, False):
                        audit = tmp_path / f"floor0-{label}-{reclaim_chunks}-{dry_run}.jsonl"
                        with pytest.raises(StoreIntegrityError):
                            store.prune_generations(
                                keep_last=12,
                                dry_run=dry_run,
                                reclaim_chunks=reclaim_chunks,
                                policy_id="retention-floor-zero-regression",
                                audit_path=audit,
                            )
                        assert not audit.exists()

            after_generations, after_chunks = _artifact_names(store_dir)
            assert after_generations == before_generations
            assert after_chunks == before_chunks


def test_prune_rejects_missing_floor_head_or_genesis_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import EmbeddedStore, StoreIntegrityError, StoreSession, embedded

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    for label, missing_gen in (("floor", 4), ("head", 12), ("genesis", 0)):
        store_dir = tmp_path / label
        with StoreSession() as session:
            populate(store_dir, count=12, session=session)
            with EmbeddedStore(store_dir, session=session, maintenance=True) as store:
                assert store.retention_floor(keep_last=8) == 4
                (store_dir / "generations" / f"{missing_gen:016x}").unlink()
                before_generations, before_chunks = _artifact_names(store_dir)
                for reclaim_chunks in (False, True):
                    audit = tmp_path / f"audit-{label}-{reclaim_chunks}.jsonl"
                    with pytest.raises(StoreIntegrityError):
                        store.prune_generations(
                            keep_last=8,
                            dry_run=False,
                            reclaim_chunks=reclaim_chunks,
                            policy_id="missing-boundary-regression",
                            audit_path=audit,
                        )
                    assert not audit.exists()
                after_generations, after_chunks = _artifact_names(store_dir)
                assert after_generations == before_generations
                assert after_chunks == before_chunks


def test_prune_allows_historical_gaps_strictly_below_positive_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import EmbeddedStore, StoreSession, embedded

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with StoreSession() as session:
        populate(tmp_path, count=12, session=session)
        # Historical gaps strictly below floor=8 remain acceptable when
        # genesis, pins, and floor..head are intact.
        for gen in (1, 2, 5, 6):
            (tmp_path / "generations" / f"{gen:016x}").unlink()

        with EmbeddedStore(tmp_path, session=session, maintenance=True) as store:
            assert store.retention_floor(keep_last=4) == 8
            audit = tmp_path.parent / "valid-gap-below-floor.jsonl"
            report = store.prune_generations(
                keep_last=4,
                dry_run=False,
                reclaim_chunks=True,
                protected_generations=[8],
                policy_id="gap-below-floor-regression",
                audit_path=audit,
            )
            assert report["status"] == "complete"
            assert report["floor"] == 8
            assert report["removed_files"] == 3  # generations 3, 4, 7
            assert audit.exists()
            assert (tmp_path / "generations" / f"{0:016x}").exists()
            for gen in range(8, 13):
                assert (tmp_path / "generations" / f"{gen:016x}").exists()
            assert store.verify()["ok"] is True


def test_prune_completeness_validation_is_bounded_for_arbitrary_numeric_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import struct
    import time

    from mncs_store import EmbeddedStore, StoreIntegrityError, StoreSession, embedded

    monkeypatch.setattr(embedded, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with StoreSession() as session:
        populate(tmp_path, count=4, session=session)
        with EmbeddedStore(tmp_path, session=session, maintenance=True) as store:
            huge_head = 10**12
            (tmp_path / "generations" / f"{huge_head:016x}").write_bytes(
                embedded.GENERATION_HEADER.pack(
                    embedded.GENERATION_MAGIC, embedded.VERSION, 0, huge_head, 0, 0, 0
                )
            )
            (tmp_path / "head").write_bytes(
                struct.pack(">2sBBQ", embedded.HEAD_MAGIC, embedded.VERSION, 0, huge_head)
            )
            for keep_last, pattern in (
                (huge_head, "incomplete"),
                (8, "retention checkpoint is missing"),
            ):
                audit = tmp_path.parent / f"huge-head-{keep_last}.jsonl"
                started = time.monotonic()
                with pytest.raises(StoreIntegrityError, match=pattern):
                    store.prune_generations(
                        keep_last=keep_last,
                        dry_run=True,
                        reclaim_chunks=True,
                        max_files=64,
                        max_seconds=2.0,
                        audit_path=audit,
                    )
                assert time.monotonic() - started < 1.0
                assert not audit.exists()

            positive_floor = store.retention_floor(keep_last=8)
            assert positive_floor > 0
            (tmp_path / "generations" / f"{positive_floor:016x}").write_bytes(
                embedded.GENERATION_HEADER.pack(
                    embedded.GENERATION_MAGIC, embedded.VERSION, 0, positive_floor, 0, 0, 0
                )
            )
            started = time.monotonic()
            with pytest.raises(StoreIntegrityError, match="incomplete"):
                store.prune_generations(
                    keep_last=8,
                    dry_run=True,
                    reclaim_chunks=True,
                    max_files=64,
                    max_seconds=2.0,
                    audit_path=tmp_path.parent / "huge-head-pos-floor.jsonl",
                )
            assert time.monotonic() - started < 1.0
