"""Bounded local-realization tests for the supported embedded Store API.

These tests use a deterministic semantic-session double to exercise filesystem
publication and recovery quickly. The retained-artifact proof and independent
SHA oracle live in ``measure_embedded_store.py`` and the Store native corpus.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import struct
from pathlib import Path
from queue import Empty

import pytest

from mncs_store import BoundObjectInput, EmbeddedStore, StoreError, StoreResultCode
from mncs_store.embedded import CrashInjected, StoreIntegrityError


class SemanticSessionDouble:
    backend = "test-semantic-session"
    artifact_sha256 = "test-artifact"
    toolchain = "test-double"

    def __init__(self) -> None:
        self.call_count = 0
        self.hash_count = 0
        self.semantic_seconds = 0.0

    @staticmethod
    def _integer(value: int) -> dict[str, object]:
        return {"integer": {"value": value}}

    def sha256(self, value: bytes) -> bytes:
        self.hash_count += 1
        return hashlib.sha256(value).digest()

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
        return (
            b"MR\x02\x00"
            + bytes(relation_type)
            + bytes(source)
            + bytes(target)
            + int(generation).to_bytes(8, "big")
            + bytes(provenance)
            + int(ordinal).to_bytes(8, "big")
            + bytes(metadata_type or bytes(32))
            + bytes(metadata_root or bytes(32))
        )

    def validate_relation(self, raw: bytes, *, expected_generation: int | None = None) -> int:
        assert len(raw) == 172
        assert raw[:4] == b"MR\x02\x00"
        if expected_generation is not None:
            assert int.from_bytes(raw[60:68], "big") <= expected_generation
        return int.from_bytes(raw[60:68], "big")

    def encode_provenance(
        self,
        source: bytes,
        producer: bytes,
        transformation: bytes,
        generation: int,
        evidence: bytes,
        ancestry: bytes,
    ) -> bytes:
        return (
            b"MP\x01\x00"
            + bytes(source)
            + bytes(producer)
            + bytes(transformation)
            + int(generation).to_bytes(8, "big")
            + bytes(evidence)
            + bytes(ancestry)
        )

    def validate_provenance(self, raw: bytes, *, expected_generation: int | None = None) -> int:
        assert len(raw) == 112
        assert raw[:4] == b"MP\x01\x00"
        if expected_generation is not None:
            assert int.from_bytes(raw[40:48], "big") <= expected_generation
        return int.from_bytes(raw[40:48], "big")

    def encode_commit_feed(
        self,
        generation: int,
        objects: int,
        relations: int,
        provenance: int,
        root: bytes,
    ) -> bytes:
        return struct.pack(">2sBBQIII32s", b"MC", 1, 0, generation, objects, relations, provenance, root)

    @staticmethod
    def recovery_decide(previous_valid: bool, candidate_class: int) -> int:
        if candidate_class == 0:
            return 1
        return 0 if previous_valid else 2

    def call(self, _module: str, function: str, arguments: list[dict[str, object]], **_kwargs: object):
        self.call_count += 1
        if function == "cas_decide":
            observed = int(arguments[0]["integer"]["value"])
            expected = int(arguments[1]["integer"]["value"])
            return {"status": "returned", "returned": [self._integer(0 if observed == expected else 1)]}
        if function == "conflict_token":
            observed = int(arguments[0]["integer"]["value"])
            expected = int(arguments[1]["integer"]["value"])
            token = observed.to_bytes(8, "big") + expected.to_bytes(8, "big")
            return {"status": "returned", "returned": [{"sequence": {"values": [{"byte": {"value": item}} for item in token]}}]}
        raise AssertionError(f"unexpected semantic function: {function}")

    def close(self) -> None:
        return None


def _session() -> SemanticSessionDouble:
    return SemanticSessionDouble()


def _commit(path: Path, identity: bytes, payload: bytes, *, expected: int = 0) -> str:
    with EmbeddedStore(path, session=_session()) as store:
        return store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=identity,
            descriptor=b"test-descriptor/1",
            payload=payload,
            expected_generation=expected,
        ).code.value


def _race_worker(path_text: str, index: int, output) -> None:
    output.put(_commit(Path(path_text), f"race-{index}".encode(), bytes([index]) * 97))


def test_large_object_uses_bounded_tree_geometry(tmp_path: Path) -> None:
    payload = bytes((index * 13) % 256 for index in range(4_000_000))
    with EmbeddedStore(tmp_path, session=_session()) as store:
        result = store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"four-megabytes",
            descriptor=b"test-descriptor/1",
            payload=payload,
            expected_generation=0,
        )
        metrics = store.object_metrics(b"test.domain/1", b"four-megabytes")
        assert result.code is StoreResultCode.COMMITTED
        assert metrics["chunk_count"] == 62
        assert metrics["tree_depth"] == 1
        assert metrics["tree_node_count"] == 3
        assert metrics["content_bytes"] == 4_000_000
        assert metrics["stored_chunk_bytes"] <= metrics["content_bytes"]
        assert metrics["structural_bytes"] < 10_000
        reopened_object = store.get_bound_object(b"test.domain/1", b"four-megabytes")
        assert reopened_object.payload == payload
        assert reopened_object.generation == 1


def test_store_verification_streams_without_retaining_payload_projection(
    tmp_path: Path,
) -> None:
    with EmbeddedStore(
        tmp_path, session=_session(), verify_on_open=False
    ) as store:
        for index in range(8):
            result = store.put_bound_object(
                domain_schema=b"test.domain/1",
                domain_identity=f"stream-{index}".encode(),
                descriptor=b"test-descriptor/1",
                payload=bytes([index]) * 65_537,
                expected_generation=index,
            )
            assert result.code is StoreResultCode.COMMITTED

        result = store.verify()
        assert result["objects"] == 8
        assert store._verified_generation == store.current_generation
        assert store._projection_generation is None
        assert store.resident_status()["verified_object_projection_entries"] == 0

    with EmbeddedStore(tmp_path, session=_session()) as reopened:
        assert reopened._verified_generation == reopened.current_generation
        assert reopened._projection_generation is None
        assert reopened.verify()["objects"] == 8
        assert reopened.resident_status()["verified_object_projection_entries"] == 0


def test_publication_extends_verified_projection_without_rereading_old_objects(
    tmp_path: Path,
) -> None:
    session = _session()
    with EmbeddedStore(tmp_path, session=session) as store:
        store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"first",
            descriptor=b"test-descriptor/1",
            payload=b"first-payload",
            expected_generation=0,
        )
        store.current_objects()
        store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"second",
            descriptor=b"test-descriptor/1",
            payload=b"second-payload",
            expected_generation=1,
        )
        after_publication = session.hash_count
        assert [item.domain_identity for item in store.current_objects()] == [b"first", b"second"]
        assert session.hash_count == after_publication


def test_domain_prefix_query_verifies_only_matching_payloads(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        store.put_bound_object(
            domain_schema=b"session.snapshot/1",
            domain_identity=b"session-a:snap:0001",
            descriptor=b"test-descriptor/1",
            payload=b"selected snapshot",
            expected_generation=0,
        )
        store.put_bound_object(
            domain_schema=b"unrelated.record/1",
            domain_identity=b"other",
            descriptor=b"test-descriptor/1",
            payload=b"unrelated payload",
            expected_generation=1,
        )

    unrelated_chunk = tmp_path / "chunks" / f"{hashlib.sha256(b'unrelated payload').hexdigest()}.chunk"
    unrelated_chunk.write_bytes(b"corrupted payload")

    with EmbeddedStore(tmp_path, session=_session(), verify_on_open=False) as reopened:
        matches = reopened.find_bound_objects(b"session.snapshot/1", b"session-a:snap:")
        assert [(item.domain_identity, item.payload) for item in matches] == [
            (b"session-a:snap:0001", b"selected snapshot")
        ]
        with pytest.raises(StoreIntegrityError):
            reopened.current_objects()


def test_publication_validates_selectively_without_rereading_unrelated_payloads(
    tmp_path: Path,
) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"first",
            descriptor=b"test-descriptor/1",
            payload=b"first-payload",
            expected_generation=0,
        )
        store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"second",
            descriptor=b"test-descriptor/1",
            payload=b"second-payload",
            expected_generation=1,
        )

    unrelated_chunk = tmp_path / "chunks" / f"{hashlib.sha256(b'first-payload').hexdigest()}.chunk"
    unrelated_chunk.write_bytes(b"corrupted payload")

    with EmbeddedStore(tmp_path, session=_session(), verify_on_open=False) as reopened:
        result = reopened.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"third",
            descriptor=b"test-descriptor/1",
            payload=b"third-payload",
            expected_generation=2,
        )
        assert result.code is StoreResultCode.COMMITTED
        matches = reopened.find_bound_objects(b"test.domain/1", b"third")
        assert [(item.domain_identity, item.payload) for item in matches] == [
            (b"third", b"third-payload")
        ]
        with pytest.raises(StoreIntegrityError):
            reopened.verify()


def test_generation_scoped_domain_index_advances_across_small_publications(
    tmp_path: Path,
) -> None:
    """Exact lookups after publication reuse the verified metadata prefix."""
    with EmbeddedStore(
        tmp_path, session=_session(), verify_on_open=False
    ) as store:
        first = store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"first",
            descriptor=b"test-descriptor/1",
            payload=b"first-payload",
            expected_generation=0,
        )
        assert first.code is StoreResultCode.COMMITTED
        assert store._domain_index_generation == 1
        assert len(store._domain_index) == 1

        assert [item.domain_identity for item in store.find_bound_objects(
            b"test.domain/1", b"first"
        )] == [b"first"]
        second = store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"second",
            descriptor=b"test-descriptor/1",
            payload=b"second-payload",
            expected_generation=1,
        )
        assert second.code is StoreResultCode.COMMITTED
        assert store._domain_index_generation == 2
        assert len(store._domain_index) == 2
        assert [item.domain_identity for item in store.find_bound_objects(
            b"test.domain/1", b"second"
        )] == [b"second"]

        batch = store.put_bound_objects(
            [
                BoundObjectInput(b"test.domain/1", b"third", b"test-descriptor/1", b"third-payload"),
                BoundObjectInput(b"test.domain/1", b"fourth", b"test-descriptor/1", b"fourth-payload"),
            ],
            expected_generation=2,
        )
        assert batch.code is StoreResultCode.COMMITTED
        assert store._domain_index_generation == 3
        assert len(store._domain_index) == 4
        assert [item.domain_identity for item in store.find_bound_objects(
            b"test.domain/1", b"fourth"
        )] == [b"fourth"]


def test_exact_bound_lookup_reads_only_the_selected_binding(tmp_path: Path) -> None:
    with EmbeddedStore(
        tmp_path, session=_session(), verify_on_open=False
    ) as store:
        for index in range(40):
            result = store.put_bound_object(
                domain_schema=b"test.domain/1",
                domain_identity=f"record-{index}".encode(),
                descriptor=b"test-descriptor/1",
                payload=f"payload-{index}".encode(),
                expected_generation=index,
            )
            assert result.code is StoreResultCode.COMMITTED

        original_read_binding = store._read_binding
        binding_reads = 0

        def count_binding_reads(*args, **kwargs):
            nonlocal binding_reads
            binding_reads += 1
            return original_read_binding(*args, **kwargs)

        store._read_binding = count_binding_reads
        selected = store.get_bound_object(b"test.domain/1", b"record-17")
        assert selected.payload == b"payload-17"
        # The selected binding is checked directly, then checked again when
        # its payload tree is opened; no other record's binding is read.
        assert binding_reads == 2
        assert store._entry_index_generation == store.current_generation


def test_domain_binding_replay_compares_two_snapshots(tmp_path: Path) -> None:
    with EmbeddedStore(
        tmp_path, session=_session(), verify_on_open=False
    ) as store:
        for index in range(4):
            result = store.put_bound_object(
                domain_schema=b"test.domain/1",
                domain_identity=f"record-{index}".encode(),
                descriptor=b"test-descriptor/1",
                payload=f"payload-{index}".encode(),
                expected_generation=index,
            )
            assert result.code is StoreResultCode.COMMITTED

        original_read_binding = store._read_binding
        binding_reads = 0
        original_read_generation = store._read_generation
        generation_reads = 0

        def count_binding_reads(*args, **kwargs):
            nonlocal binding_reads
            binding_reads += 1
            return original_read_binding(*args, **kwargs)

        store._read_binding = count_binding_reads
        def count_generation_reads(*args, **kwargs):
            nonlocal generation_reads
            generation_reads += 1
            return original_read_generation(*args, **kwargs)

        store._read_generation = count_generation_reads
        assert store.domain_bindings_since(2) == (
            (3, b"test.domain/1", b"record-2"),
            (4, b"test.domain/1", b"record-3"),
        )
        assert binding_reads == 2
        assert generation_reads == 2

        with pytest.raises(StoreError):
            store.domain_bindings_since(0, max_generations=2)


def test_generation_bound_lookup_maps_follow_publication_and_recovery(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        first = store.put_bound_object(
            domain_schema=b"schema/a",
            domain_identity=b"shared",
            descriptor=b"test-descriptor/1",
            payload=b"first",
            expected_generation=0,
        )
        second = store.put_bound_object(
            domain_schema=b"schema/b",
            domain_identity=b"shared",
            descriptor=b"test-descriptor/1",
            payload=b"second",
            expected_generation=1,
        )
        assert store.object_for_binding(first.binding_id).payload == b"first"
        assert store.object_for_binding(second.binding_id).payload == b"second"
        assert store.logical_id_for_domain(b"schema/a", b"shared") == first.logical_id
        assert store.logical_id_for_domain(b"schema/b", b"shared") == second.logical_id
        assert store.logical_ids_for_domain_identity(b"shared") == (
            first.logical_id,
            second.logical_id,
        )

    with EmbeddedStore(tmp_path, session=_session()) as reopened:
        assert reopened.object_for_binding(first.binding_id).logical_id == first.logical_id
        assert reopened.logical_id_for_domain(b"schema/b", b"shared") == second.logical_id
        assert reopened.logical_ids_for_domain_identity(b"shared") == (
            first.logical_id,
            second.logical_id,
        )


def test_objects_at_returns_verified_historical_generation(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        store.put_bound_object(
            domain_schema=b"history/1",
            domain_identity=b"first",
            descriptor=b"history-descriptor/1",
            payload=b"first payload",
            expected_generation=0,
        )
        first_generation = store.current_generation
        store.put_bound_object(
            domain_schema=b"history/1",
            domain_identity=b"second",
            descriptor=b"history-descriptor/1",
            payload=b"second payload",
            expected_generation=first_generation,
        )

        historical = store.objects_at(first_generation)
        assert [(item.domain_identity, item.payload) for item in historical] == [
            (b"first", b"first payload")
        ]
        assert [item.domain_identity for item in store.objects_at(store.current_generation)] == [
            b"first",
            b"second",
        ]
        with pytest.raises(StoreError) as future:
            store.objects_at(store.current_generation + 1)
        assert future.value.code is StoreResultCode.DENIED


def test_batch_publication_commits_all_objects_in_one_generation(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        result = store.put_bound_objects(
            [
                BoundObjectInput(
                    b"batch/1",
                    b"one",
                    b"descriptor/1",
                    b"payload-one",
                ),
                BoundObjectInput(
                    b"batch/1",
                    b"two",
                    b"descriptor/1",
                    b"payload-two",
                ),
            ],
            expected_generation=0,
        )
        assert result.code is StoreResultCode.COMMITTED
        assert result.generation == 1
        assert len(result.commits) == 2
        assert {item.generation for item in store.current_objects()} == {1}
        assert [item.domain_identity for item in store.current_objects()] == [b"one", b"two"]


def test_delta_generations_preserve_snapshots_with_bounded_checkpoints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import embedded as embedded_module

    monkeypatch.setattr(embedded_module, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with EmbeddedStore(tmp_path, session=_session()) as store:
        for index in range(1, 6):
            result = store.put_bound_object(
                domain_schema=b"fixture/1",
                domain_identity=f"object-{index}".encode(),
                descriptor=b"fixture-descriptor/1",
                payload=f"payload-{index}".encode(),
                expected_generation=index - 1,
            )
            assert result.committed

        generations = tmp_path / "generations"
        assert (generations / f"{1:016x}").read_bytes()[:2] == b"MD"
        assert (generations / f"{4:016x}").read_bytes()[:2] == b"MG"
        delta = (generations / f"{5:016x}").read_bytes()
        assert delta[:2] == b"MD"
        entries, relations, provenance = store._read_generation_parts(5)
        full = store._generation_bytes(5, entries, relations, provenance)
        assert len(delta) < len(full) // 2
        assert store.domain_bindings_at(2) == (
            (b"fixture/1", b"object-1"),
            (b"fixture/1", b"object-2"),
        )
        assert [item.domain_identity for item in store.objects_at(3)] == [
            b"object-1", b"object-2", b"object-3"]
        assert len(store.commit_feed(5)) > 0

    with EmbeddedStore(tmp_path, session=_session(), read_only=True) as reopened:
        assert reopened.current_generation == 5
        assert [item.domain_identity for item in reopened.current_objects()] == [
            b"object-1", b"object-2", b"object-3", b"object-4", b"object-5"]
        assert reopened.domain_bindings_at(1) == ((b"fixture/1", b"object-1"),)


def test_typed_relations_and_provenance_are_generation_bound(tmp_path: Path) -> None:
    session = _session()
    with EmbeddedStore(tmp_path, session=session) as store:
        provenance = store.make_provenance(
            source=b"source-id"[:12].ljust(12, b"-"),
            producer=b"producer-id"[:12].ljust(12, b"-"),
            transformation=b"transform-id"[:12].ljust(12, b"-"),
            generation=1,
            evidence=hashlib.sha256(b"payload").digest(),
            ancestry=hashlib.sha256(b"gen-0").digest(),
        )
        relation = store.make_relation(
            relation_type=hashlib.sha256(b"test-relation/1").digest(),
            source=b"source-id"[:12].ljust(12, b"-"),
            target=b"target-id"[:12].ljust(12, b"-"),
            generation=1,
            provenance=hashlib.sha256(provenance).digest(),
            ordinal=0,
        )
        result = store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"typed",
            descriptor=b"test-descriptor/1",
            payload=b"payload",
            expected_generation=0,
            relations=[relation],
            provenance=[provenance],
        )
        assert result.code is StoreResultCode.COMMITTED
        assert store.typed_records("relations") == [relation]
        assert store.typed_records("provenance") == [provenance]
        assert store.verify()["relations"] == 1
        assert store.verify()["provenance_records"] == 1

    with EmbeddedStore(tmp_path, session=_session()) as reopened:
        assert reopened.typed_records("relations") == [relation]
        assert reopened.typed_records("provenance") == [provenance]


def test_interior_chunk_corruption_fails_closed(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        store.put_bound_object(
            domain_schema=b"test.domain/1",
            domain_identity=b"corrupt",
            descriptor=b"test-descriptor/1",
            payload=b"x" * 140_000,
            expected_generation=0,
        )
    interior = sorted((tmp_path / "chunks").glob("*.chunk"))[1]
    original = interior.read_bytes()
    interior.write_bytes(b"z" * len(original))
    try:
        with EmbeddedStore(tmp_path, session=_session(), verify_on_open=False) as reopened:
            with pytest.raises(StoreError) as issue:
                reopened.verify()
            assert issue.value.code is StoreResultCode.INTEGRITY_FAILURE
    finally:
        interior.write_bytes(original)


@pytest.mark.parametrize(
    "failpoint",
    [
        "during_chunk_staging",
        "after_content_durability",
        "during_structural_metadata_staging",
        "after_binding_durability",
        "before_generation_publication",
        "after_generation_publication",
        "after_head_replace_before_directory_sync",
        "during_head_publication",
        "after_head_publication",
        "during_cleanup",
    ],
)
def test_fault_points_recover_old_or_new(tmp_path: Path, failpoint: str) -> None:
    def inject(name: str) -> None:
        if name == failpoint:
            raise CrashInjected(name)

    with pytest.raises(CrashInjected):
        with EmbeddedStore(tmp_path, session=_session(), failpoint=inject) as store:
            store.put_bound_object(
                domain_schema=b"test.domain/1",
                domain_identity=b"fault",
                descriptor=b"test-descriptor/1",
                payload=b"fault" * 300,
                expected_generation=0,
            )

    with EmbeddedStore(tmp_path, session=_session()) as recovered:
        assert recovered.current_generation in {0, 1}
        assert recovered.recovery_result in {
            StoreResultCode.RECOVERED_OLD,
            StoreResultCode.RECOVERED_NEW,
        }
        if recovered.current_generation == 1:
            assert recovered.current_objects()[0].payload == b"fault" * 300
        else:
            assert recovered.current_objects() == []


@pytest.mark.parametrize(
    "failpoint",
    [
        "during_chunk_staging",
        "after_content_durability",
        "during_structural_metadata_staging",
        "after_binding_durability",
        "before_generation_publication",
        "after_generation_publication",
        "after_head_replace_before_directory_sync",
        "during_head_publication",
        "after_head_publication",
        "during_cleanup",
    ],
)
def test_batch_fault_points_recover_old_or_new(tmp_path: Path, failpoint: str) -> None:
    def inject(name: str) -> None:
        if name == failpoint:
            raise CrashInjected(name)

    batch = [
        BoundObjectInput(b"batch/1", b"one", b"descriptor/1", b"payload-one"),
        BoundObjectInput(b"batch/1", b"two", b"descriptor/1", b"payload-two"),
    ]
    with pytest.raises(CrashInjected):
        with EmbeddedStore(tmp_path, session=_session(), failpoint=inject) as store:
            store.put_bound_objects(batch, expected_generation=0)

    with EmbeddedStore(tmp_path, session=_session()) as recovered:
        assert recovered.current_generation in {0, 1}
        assert recovered.recovery_result in {
            StoreResultCode.RECOVERED_OLD,
            StoreResultCode.RECOVERED_NEW,
        }
        if recovered.current_generation == 1:
            assert [item.payload for item in recovered.current_objects()] == [
                b"payload-one",
                b"payload-two",
            ]
        else:
            assert recovered.current_objects() == []


def test_real_processes_allow_one_expected_generation_commit(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()):
        pass
    context = multiprocessing.get_context("fork")
    output = context.Queue()
    processes = [
        context.Process(target=_race_worker, args=(str(tmp_path), index, output))
        for index in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    results = []
    for _ in processes:
        try:
            results.append(output.get(timeout=2))
        except Empty as exc:
            raise AssertionError("race worker did not return a typed result") from exc
    assert sorted(results) == ["COMMITTED", "STALE_GENERATION"]
    with EmbeddedStore(tmp_path, session=_session()) as reopened:
        assert reopened.current_generation == 1
        assert len(reopened.current_objects()) == 1


def test_binding_identity_feed_reads_committed_history_without_payload_materialization(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        store.put_bound_object(domain_schema=b'fixture/1', domain_identity=b'first',
            descriptor=b'fixture-descriptor/1', payload=b'first payload', expected_generation=0)
        first = store.current_generation
        store.put_bound_object(domain_schema=b'fixture/1', domain_identity=b'second',
            descriptor=b'fixture-descriptor/1', payload=b'second payload', expected_generation=first)
    with EmbeddedStore(tmp_path, session=_session(), read_only=True) as reader:
        reader._read_entry = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('feed read payload'))
        assert reader.domain_bindings_at(first) == ((b'fixture/1', b'first'),)
        assert reader.domain_bindings_at(reader.current_generation) == ((b'fixture/1', b'first'), (b'fixture/1', b'second'))
        for invalid in (True, -1, reader.current_generation + 1):
            with pytest.raises(StoreError):
                reader.domain_bindings_at(invalid)


def test_binding_identity_feed_cannot_hide_corrupt_binding_metadata(tmp_path: Path) -> None:
    with EmbeddedStore(tmp_path, session=_session()) as store:
        result = store.put_bound_object(domain_schema=b'fixture/1', domain_identity=b'first',
            descriptor=b'fixture-descriptor/1', payload=b'first payload', expected_generation=0)
    binding = next((tmp_path / 'bindings').iterdir())
    binding.write_bytes(b'corrupt binding')
    with EmbeddedStore(tmp_path, session=_session(), read_only=True) as reader:
        with pytest.raises(StoreIntegrityError):
            reader.domain_bindings_at(reader.current_generation)


def test_generation_prune_reclaims_below_checkpoint_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import embedded as embedded_module

    monkeypatch.setattr(embedded_module, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with EmbeddedStore(tmp_path, session=_session()) as store:
        for index in range(1, 11):
            result = store.put_bound_object(
                domain_schema=b"fixture/1",
                domain_identity=f"object-{index}".encode(),
                descriptor=b"fixture-descriptor/1",
                payload=f"payload-{index}".encode(),
                expected_generation=index - 1,
            )
            assert result.committed
        assert store.current_generation == 10
        generations = tmp_path / "generations"
        assert (generations / f"{4:016x}").read_bytes()[:2] == b"MG"

        # Retention floor is the checkpoint at or below head - keep_last.
        assert store.retention_floor(keep_last=4) == 4
        inventory = store.generation_inventory(keep_last=4)
        assert inventory["head"] == 10
        assert inventory["floor"] == 4
        assert inventory["floor_is_checkpoint"] is True
        assert inventory["prunable_files"] == 3
        assert inventory["prunable_bytes"] > 0

        # Dry runs classify without deleting.
        preview = store.prune_generations(keep_last=4, dry_run=True)
        assert preview["removed_files"] == 0
        assert preview["prunable_files"] == 3
        assert (generations / f"{1:016x}").exists()

        pruned = store.prune_generations(keep_last=4, dry_run=False)
        assert pruned["removed_files"] == 3
        assert pruned["removed_bytes"] == inventory["prunable_bytes"]
        assert pruned["floor"] == 4
        assert pruned["head"] == 10
        # The genesis anchor survives; only (genesis, floor) is reclaimed.
        assert (generations / f"{0:016x}").exists()
        for generation in range(1, 4):
            assert not (generations / f"{generation:016x}").exists()
        for generation in range(4, 11):
            assert (generations / f"{generation:016x}").exists()

        # The retained window still replays: floor reads, cursor reads read,
        # and the bounded delta feed covers (cursor, head].
        assert [item.domain_identity for item in store.objects_at(4)] == [
            f"object-{index}".encode() for index in range(1, 5)]
        assert [item.domain_identity for item in store.objects_at(10)] == [
            f"object-{index}".encode() for index in range(1, 11)]
        assert store.domain_bindings_since(6) == tuple(
            (generation, b"fixture/1", f"object-{generation}".encode())
            for generation in range(7, 11)
        )
        assert store.generation_inventory(keep_last=4)["prunable_files"] == 0

    with EmbeddedStore(tmp_path, session=_session(), read_only=True) as reopened:
        assert reopened.current_generation == 10
        assert len(reopened.current_objects()) == 10
        with pytest.raises(StoreError):
            reopened.prune_generations(keep_last=4, dry_run=False)


def test_generation_prune_is_noop_inside_retention_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mncs_store import embedded as embedded_module

    monkeypatch.setattr(embedded_module, "GENERATION_CHECKPOINT_INTERVAL", 4)
    with EmbeddedStore(tmp_path, session=_session()) as store:
        store.put_bound_object(
            domain_schema=b"fixture/1",
            domain_identity=b"only",
            descriptor=b"fixture-descriptor/1",
            payload=b"payload",
            expected_generation=0,
        )
        assert store.retention_floor(keep_last=4096) == 0
        pruned = store.prune_generations(keep_last=4096, dry_run=False)
        assert pruned["removed_files"] == 0
        assert (tmp_path / "generations" / f"{0:016x}").exists()
        for invalid in (True, 0, -1):
            with pytest.raises(StoreError):
                store.prune_generations(keep_last=invalid, dry_run=True)
