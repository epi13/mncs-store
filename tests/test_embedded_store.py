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
from mncs_store.embedded import CrashInjected


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
