"""Focused obligations for the machine-native Store substrate.

This file is intentionally separate from the broad historical matrix.  It
is an ordinary canonical regression for fixed relation/provenance/feed
representations and a bounded Store reopen path; scale and all-backend
matrices remain scheduled obligations.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from mncs_exec import (
    B,
    BYTES,
    U64,
    as_bool,
    as_bytes,
    as_int,
    call_many,
    require_returned,
)
from retained_session import RetainedEngine
from store_phase2 import StorePhase2


RESEARCH = "mncs-research-bytecode"


def _relation_args():
    return [
        U64(2),
        BYTES(bytes(range(12))),
        BYTES(bytes(range(12, 24))),
        U64(7),
        BYTES(bytes(range(32))),
        U64(9),
    ]


def test_relationship_and_provenance_records_are_typed_and_inspectable():
    relation_calls = []
    for kind in range(1, 5):
        args = _relation_args()
        args[0] = U64(kind)
        relation_calls.append((f"roundtrip-{kind}", "roundtrip_fields", args))
    out = call_many(
        "src/store/relationship.mncs",
        "store.relationship.v1",
        relation_calls + [("encode", "encode_fields", _relation_args())],
        RESEARCH,
    )
    assert all(
        as_bool(require_returned(out[f"roundtrip-{kind}"], "relation roundtrip"))
        for kind in range(1, 5)
    )
    relation = as_bytes(require_returned(out["encode"], "relation encode"))
    assert len(relation) == 80
    assert relation[:8] == bytes.fromhex("4d52010002000000")

    p_args = [
        BYTES(bytes(range(12))),
        BYTES(bytes(range(12, 24))),
        BYTES(bytes(range(24, 36))),
        U64(7),
        BYTES(bytes(range(32))),
        BYTES(bytes(reversed(range(32)))),
    ]
    out = call_many(
        "src/store/provenance.mncs",
        "store.provenance.v1",
        [("roundtrip", "roundtrip_fields", p_args),
         ("encode", "encode_fields", p_args)],
        RESEARCH,
    )
    assert as_bool(require_returned(out["roundtrip"], "provenance roundtrip"))
    provenance = as_bytes(require_returned(out["encode"], "provenance encode"))
    assert len(provenance) == 112
    assert provenance[:4] == bytes.fromhex("4d500100")


def test_commit_feed_reports_generation_freshness():
    root = bytes(range(32))
    out = call_many(
        "src/store/commit_feed.mncs",
        "store.commit_feed.v1",
        [
            ("roundtrip", "roundtrip_fields",
             [U64(7), U64(3), U64(4), U64(2), BYTES(root)]),
            ("equal", "through", [U64(7), U64(7)]),
            ("stale", "through", [U64(7), U64(8)]),
            ("future", "through", [U64(9), U64(8)]),
        ],
        RESEARCH,
    )
    assert as_bool(require_returned(out["roundtrip"], "feed roundtrip"))
    assert as_int(require_returned(out["equal"], "equal freshness")) == 0
    assert as_int(require_returned(out["stale"], "stale freshness")) == 1
    assert as_int(require_returned(out["future"], "future freshness")) == 2


def test_relation_object_survives_store_commit_close_and_reopen(tmp_path: Path):
    engine = RetainedEngine()
    relation_args = [
        U64(2),
        BYTES(bytes(range(12))),
        BYTES(bytes(range(12, 24))),
        U64(1),
        BYTES(bytes(range(32))),
        U64(0),
    ]
    relation = as_bytes(
        require_returned(
            engine.run(
                "src/store/relationship.mncs",
                "store.relationship.v1",
                [("relation", "encode_fields", relation_args)],
            )["relation"],
            "relation encode",
        )
    )
    provenance_args = [
        BYTES(bytes(range(12))),
        BYTES(bytes(range(12, 24))),
        BYTES(bytes(range(24, 36))),
        U64(1),
        BYTES(bytes(range(32))),
        BYTES(bytes(reversed(range(32)))),
    ]
    provenance = as_bytes(
        require_returned(
            engine.run(
                "src/store/provenance.mncs",
                "store.provenance.v1",
                [("provenance", "encode_fields", provenance_args)],
            )["provenance"],
            "provenance encode",
        )
    )
    store = StorePhase2.create(tmp_path / "store", engine=engine)
    try:
        oid = store.put_blob(relation)
        assert store.get_blob(oid) == relation
        assert store.verify(oid)
        assert store.publication_trace[-1]["decision"] == 0
        feed = as_bytes(
            require_returned(
                engine.run(
                    "src/store/commit_feed.mncs",
                    "store.commit_feed.v1",
                    [("feed", "encode_fields", [U64(1), U64(1), U64(1), U64(1), BYTES(bytes(32))])],
                )["feed"],
                "feed encode",
            )
        )
        store.persist_typed_commit(feed, [relation], [provenance])
        assert store.typed_records("relations") == [relation]
        assert store.typed_records("provenance") == [provenance]
        assert store.typed_records("feeds") == [feed]
    finally:
        store.close()

    reopened = StorePhase2.open(tmp_path / "store", engine=engine)
    try:
        assert reopened.get_blob(oid) == relation
        assert reopened.verify(oid)
        assert reopened.typed_records("relations") == [relation]
        assert reopened.typed_records("provenance") == [provenance]
        assert reopened.typed_records("feeds") == [feed]
    finally:
        reopened.close()
        engine.close()


def test_publication_policy_rejects_incomplete_candidate():
    out = call_many(
        "src/store/publication.mncs",
        "store.publication.v1",
        [("reject", "publication_admit",
          [B(True), B(False), B(True), B(True)])],
        RESEARCH,
    )
    assert as_int(require_returned(out["reject"], "publication rejection")) == 1


def test_retained_session_reuses_one_admitted_artifact():
    engine = RetainedEngine()
    calls = [
        ("admit", "publication_admit", [B(True), B(True), B(True), B(True)]),
        ("reject", "publication_admit", [B(True), B(False), B(True), B(True)]),
    ]
    try:
        first = engine.run(
            "src/store/publication.mncs",
            "store.publication.v1",
            [calls[0]],
        )
        second = engine.run(
            "src/store/publication.mncs",
            "store.publication.v1",
            [calls[1]],
        )
        assert as_int(require_returned(first["admit"], "retained admission")) == 0
        assert as_int(require_returned(second["reject"], "retained rejection")) == 1
        metrics = engine.metrics()
        assert metrics["session_count"] == 1
        assert metrics["semantic_batch_count"] == 2
        assert metrics["semantic_call_count"] == 2
        assert metrics["cold_admission_seconds"] >= metrics["session_open_seconds"]
    finally:
        engine.close()


def test_native_publication_effect_is_explicitly_granted():
    engine = RetainedEngine()
    with TemporaryDirectory(prefix="mncs-store-publish-") as root:
        try:
            result = engine.run(
                "src/store/publication.mncs",
                "store.publication.v1",
                [
                    (
                        "publish",
                        "publish",
                        [BYTES(b"stage"), BYTES(b"final"), BYTES(b"typed")],
                    )
                ],
                grants=["--grant-fs", f"fs_root={root}"],
            )
            entry = require_returned(result["publish"], "native publication")
            assert as_int(entry) >= 0
            assert Path(root, "final").read_bytes() == b"typed"
        finally:
            engine.close()
