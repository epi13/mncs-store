"""Phase 2 tests: multi-chunk objects, generations, snapshots, recovery.

Every test runs the real on-disk layout (tmp_path) with all semantic
bytes AND all semantic decisions produced by mncs-language executions
(see store_phase2.py for the host/MNCS responsibility split). Host-side
faults simulate crashes and corruption; each must surface as an explicit
ConflictError/IntegrityError/NotFoundError/SnapshotExpired, never silent
data and never a mixed generation.
"""

import os

import pytest

from mncs_exec import U64, as_int, require_returned
from store_phase1a import (
    Engine,
    IntegrityError,
    NotFoundError,
    StorePhase1a,
    UnsupportedWidthError,
)
from store_phase2 import (
    REFERENCE_GENERAL_BLOB_LIMIT,
    ConflictError,
    FaultInjected,
    SnapshotExpired,
    StorePhase2,
    _tail_width,
)


def crash_reopen(path, engine):
    """Unopened instance observing only on-disk state (post-crash).

    recover() reads meta/current/generation files directly and makes no
    use of carried state, exactly like a new process after a crash.
    """
    s = StorePhase2.__new__(StorePhase2)
    StorePhase1a.__init__(s, path, engine)
    s._retained = set()
    s.trace = []
    return s


@pytest.fixture
def engine():
    return Engine()


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "store")


def reopen(path, engine):
    """Fresh instance with no carried state (process-restart equivalent)."""
    return StorePhase2.open(path, engine)


def blob(n, seed=0):
    return bytes((seed + i) % 256 for i in range(n))


# -- multi-chunk boundaries -------------------------------------------------

BOUNDARY_SIZES = [0, 1, 4, 5, 8, 9, 16, 17, 31, 32, 33, 40, 63, 64, 65,
                  96, 128, 256, 512, 959, 960, 961, 992]


def test_general_blob_boundaries_roundtrip(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    items = [(n, blob(n, seed=n)) for n in BOUNDARY_SIZES]
    oids = s.put_general_many(items)
    assert len({o.hex() for o in oids}) == len(items)
    for (n, want), oid in zip(items, oids):
        assert s.get_blob(oid) == want, f"size {n} mismatch before close"
    gen = s.generation()
    s.close()
    s2 = reopen(store_path, engine)
    assert s2.generation() == gen
    for (n, want), oid in zip(items, oids):
        assert s2.verify(oid) is True
        assert s2.get_blob(oid) == want, f"size {n} mismatch after reopen"


def test_over_ceiling_rejected_without_truncation(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    with pytest.raises(UnsupportedWidthError):
        s.put_blob(bytes(REFERENCE_GENERAL_BLOB_LIMIT + 1))
    # The failed put committed nothing.
    assert s.generation() == 0


def test_tail_width_host_mirror_matches_mncs(store_path, engine):
    """The host width selector must agree with MNCS tail_width_for 0..33."""
    s = StorePhase2.create(store_path, engine)
    out = s._mncs("src/store/chunk.mncs", "store.chunk.v1",
                  [(f"w{r}", "tail_width_for", [U64(r)]) for r in range(34)])
    for r in range(34):
        mncs_w = as_int(require_returned(out[f"w{r}"], "width"))
        host_w = 0 if r == 0 or r > 32 else _tail_width(r)
        assert mncs_w == host_w, f"remainder {r}: mncs={mncs_w} host={host_w}"


def test_identical_content_shares_chunks_without_merging(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    data = blob(100, seed=7)
    o1 = s.put_blob(data)
    n_chunks = len(s.chunk_files())
    assert n_chunks == 4  # 3 body + 1 tail(4B)
    o2 = s.put_blob(data)
    assert o1 != o2
    assert len(s.chunk_files()) == n_chunks  # structural sharing
    e1 = s.load_committed2(o1)
    e2 = s.load_committed2(o2)
    assert e1["manifest"] == e2["manifest"]  # identical content, one root
    assert s.get_blob(o1) == data
    assert s.get_blob(o2) == data


def test_overlapping_blobs_share_body_chunks(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    prefix = blob(64, seed=1)
    o1 = s.put_blob(prefix + bytes(36))
    n1 = len(s.chunk_files())
    o2 = s.put_blob(prefix + bytes([9] * 36))
    # First two body chunks shared; tails differ.
    assert len(s.chunk_files()) == n1 + 2
    assert s.get_blob(o1) == prefix + bytes(36)
    assert s.get_blob(o2) == prefix + bytes([9] * 36)


# -- multi-chunk integrity ---------------------------------------------------

def _chunk_path(store_path, entry, i=0):
    return os.path.join(store_path, "chunks", entry["digests"][i].hex())


def test_reordered_manifest_rejected(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    oid = s.put_blob(blob(70, seed=3))
    entry = s.load_committed2(oid)
    assert len(entry["digests"]) == 3
    man = bytearray(entry["manifest"])
    man[28:60], man[60:92] = man[60:92], man[28:60]  # swap first two digests
    with open(os.path.join(store_path, "objects", oid.hex()), "wb") as f:
        f.write(bytes(man))
    with pytest.raises(IntegrityError):
        s.get_blob(oid)


def test_corrupt_body_chunk_rejected(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    oid = s.put_blob(blob(70, seed=3))
    entry = s.load_committed2(oid)
    path = _chunk_path(store_path, entry, 0)
    with open(path, "r+b") as f:
        data = bytearray(f.read())
        data[10] ^= 0xFF
        f.seek(0)
        f.write(bytes(data))
    with pytest.raises(IntegrityError):
        s.get_blob(oid)


def test_missing_tail_chunk_is_corruption_not_notfound(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    oid = s.put_blob(blob(70, seed=3))
    entry = s.load_committed2(oid)
    os.remove(_chunk_path(store_path, entry, len(entry["digests"]) - 1))
    with pytest.raises(IntegrityError):
        s.get_blob(oid)
    # Unknown OBJECTS are still NotFound (kept distinct from corruption).
    with pytest.raises(NotFoundError):
        s.get_blob(bytes(12))


def test_truncated_and_magic_flipped_manifests_rejected(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    oid = s.put_blob(blob(70, seed=3))
    obj_path = os.path.join(store_path, "objects", oid.hex())
    with open(obj_path, "rb") as f:
        good = f.read()
    with open(obj_path, "wb") as f:
        f.write(good[:60])  # torn manifest
    with pytest.raises(IntegrityError):
        s.get_blob(oid)
    with open(obj_path, "wb") as f:
        bad = bytearray(good)
        bad[0] = 0x58
        f.write(bytes(bad))
    with pytest.raises(IntegrityError):
        s.get_blob(oid)


def test_noncanonical_tail_padding_rejected(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    oid = s.put_blob(blob(40, seed=11))  # 32 + tail 8
    entry = s.load_committed2(oid)
    tail_path = _chunk_path(store_path, entry, 1)
    with open(tail_path, "r+b") as f:
        data = bytearray(f.read())
        # Payload byte at tail index 8 is padding (true tail len 8 of
        # width 8 -> no padding here); corrupt the last TRUE byte instead
        # of padding: digest mismatch path. For the padding path, use a
        # 33-byte object (tail 1 of width 4, 3 padding bytes).
        f.seek(0)
        f.write(bytes(data))
    oid2 = s.put_blob(blob(33, seed=5))
    entry2 = s.load_committed2(oid2)
    tail2 = _chunk_path(store_path, entry2, 1)
    with open(tail2, "r+b") as f:
        data = bytearray(f.read())
        assert len(data) == 8  # frame: 4 header + 4 payload
        data[7] = 0xFF  # last padding byte must stay zero
        f.seek(0)
        f.write(bytes(data))
    with pytest.raises(IntegrityError):
        s.get_blob(oid2)


# -- generations and compare-and-transition ----------------------------------

def test_generations_and_stale_writer_conflict(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    assert s.generation() == 0
    o1 = s.put_blob(blob(10, seed=1))
    assert s.generation() == 1
    # Valid successor: expected == current.
    o2 = s.cas_put_blob(1, blob(10, seed=2))
    assert s.generation() == 2
    # Stale writer: expected 1, current 2 -> typed conflict, no commit.
    with pytest.raises(ConflictError) as exc:
        s.cas_put_blob(1, blob(10, seed=3))
    assert exc.value.observed == 2
    assert exc.value.attempted == 1
    assert len(exc.value.token) == 16
    assert exc.value.token[0:2] == b"CF"
    assert s.generation() == 2
    # Both committed objects still read.
    assert s.get_blob(o1) == blob(10, seed=1)
    assert s.get_blob(o2) == blob(10, seed=2)
    # The conflicted bytes were never staged: unknown to the store.
    s.close()
    s2 = reopen(store_path, engine)
    assert s2.generation() == 2


def test_repeated_commit_same_content_new_generation(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    o1 = s.put_blob(blob(50, seed=4))
    o2 = s.put_blob(blob(50, seed=4))
    assert o1 != o2
    assert s.generation() == 2
    assert s.load_committed2(o1)["root_hex"] == \
        s.load_committed2(o2)["root_hex"]


def test_historical_generation_files_immutable(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    s.put_blob(blob(10, seed=1))
    with open(os.path.join(store_path, "generations", "00000001"), "rb") as f:
        gen1 = f.read()
    s.put_blob(blob(20, seed=2))
    with open(os.path.join(store_path, "generations", "00000001"), "rb") as f:
        assert f.read() == gen1
    assert s.generation() == 2


def test_commit_trace_reaches_published(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    s.put_blob(blob(33, seed=6))
    assert s.trace == [(0, True, 1), (1, True, 2), (2, True, 3),
                       (3, True, 4)], f"unexpected commit trace: {s.trace}"


def test_v1_and_v2_objects_coexist(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    ou = s.put_u32(0x01020304)
    ob = s.put_blob(blob(100, seed=8))
    assert s.generation() == 2
    assert s.verify_all() == 2
    assert s.get_u32(ou) == 0x01020304
    assert s.get_blob(ob) == blob(100, seed=8)
    s.close()
    s2 = reopen(store_path, engine)
    assert s2.get_u32(ou) == 0x01020304
    assert s2.get_blob(ob) == blob(100, seed=8)


# -- snapshots -----------------------------------------------------------------

def test_snapshot_stable_across_publish(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    o1 = s.put_blob(blob(20, seed=1))
    token, gen = s.acquire_snapshot()
    assert gen == 1
    assert len(token) == 12
    assert token[0:2] == b"SN"
    o2 = s.put_blob(blob(20, seed=2))
    assert s.generation() == 2
    # Old snapshot still reads the old generation: o1 visible, o2 absent
    # (it did not exist at gen 1) — never a mixture.
    assert s.snapshot_get_blob(token, gen, o1) == blob(20, seed=1)
    with pytest.raises(NotFoundError):
        s.snapshot_get_blob(token, gen, o2)
    # A fresh snapshot sees the new generation.
    token2, gen2 = s.acquire_snapshot()
    assert gen2 == 2
    assert s.snapshot_get_blob(token2, gen2, o2) == blob(20, seed=2)
    assert s.snapshot_get_blob(token2, gen2, o1) == blob(20, seed=1)


def test_snapshot_permit_refuses_foreign_generation(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    s.put_blob(blob(20, seed=1))
    token, gen = s.acquire_snapshot()
    assert s.snapshot_ok(token, gen) is True
    assert s.snapshot_ok(token, gen + 1) is False
    other = s.put_blob(blob(20, seed=9))
    with pytest.raises(SnapshotExpired):
        s.snapshot_get_blob(token, gen + 1, other)


def test_snapshot_release_allows_reclamation_progress(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    o1 = s.put_blob(bytes([0xAA] * 40))
    token, gen = s.acquire_snapshot()
    s.put_blob(bytes([0xBB] * 40))
    # While the snapshot is live, reclaim must keep the gen-1 file (pinned)
    # and every chunk (all referenced).
    deleted, pruned_gens, _ = s.reclaim(keep_gens=1, dry_run=True)
    assert deleted == []
    assert 1 not in pruned_gens  # gen 1 pinned by the live snapshot
    s.release_snapshot(gen)
    deleted, pruned_gens, _ = s.reclaim(keep_gens=1, dry_run=True)
    # Generation files prune by horizon + pin: gen 1 is unpinned and outside
    # keep_gens=1, so its file is reported. Chunks prune by reference, and
    # generations are full snapshots — gen 2's manifest still names o1's
    # chunks — so no chunk is reported while o1 stays logically live.
    assert 1 in pruned_gens, f"gen-1 file not prunable: {pruned_gens}"
    assert deleted == [], f"live chunks must survive: {deleted}"
    assert s.get_blob(o1) == bytes([0xAA] * 40)  # still readable pre-prune
    # Orphan bytes (crash leftovers no manifest names) ARE chunk-prunable.
    orphan = "0f" * 32
    with open(os.path.join(store_path, "chunks", orphan), "wb") as f:
        f.write(bytes([0xEE] * 36))
    deleted, _, _ = s.reclaim(keep_gens=1, dry_run=True)
    assert orphan in deleted, f"orphan chunk not prunable: {deleted}"


# -- fault injection and recovery -------------------------------------------------

FAULT_POINTS = ["before-chunks", "mid-chunks", "after-chunks",
                "after-manifests", "after-generation", "before-current",
                "after-current"]


@pytest.mark.parametrize("fault", FAULT_POINTS)
def test_fault_boundaries_recover_to_old_or_new(store_path, engine, fault):
    s = StorePhase2.create(store_path, engine)
    old_oid = s.put_blob(blob(32, seed=1))
    old_gen = s.generation()
    new_data = blob(40, seed=2)
    try:
        s.put_blob(new_data, fault_at=fault)
        committed = True
    except FaultInjected:
        committed = False
    # Simulate process restart: fresh instance, no carried state.
    s2 = crash_reopen(store_path, engine)
    decision, gen, _ = s2.recover()
    assert decision in ("healthy", "stay", "promote")
    # Central invariant: readers observe the old committed generation or
    # the new valid one — never a mixture, never fabrication.
    assert gen in (old_gen, old_gen + 1)
    assert s2.get_blob(old_oid) == blob(32, seed=1)
    if gen == old_gen + 1:
        # Only a fully valid candidate promotes (MNCS recover_decide):
        # that is exactly the crashes at/after the generation file
        # completed. Earlier crashes stay on the previous generation.
        assert fault in ("after-generation", "before-current",
                         "after-current"), fault
    else:
        assert fault in ("before-chunks", "mid-chunks", "after-chunks",
                         "after-manifests"), fault
    s2.close()


def test_torn_generation_file_stays_on_previous(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    old_oid = s.put_blob(blob(32, seed=1))
    try:
        s.put_blob(blob(40, seed=2), fault_at="after-generation")
    except FaultInjected:
        pass
    # Tear the candidate generation file (truncate mid-record).
    with open(os.path.join(store_path, "generations", "00000002"), "r+b") as f:
        data = f.read()
        f.seek(0)
        f.write(data[:20])
        f.truncate()
    s2 = crash_reopen(store_path, engine)
    code, _, _ = s2._classify_generation_file(2)
    assert code == 1  # torn candidate: structurally incomplete
    decision, gen, _ = s2.recover()
    assert (decision, gen) == ("stay", 1)
    assert s2.get_blob(old_oid) == blob(32, seed=1)


def test_missing_chunk_candidate_not_promoted(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    old_oid = s.put_blob(blob(32, seed=1))
    before = set(s.chunk_files())
    try:
        s.put_blob(blob(64, seed=2), fault_at="after-generation")
    except FaultInjected:
        pass
    # Delete one candidate-only chunk: the candidate is incomplete.
    candidate_chunks = set(s.chunk_files()) - before
    assert candidate_chunks, "expected new chunk files from the candidate"
    os.remove(os.path.join(store_path, "chunks",
                           sorted(candidate_chunks)[0]))
    s2 = crash_reopen(store_path, engine)
    code, _, _ = s2._classify_generation_file(2)
    assert code == 2  # structure parses, referenced bytes absent
    decision, gen, _ = s2.recover()
    assert (decision, gen) == ("stay", 1)
    assert s2.get_blob(old_oid) == blob(32, seed=1)


def test_stale_temp_reclaimed_only_when_unreferenced(store_path, engine):
    s = StorePhase2.create(store_path, engine)
    o1 = s.put_blob(blob(40, seed=1))
    entry = s.load_committed2(o1)
    # Referenced-content temp: conservative keep.
    with open(os.path.join(store_path, "temp", "staged-a"), "wb") as f:
        f.write(entry["frames"][0])
    # Garbage temp: prunable.
    with open(os.path.join(store_path, "temp", "staged-b"), "wb") as f:
        f.write(b"\x00" * 7)
    deleted, _, kept = s.reclaim(keep_gens=1, dry_run=True)
    assert "temp/staged-b" in deleted
    assert "temp/staged-a" in kept
    assert s.get_blob(o1) == blob(40, seed=1)
