"""Phase 1a lifecycle tests: put/close/reopen/get/verify + corruption.

Every test runs the real on-disk layout (tmp_path) with all semantic
bytes produced by mncs-language executions (see store_phase1a.py for the
host/MNCS responsibility split). Host-side byte flips simulate torn
writes and corruption; each must surface as an explicit
IntegrityError/NotFoundError/TypeMismatchError, never silent data.
"""

import os

import pytest

from mncs_exec import BYTES, U32, U64, call_many
from store_phase1a import (
    Engine,
    IntegrityError,
    NotFoundError,
    StoreError,
    StorePhase1a,
    TypeMismatchError,
    UnsupportedWidthError,
)

U32_MAX = 0xFFFFFFFF
U64_MAX = 0xFFFFFFFFFFFFFFFF


@pytest.fixture
def engine():
    return Engine()


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "store")


def reopen(path, engine):
    """Fresh instance with no carried state (process-restart equivalent).

    The driver holds no module-level caches, so a new instance observes
    only committed on-disk state, exactly like a new process would.
    """
    return StorePhase1a.open(path, engine)


def test_u32_roundtrip_close_reopen(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    assert s.generation() == 0
    oid = s.put_u32(0x01020304)
    assert len(oid) == 12
    assert s.generation() == 1
    assert s.get_u32(oid) == 0x01020304
    s.close()
    s2 = reopen(store_path, engine)
    assert s2.generation() == 1
    assert s2.verify(oid) is True
    assert s2.get_u32(oid) == 0x01020304


def test_typed_matrix_close_reopen(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    blob8 = bytes(range(8))
    blob16 = bytes(range(16))
    blob32 = bytes(range(32))
    oids = s.put_many([
        ("u32", [U32(U32_MAX)]),
        ("u64", [U64(0x8000000000000000)]),
        ("u64", [U64(U64_MAX)]),
        ("pair", [U32(1), U32(2)]),
        ("blob8", [BYTES(blob8)]),
        ("blob16", [BYTES(blob16)]),
        ("blob32", [BYTES(blob32)]),
        ("empty", []),
    ])
    assert len({o.hex() for o in oids}) == 8  # distinct logical identities
    assert s.generation() == 1  # one atomic commit for the batch
    assert s.verify_all() == 8
    s.close()
    s2 = reopen(store_path, engine)
    assert s2.verify_all() == 8
    got = s2.get_many([
        ("a", oids[0], "u32"),
        ("b", oids[1], "u64"),
        ("c", oids[2], "u64"),
        ("d", oids[3], "pair-first"),
        ("e", oids[4], "blob"),
        ("f", oids[5], "blob"),
        ("g", oids[6], "blob"),
    ])
    assert got["a"] == U32_MAX
    assert got["b"] == 0x8000000000000000
    assert got["c"] == U64_MAX
    assert got["d"] == (1, 2)
    assert got["e"] == blob8
    assert got["f"] == blob16
    assert got["g"] == blob32
    # Empty object verifies; it carries no payload to decode.
    assert s2.verify(oids[7]) is True
    entry = s2.load_committed(oids[7])
    assert entry["frame"] == bytes([0x43, 0x01, 0x00, 0x00])


def test_identical_content_dedups_without_merging_identity(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    o1 = s.put_u32(7)
    n_chunks_1 = len(s.chunk_files())
    o2 = s.put_u32(7)
    assert o1 != o2  # distinct logical objects (invariant 4)
    assert len(s.chunk_files()) == n_chunks_1 == 1  # one shared chunk
    with open(os.path.join(store_path, "objects", o1.hex()), "rb") as f:
        r1 = f.read()
    with open(os.path.join(store_path, "objects", o2.hex()), "rb") as f:
        r2 = f.read()
    assert r1 == r2  # identical content -> identical root bytes
    # Roots carry no logical identity (o1/o2 differ but r1 == r2), so
    # chunk/root dedup can never merge distinct logical objects.
    assert s.get_u32(o1) == 7
    assert s.get_u32(o2) == 7


def test_chunk_immutability_never_overwritten(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    o1 = s.put_u32(7)
    chunk_name = s.load_committed(o1)["chunk_digest"].hex()
    chunk_path = os.path.join(store_path, "chunks", chunk_name)
    with open(chunk_path, "rb") as f:
        good = f.read()
    # A faulty writer plants different bytes under the same content name.
    with open(chunk_path, "wb") as f:
        f.write(b"X" * len(good))
    # Reads now fail integrity (hash mismatch), loudly.
    with pytest.raises(IntegrityError):
        s.get_u32(o1)
    # And re-putting the same value refuses to "heal" by overwriting.
    with pytest.raises(IntegrityError):
        s.put_u32(7)


def test_corrupt_chunk_payload_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u64(0x0102030405060708)
    entry = s.load_committed(oid)
    chunk_path = os.path.join(store_path, "chunks",
                              entry["chunk_digest"].hex())
    with open(chunk_path, "r+b") as f:
        data = bytearray(f.read())
        data[6] ^= 0xFF  # flip a payload bit; framing stays valid
        f.seek(0)
        f.write(data)
    with pytest.raises(IntegrityError):
        s.verify(oid)
    with pytest.raises(IntegrityError):
        s.get_u64(oid)


def test_corrupt_chunk_length_field_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(9)
    entry = s.load_committed(oid)
    chunk_path = os.path.join(store_path, "chunks",
                              entry["chunk_digest"].hex())
    with open(chunk_path, "r+b") as f:
        data = bytearray(f.read())
        data[3] ^= 0xFF  # declared length no longer matches the file
        f.seek(0)
        f.write(data)
    with pytest.raises(IntegrityError):
        s.verify(oid)


def test_corrupt_manifest_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(11)
    obj_path = os.path.join(store_path, "objects", oid.hex())
    with open(obj_path, "r+b") as f:
        data = bytearray(f.read())
        data[25] ^= 0xFF  # total_len disagrees with descriptor/chunk lens
        f.seek(0)
        f.write(data)
    with pytest.raises(IntegrityError):
        s.get_u32(oid)


def test_corrupt_manifest_magic_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(13)
    obj_path = os.path.join(store_path, "objects", oid.hex())
    with open(obj_path, "r+b") as f:
        data = bytearray(f.read())
        data[0] = 0x00
        f.seek(0)
        f.write(data)
    with pytest.raises(IntegrityError):
        s.verify(oid)


def test_corrupt_root_digest_mismatches_committed_identity(store_path, engine):
    # Structural validation still passes (bytes 0..32 intact) but the root
    # no longer matches the generation mapping: integrity failure, proving
    # identity-vs-structure separation.
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(17)
    obj_path = os.path.join(store_path, "objects", oid.hex())
    with open(obj_path, "r+b") as f:
        data = bytearray(f.read())
        data[40] ^= 0x01
        f.seek(0)
        f.write(data)
    with pytest.raises(IntegrityError):
        s.verify(oid)


def test_truncated_files_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_blob(bytes(range(16)))
    entry = s.load_committed(oid)
    chunk_path = os.path.join(store_path, "chunks",
                              entry["chunk_digest"].hex())
    with open(chunk_path, "rb") as f:
        data = f.read()
    with open(chunk_path, "wb") as f:
        f.write(data[:-1])  # torn chunk write
    with pytest.raises(IntegrityError):
        s.get_blob(oid)
    with open(chunk_path, "wb") as f:
        f.write(data)  # restore
    assert s.get_blob(oid) == bytes(range(16))
    obj_path = os.path.join(store_path, "objects", oid.hex())
    with open(obj_path, "rb") as f:
        root = f.read()
    with open(obj_path, "wb") as f:
        f.write(root[:63])  # torn manifest write
    with pytest.raises(IntegrityError):
        s.verify(oid)


def test_missing_chunk_is_integrity_failure_not_not_found(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(19)
    entry = s.load_committed(oid)
    os.unlink(os.path.join(store_path, "chunks",
                           entry["chunk_digest"].hex()))
    with pytest.raises(IntegrityError):
        s.get_u32(oid)
    # Unknown logical identities are a DIFFERENT, explicit condition.
    with pytest.raises(NotFoundError):
        s.get_u32(bytes(12))


def test_missing_object_file_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(23)
    os.unlink(os.path.join(store_path, "objects", oid.hex()))
    with pytest.raises(IntegrityError):
        s.verify(oid)


def test_type_mismatch_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    ou32 = s.put_u32(29)
    opair = s.put_pair(1, 2)
    with pytest.raises(TypeMismatchError):
        s.get_u64(ou32)
    with pytest.raises(TypeMismatchError):
        s.get_u32(opair)
    with pytest.raises(TypeMismatchError):
        s.get_blob(ou32)
    # Correctly typed reads still work after mismatched attempts.
    assert s.get_u32(ou32) == 29
    assert s.get_pair(opair) == (1, 2)


def test_determinism_across_independent_stores(tmp_path, engine):
    items = [("u32", [U32(42)]), ("pair", [U32(3), U32(4)])]
    stores = []
    for name in ("a", "b"):
        p = str(tmp_path / name)
        st = StorePhase1a.create(p, engine)
        st.put_many(items)
        st.close()
        stores.append(p)
    for rel in ("chunks", "objects", "generations"):
        a_files = sorted(os.listdir(os.path.join(stores[0], rel)))
        b_files = sorted(os.listdir(os.path.join(stores[1], rel)))
        assert a_files == b_files, f"{rel} file sets differ"
        for name in a_files:
            with open(os.path.join(stores[0], rel, name), "rb") as f:
                ba = f.read()
            with open(os.path.join(stores[1], rel, name), "rb") as f:
                bb = f.read()
            assert ba == bb, f"{rel}/{name} bytes differ"


def test_mncs_digests_agree_with_independent_oracle(store_path, engine):
    """hashlib cross-checks MNCS sha256 over real persisted chunk files."""
    from store_phase1a import StorePhase1a as S

    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(0xDEADBEEF & 0xFFFFFFFF)
    s.close()
    s2 = reopen(store_path, engine)
    entry = s2.load_committed(oid)
    with open(os.path.join(store_path, "chunks",
                           entry["chunk_digest"].hex()), "rb") as f:
        frame = f.read()
    assert S.sha256_oracle(frame) == entry["chunk_digest"]
    assert s2.get_u32(oid) == (0xDEADBEEF & 0xFFFFFFFF)


def test_store_version_gate(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    s.close()
    with open(os.path.join(store_path, "meta"), "r+b") as f:
        meta = bytearray(f.read())
        meta[2] = 99  # unknown future store version
        f.seek(0)
        f.write(meta)
    with pytest.raises(StoreError):
        StorePhase1a.open(store_path, engine)


def test_store_magic_gate(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    s.close()
    with open(os.path.join(store_path, "meta"), "r+b") as f:
        meta = bytearray(f.read())
        meta[0] = 0x00
        f.seek(0)
        f.write(meta)
    with pytest.raises(IntegrityError):
        StorePhase1a.open(store_path, engine)


def test_stale_serial_counter_heals_on_open(store_path, engine):
    """A torn meta write must not reissue serials (recovery scan)."""
    s = StorePhase1a.create(store_path, engine)
    o1 = s.put_u32(100)
    s.close()
    # Simulate a crash between the current-pointer rename and the meta
    # persist: meta still claims serial 1 is next, but object 1 exists.
    with open(os.path.join(store_path, "meta"), "r+b") as f:
        meta = bytearray(f.read())
        meta[8:16] = (1).to_bytes(8, "big")
        f.seek(0)
        f.write(meta)
    s2 = reopen(store_path, engine)
    o2 = s2.put_u32(200)
    assert o1 != o2  # serial was healed forward, not reused
    assert s2.get_u32(o1) == 100
    assert s2.get_u32(o2) == 200


def test_truncated_generation_rejected_on_open(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    s.put_u32(1)
    s.close()
    gen_path = os.path.join(store_path, "generations", "00000001")
    with open(gen_path, "rb") as f:
        data = f.read()
    with open(gen_path, "wb") as f:
        f.write(data[:-1])  # torn generation write
    with pytest.raises(IntegrityError):
        StorePhase1a.open(store_path, engine)


def test_unsupported_blob_length_rejected_without_storage(store_path, engine):
    """Odd blob lengths fail explicitly before any byte is stored."""
    s = StorePhase1a.create(store_path, engine)
    with pytest.raises(UnsupportedWidthError):
        s.put_blob(bytes(range(5)))
    assert s.chunk_files() == []  # nothing persisted, no serial side effects
    assert s.generation() == 0


def test_empty_blob_reads_back_empty(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_empty()
    s.close()
    s2 = reopen(store_path, engine)
    assert s2.get_blob(oid) == b""
    assert s2.get_many([("e", oid, "blob")]) == {"e": b""}


def test_overlong_root_file_rejected(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(31)
    obj_path = os.path.join(store_path, "objects", oid.hex())
    with open(obj_path, "rb") as f:
        root = f.read()
    with open(obj_path, "wb") as f:
        f.write(root + b"\x00")  # appended garbage after a torn write
    with pytest.raises(IntegrityError):
        s.verify(oid)


def test_double_close_is_noop(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    s.put_u32(1)
    s.close()
    s.close()  # must not raise
    s2 = reopen(store_path, engine)
    assert s2.generation() == 1


def test_missing_current_pointer_rejected_on_open(store_path, engine):
    s = StorePhase1a.create(store_path, engine)
    s.put_u32(1)
    s.close()
    os.unlink(os.path.join(store_path, "current"))
    with pytest.raises(IntegrityError):
        StorePhase1a.open(store_path, engine)


def test_in_language_chunk_file_verification(store_path, engine):
    """store.read_verify proves file->digest verification in-language.

    Needs combined --grant-read + --grant-crypto for one capability; if the
    CLI refuses that combination, this is language pressure (recorded) and
    the test skips instead of passing vacuously.
    """
    from mncs_exec import StoreHarnessError

    s = StorePhase1a.create(store_path, engine)
    oid = s.put_u32(0x01020304)
    entry = s.load_committed(oid)
    chunk_path = os.path.join(store_path, "chunks",
                              entry["chunk_digest"].hex())
    grants = ["--grant-read", f"store_read={chunk_path}",
              "--grant-crypto", "store_read"]
    try:
        out = call_many(
            "src/store/read_verify.mncs", "store.read_verify.v1",
            [("v", "verify_chunk_file4", [BYTES(entry["chunk_digest"])])],
            engine.backend, grants=grants)
    except StoreHarnessError as exc:
        pytest.skip(f"combined read+crypto grants unsupported: {exc}")
        return
    case = out["v"]
    if case.get("status") == "unsupported":
        pytest.skip("in-language file verification unsupported on this backend")
        return
    from mncs_exec import as_bool, require_returned
    assert as_bool(require_returned(case, "in-language verify")) is True
    # Corrupt the file; the same in-language check must now fail.
    with open(chunk_path, "rb") as f:
        data = f.read()
    with open(chunk_path, "wb") as f:
        f.write(data[:-1] + bytes([data[-1] ^ 0xFF]))
    try:
        out = call_many(
            "src/store/read_verify.mncs", "store.read_verify.v1",
            [("v", "verify_chunk_file4", [BYTES(entry["chunk_digest"])])],
            engine.backend, grants=grants)
        assert as_bool(require_returned(out["v"], "in-language re-verify")) is False
    finally:
        with open(chunk_path, "wb") as f:
            f.write(data)
