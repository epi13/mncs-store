"""Adaptive Store substrate: semantic envelopes, representations, blocks, plans.

Part 1 exercises the new MNCS modules through the real CLI across
executable backends: every expectation below is hand-computed from the
record layouts and decision rules documented in `src/store/*.mncs`.
Part 2 runs the EmbeddedStore adaptive paths through a real retained
MNCS session: inspection without materialization, selective block
reads, intent-driven selection, coded representations, external plans,
corruption failures, and legacy synthesis.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from mncs_exec import (
    BYTES,
    FAST_BACKENDS,
    U64,
    as_bool,
    as_bytes,
    as_int,
    call_many,
    require_returned,
)
from mncs_store import (
    BlockInput,
    EmbeddedStore,
    RepresentationInput,
    StoreError,
    StoreResultCode,
    StoreSession,
)
from mncs_store.embedded import CrashInjected, StoreIntegrityError

RESEARCH = "mncs-research-bytecode"

ID_CODEC = bytes([
    77, 78, 67, 83, 45, 83, 84, 79, 82, 69, 45, 67, 79, 68, 69, 67,
    45, 73, 68, 69, 78, 84, 73, 84, 89, 45, 48, 49, 0, 0, 0, 0,
])
RLE_CODEC = bytes([
    77, 78, 67, 83, 45, 83, 84, 79, 82, 69, 45, 67, 79, 68, 69, 67,
    45, 82, 76, 69, 45, 86, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0,
])
ZERO32 = bytes(32)

MAX_U64 = 2**64 - 1


def _mutate(record: bytes, offset: int, value: bytes) -> bytes:
    raw = bytearray(record)
    raw[offset : offset + len(value)] = value
    return bytes(raw)


# ---------------------------------------------------------------------------
# Codec: registry and bounded RLE windows.
# ---------------------------------------------------------------------------


def test_codec_registry_names_two_supported_codecs():
    out = call_many(
        "src/store/codec.mncs",
        "store.codec.v1",
        [
            ("sup0", "codec_is_supported", [U64(0)]),
            ("sup1", "codec_is_supported", [U64(1)]),
            ("sup7", "codec_is_supported", [U64(7)]),
            ("code-id", "codec_code_for", [BYTES(ID_CODEC)]),
            ("code-rle", "codec_code_for", [BYTES(RLE_CODEC)]),
            ("code-zero", "codec_code_for", [BYTES(ZERO32)]),
            ("class0", "codec_decode_class", [U64(0)]),
            ("class1", "codec_decode_class", [U64(1)]),
            ("class9", "codec_decode_class", [U64(9)]),
            ("id0", "codec_identity_for", [U64(0)]),
            ("id1", "codec_identity_for", [U64(1)]),
            ("id9", "codec_identity_for", [U64(9)]),
        ],
        RESEARCH,
    )
    assert as_bool(require_returned(out["sup0"], "identity supported"))
    assert as_bool(require_returned(out["sup1"], "rle supported"))
    assert not as_bool(require_returned(out["sup7"], "unknown refused"))
    assert as_int(require_returned(out["code-id"], "identity code")) == 0
    assert as_int(require_returned(out["code-rle"], "rle code")) == 1
    assert as_int(require_returned(out["code-zero"], "zero code")) == MAX_U64
    assert as_int(require_returned(out["class0"], "identity class")) == 0
    assert as_int(require_returned(out["class1"], "rle class")) == 1
    assert as_int(require_returned(out["class9"], "unknown class")) == 3
    assert as_bytes(require_returned(out["id0"], "identity bytes")) == ID_CODEC
    assert as_bytes(require_returned(out["id1"], "rle bytes")) == RLE_CODEC
    assert as_bytes(require_returned(out["id9"], "unknown bytes")) == ZERO32


def test_codec_rle_matches_hand_computed_vectors():
    distinct64 = bytes(range(64))
    same64 = bytes([9]) * 64
    out = call_many(
        "src/store/codec.mncs",
        "store.codec.v1",
        [
            ("len-empty", "rle_encode_block_len", [BYTES(b"")]),
            ("len-one", "rle_encode_block_len", [BYTES(bytes([7]))]),
            ("len-two", "rle_encode_block_len", [BYTES(bytes([7, 7]))]),
            ("len-three", "rle_encode_block_len", [BYTES(bytes([7, 7, 7]))]),
            ("len-mixed", "rle_encode_block_len", [BYTES(bytes([7, 7, 7, 7, 1]))]),
            ("len-distinct64", "rle_encode_block_len", [BYTES(distinct64)]),
            ("len-same64", "rle_encode_block_len", [BYTES(same64)]),
            ("len-split", "rle_encode_block_len", [BYTES(bytes([5, 5, 6, 6, 6, 6]))]),
            ("enc-mixed", "rle_encode_block", [BYTES(bytes([7, 7, 7, 7, 1]))]),
            ("enc-one", "rle_encode_block", [BYTES(bytes([7]))]),
            ("enc-distinct64", "rle_encode_block", [BYTES(distinct64)]),
            ("enc-same64", "rle_encode_block", [BYTES(same64)]),
            ("enc-split", "rle_encode_block", [BYTES(bytes([5, 5, 6, 6, 6, 6]))]),
            ("enc-empty", "rle_encode_block", [BYTES(b"")]),
        ],
        RESEARCH,
    )
    get = lambda cid: as_int(require_returned(out[cid], cid))
    assert get("len-empty") == 0
    assert get("len-one") == 2  # literal token + 1 byte
    assert get("len-two") == 3
    assert get("len-three") == 2  # repeat token + value
    assert get("len-mixed") == 4  # [129,7,0,1]
    assert get("len-distinct64") == 65  # worst case: one literal token
    assert get("len-same64") == 2
    assert get("len-split") == 5  # [1,5,5,129,6]
    mixed = as_bytes(require_returned(out["enc-mixed"], "mixed bytes"))
    assert mixed[:4] == bytes([129, 7, 0, 1])
    assert mixed[4:] == bytes(61)
    assert as_bytes(require_returned(out["enc-one"], "one"))[:2] == bytes([0, 7])
    assert as_bytes(require_returned(out["enc-distinct64"], "distinct")) == bytes([63]) + distinct64
    assert as_bytes(require_returned(out["enc-same64"], "same"))[:2] == bytes([189, 9])
    assert as_bytes(require_returned(out["enc-split"], "split"))[:5] == bytes([1, 5, 5, 129, 6])
    assert as_bytes(require_returned(out["enc-empty"], "empty")) == bytes(65)


def test_codec_rle_decode_validates_and_fails_closed():
    out = call_many(
        "src/store/codec.mncs",
        "store.codec.v1",
        [
            ("ok", "rle_decode_block", [BYTES(bytes([129, 7, 0, 1])), U64(4), U64(5)]),
            ("plen", "rle_decode_block", [BYTES(bytes([129, 7, 0, 1])), U64(4), U64(4)]),
            ("trunc", "rle_decode_block", [BYTES(bytes([5, 1, 2])), U64(3), U64(6)]),
            ("trunc-rep", "rle_decode_block", [BYTES(bytes([200])), U64(1), U64(3)]),
            ("trail", "rle_decode_block", [BYTES(bytes([129, 7, 0, 1, 9])), U64(5), U64(5)]),
            ("badlen", "rle_decode_block", [BYTES(bytes([129, 7, 0, 1])), U64(3), U64(5)]),
            ("empty", "rle_decode_block", [BYTES(b""), U64(0), U64(0)]),
            ("declen-ok", "rle_decoded_length", [BYTES(bytes([129, 7, 0, 1])), U64(4)]),
            ("declen-bad", "rle_decoded_length", [BYTES(bytes([5, 1, 2])), U64(3)]),
        ],
        RESEARCH,
    )
    good = as_bytes(require_returned(out["ok"], "decode ok"))
    assert good[0] == 0
    assert good[1:6] == bytes([7, 7, 7, 7, 1])
    assert as_bytes(require_returned(out["plen"], "short plen"))[0] == 3
    trunc = as_bytes(require_returned(out["trunc"], "truncated"))
    assert trunc[0] == 2
    assert trunc[1:] == bytes(64)  # fail closed: no partial bytes
    assert as_bytes(require_returned(out["trunc-rep"], "truncated repeat"))[0] == 2
    assert as_bytes(require_returned(out["trail"], "trailing"))[0] == 3
    assert as_bytes(require_returned(out["badlen"], "bad length"))[0] == 1
    assert as_bytes(require_returned(out["empty"], "empty"))[0] == 0
    assert as_int(require_returned(out["declen-ok"], "decoded length")) == 5
    assert as_int(require_returned(out["declen-bad"], "bad length max")) == MAX_U64


def test_codec_rle_roundtrip_sweep():
    # Tricky shapes plus deterministic pseudo-random windows. The law is
    # decode(encode(x)) == x with the exact encoded length; vectors above
    # pin the byte format independently.
    state = 0x12345678
    windows = [
        b"",
        bytes([0]),
        bytes([255]),
        bytes([1, 1]),
        bytes([1, 2]),
        bytes([4, 4, 4]),
        bytes([4]) * 63 + bytes([5]),
        bytes([5]) + bytes([4]) * 63,
        bytes(range(64)),
        bytes([7]) * 64,
        bytes([i % 2 for i in range(64)]),
        bytes([1, 1, 2, 2, 2, 3, 3, 3, 3]),
    ]
    for _ in range(8):
        window = bytearray()
        for _ in range(64):
            state = (state * 1103515245 + 12345) % 2**31
            window.append((state >> 16) % 256)
        windows.append(bytes(window))
    out = call_many(
        "src/store/codec.mncs",
        "store.codec.v1",
        [(f"enc-{i}", "rle_encode_block", [BYTES(w)]) for i, w in enumerate(windows)]
        + [(f"len-{i}", "rle_encode_block_len", [BYTES(w)]) for i, w in enumerate(windows)],
        RESEARCH,
    )
    pairs = []
    for i, window in enumerate(windows):
        raw = as_bytes(require_returned(out[f"enc-{i}"], f"encode {i}"))
        length = as_int(require_returned(out[f"len-{i}"], f"length {i}"))
        assert 0 <= length <= 65
        assert (length == 0) == (len(window) == 0)
        assert raw[length:] == bytes(65 - length)
        pairs.append((raw[:length], len(window)))
    out = call_many(
        "src/store/codec.mncs",
        "store.codec.v1",
        [
            (f"dec-{i}", "rle_decode_block", [BYTES(data), U64(len(data)), U64(plain)])
            for i, (data, plain) in enumerate(pairs)
        ],
        RESEARCH,
    )
    for i, window in enumerate(windows):
        decoded = as_bytes(require_returned(out[f"dec-{i}"], f"decode {i}"))
        assert decoded[0] == 0
        assert decoded[1 : 1 + len(window)] == window


# ---------------------------------------------------------------------------
# Intent: generic, implementation-free access constraints.
# ---------------------------------------------------------------------------


def _intent_args(fidelity=5, latency=0, compute=0, memory=0, transfer=0,
                 frequency=0, lifetime=0, locality=0):
    return [U64(fidelity), U64(latency), U64(compute), U64(memory),
            U64(transfer), U64(frequency), U64(lifetime), U64(locality)]


def test_intent_roundtrip_default_and_normalize():
    out = call_many(
        "src/store/intent.mncs",
        "store.intent.v1",
        [
            ("rt", "roundtrip_fields", _intent_args()),
            ("rt2", "roundtrip_fields", _intent_args(1, 2, 3, 4, 100, 3, 4, 999)),
            ("default", "default_intent", []),
            ("wild", "encode_fields", _intent_args(9, 1, 2, 3, 4, 7, 9, 123456)),
            ("known5", "fidelity_is_known", [U64(5)]),
            ("known6", "fidelity_is_known", [U64(6)]),
            ("badver", "validate", [BYTES(bytes([65, 73, 2]) + bytes(61))]),
        ],
        RESEARCH,
    )
    assert as_bool(require_returned(out["rt"], "roundtrip"))
    assert as_bool(require_returned(out["rt2"], "roundtrip 2"))
    default = as_bytes(require_returned(out["default"], "default"))
    assert len(default) == 64
    assert as_bool(require_returned(out["known5"], "known 5"))
    assert not as_bool(require_returned(out["known6"], "known 6"))
    assert as_int(require_returned(out["badver"], "bad version")) == 3
    out = call_many(
        "src/store/intent.mncs",
        "store.intent.v1",
        [
            ("v", "validate", [BYTES(default)]),
            ("f", "fidelity", [BYTES(default)]),
            ("n", "normalize", [BYTES(as_bytes(require_returned(out["wild"], "wild")))]),
        ],
        RESEARCH,
    )
    assert as_int(require_returned(out["v"], "default valid")) == 0
    assert as_int(require_returned(out["f"], "default fidelity")) == 5
    norm = as_bytes(require_returned(out["n"], "normalized"))
    out = call_many(
        "src/store/intent.mncs",
        "store.intent.v1",
        [
            ("f", "fidelity", [BYTES(norm)]),
            ("q", "frequency", [BYTES(norm)]),
            ("e", "lifetime", [BYTES(norm)]),
            ("l", "locality", [BYTES(norm)]),
            ("t", "transfer", [BYTES(norm)]),
        ],
        RESEARCH,
    )
    # Unknown codes normalize: fidelity clamps to exact, frequency and
    # lifetime reset to unknown, weights and locality pass through.
    assert as_int(require_returned(out["f"], "norm fidelity")) == 5
    assert as_int(require_returned(out["q"], "norm frequency")) == 0
    assert as_int(require_returned(out["e"], "norm lifetime")) == 0
    assert as_int(require_returned(out["l"], "norm locality")) == 123456
    assert as_int(require_returned(out["t"], "norm transfer")) == 4


# ---------------------------------------------------------------------------
# Envelope: inspect before materializing.
# ---------------------------------------------------------------------------

LOGICAL = bytes(range(12))
ENV_TYPE = bytes(range(1, 33))
ENV_SYN = bytes(range(2, 34))
ENV_ROOT = bytes(range(3, 35))
ENV_PROV = bytes(range(4, 36))
ENV_BTAB = bytes(range(5, 37))


def _envelope_args():
    return [
        BYTES(LOGICAL), BYTES(ENV_TYPE), BYTES(ENV_SYN), U64(512), U64(2), U64(4),
        U64(1000), U64(8000), BYTES(ENV_ROOT), BYTES(ENV_PROV), BYTES(ENV_BTAB),
        U64(7), U64(39), U64(7),
    ]


def test_envelope_roundtrip_and_default_synthesis():
    out = call_many(
        "src/store/envelope.mncs",
        "store.envelope.v1",
        [
            ("rt", "roundtrip_fields", _envelope_args()),
            ("enc", "encode_fields", _envelope_args()),
            ("dflt", "envelope_default_for",
             [BYTES(LOGICAL), BYTES(ENV_ROOT), U64(4096), U64(3)]),
            ("b0", "bitmap_with", [U64(0), U64(0)]),
            ("b5", "bitmap_with", [U64(1), U64(5)]),
            ("bidem", "bitmap_with", [U64(33), U64(0)]),
            ("bunk", "bitmap_with", [U64(33), U64(9)]),
        ],
        RESEARCH,
    )
    assert as_bool(require_returned(out["rt"], "roundtrip"))
    env = as_bytes(require_returned(out["enc"], "encoded"))
    assert len(env) == 256
    assert env[:4] == bytes([69, 86, 1, 7])
    assert as_int(require_returned(out["b0"], "bit 0")) == 1
    assert as_int(require_returned(out["b5"], "bit 5")) == 33
    assert as_int(require_returned(out["bidem"], "idempotent")) == 33
    assert as_int(require_returned(out["bunk"], "unknown level")) == 33
    default = as_bytes(require_returned(out["dflt"], "default"))
    out = call_many(
        "src/store/envelope.mncs",
        "store.envelope.v1",
        [
            ("ve", "validate_exact", [BYTES(env)]),
            ("vd", "validate_exact", [BYTES(default)]),
            ("o5", "offers", [BYTES(env), U64(5)]),
            ("o3", "offers", [BYTES(env), U64(3)]),
            ("o6", "offers", [BYTES(env), U64(6)]),
            ("o0", "offers", [BYTES(default), U64(0)]),
            ("c-ok", "envelope_consistent",
             [BYTES(default), BYTES(LOGICAL), BYTES(ENV_ROOT), U64(4096), U64(3)]),
            ("c-bad", "envelope_consistent",
             [BYTES(default), BYTES(LOGICAL), BYTES(ENV_ROOT), U64(4097), U64(3)]),
            ("rc", "rep_count", [BYTES(env)]),
            ("sb", "stored_bytes", [BYTES(env)]),
            ("gen", "generation", [BYTES(default)]),
        ],
        RESEARCH,
    )
    assert as_int(require_returned(out["ve"], "exact valid")) == 0
    assert as_int(require_returned(out["vd"], "default valid")) == 0
    assert as_bool(require_returned(out["o5"], "offers 5"))
    assert not as_bool(require_returned(out["o3"], "offers 3"))
    assert not as_bool(require_returned(out["o6"], "offers 6"))
    assert as_bool(require_returned(out["o0"], "default offers 0"))
    assert as_bool(require_returned(out["c-ok"], "consistent"))
    assert not as_bool(require_returned(out["c-bad"], "inconsistent"))
    assert as_int(require_returned(out["rc"], "rep count")) == 2
    assert as_int(require_returned(out["sb"], "stored")) == 1000
    assert as_int(require_returned(out["gen"], "generation")) == 3


def test_envelope_exact_validation_failure_codes():
    out = call_many(
        "src/store/envelope.mncs", "store.envelope.v1",
        [("enc", "encode_fields", _envelope_args())], RESEARCH,
    )
    env = as_bytes(require_returned(out["enc"], "encoded"))
    cases = [
        # (name, mutation, expected code)
        ("zero-logical", _mutate(env, 4, bytes(12)), 5),
        ("zero-reps", _mutate(env, 88, (0).to_bytes(8, "big")), 6),
        ("no-identity-bit", _mutate(env, 224, (38).to_bytes(8, "big")), 7),
        ("synopsis-mismatch", _mutate(env, 48, bytes(32)), 8),
        ("table-mismatch", _mutate(env, 184, bytes(32)), 9),
        ("multi-mismatch", _mutate(env, 3, bytes([5])), 10),
        ("bad-flags", _mutate(env, 3, bytes([9])), 4),
        ("dirty-reserved", _mutate(env, 255, bytes([1])), 4),
        ("bad-magic", _mutate(env, 0, bytes([69, 87])), 2),
        ("short", env[:255], 1),
    ]
    out = call_many(
        "src/store/envelope.mncs",
        "store.envelope.v1",
        [("v-" + name, "validate_exact" if len(raw) == 256 else "validate", [BYTES(raw)])
         for name, raw, _code in cases],
        RESEARCH,
    )
    for name, _raw, code in cases:
        assert as_int(require_returned(out["v-" + name], name)) == code


# ---------------------------------------------------------------------------
# Representation: descriptors, cost, and selection tournaments.
# ---------------------------------------------------------------------------

ROOTA = bytes(range(10, 42))
ROOTB = bytes(range(20, 52))
ROOTC = bytes(range(30, 62))


def _rep_args(fidelity=5, codec=0, codec_id=ID_CODEC, stored=8000, plain=8000,
              dclass=0, first=0, blocks=1, root=ROOTA, flags=1):
    return [U64(fidelity), U64(codec), BYTES(codec_id), U64(stored), U64(plain),
            U64(dclass), U64(first), U64(blocks), BYTES(root), U64(flags)]


def _encode_reps():
    return call_many(
        "src/store/representation.mncs",
        "store.representation.v1",
        [
            ("rt", "roundtrip_fields", _rep_args(5, 1, RLE_CODEC, 1000, 8000, 1, 0, 1, ROOTB, 1)),
            ("exact", "encode_fields", _rep_args()),
            ("rle", "encode_fields", _rep_args(5, 1, RLE_CODEC, 1000, 8000, 1, 0, 1, ROOTB, 1)),
            ("syn", "encode_fields", _rep_args(1, 0, ID_CODEC, 200, 200, 0, 0, 0, ROOTC, 0)),
            ("heavy", "encode_fields", _rep_args(5, 0, ID_CODEC, 7000, 8000, 3, 0, 1, ROOTA, 1)),
            ("opaque", "encode_fields", _rep_args(1, 0, ID_CODEC, 200, 200, 0, 0, 0, ROOTC, 0)),
            ("bad5", "encode_fields", _rep_args(9)),
            ("bad6", "encode_fields", _rep_args(5, 7)),
            ("bad7", "encode_fields", _rep_args(5, 0, RLE_CODEC)),
            ("bad8", "encode_fields", _rep_args(5, 0, ID_CODEC, 8, 8, 9)),
            ("bad9", "encode_fields", _rep_args(1, 0, ID_CODEC, 8, 8, 0, 3, 0, ROOTA, 0)),
            ("bad10", "encode_fields", _rep_args(4)),
            ("bad11", "encode_fields", _rep_args(5, 0, ID_CODEC, 8, 8, 0, 0, 1, bytes(32), 1)),
            ("mid4", "encode_fields", _rep_args(4, 0, ID_CODEC, 500, 900, 0, 0, 0, ROOTC, 0)),
            ("mid3", "encode_fields", _rep_args(3, 0, ID_CODEC, 100, 200, 0, 0, 0, ROOTB, 0)),
        ],
        RESEARCH,
    )


def test_representation_validation_failure_codes():
    out = _encode_reps()
    assert as_bool(require_returned(out["rt"], "roundtrip"))
    raws = {key: as_bytes(require_returned(out[key], key)) for key in (
        "exact", "rle", "syn", "opaque", "bad5", "bad6", "bad7", "bad8",
        "bad9", "bad10", "bad11")}
    raws["bad4"] = _mutate(raws["exact"], 127, bytes([1]))
    out = call_many(
        "src/store/representation.mncs",
        "store.representation.v1",
        [("v-" + key, "validate_exact", [BYTES(raw)]) for key, raw in raws.items()] + [
            ("sat1", "satisfies", [BYTES(raws["rle"]), U64(5)]),
            ("sat2", "satisfies", [BYTES(raws["syn"]), U64(5)]),
            ("sat3", "satisfies", [BYTES(raws["syn"]), U64(1)]),
        ],
        RESEARCH,
    )
    assert as_int(require_returned(out["v-exact"], "exact")) == 0
    assert as_int(require_returned(out["v-rle"], "rle")) == 0
    assert as_int(require_returned(out["v-syn"], "synopsis")) == 0
    assert as_int(require_returned(out["v-opaque"], "opaque")) == 0
    assert as_int(require_returned(out["v-bad4"], "code 4")) == 4
    for code in (5, 6, 7, 8, 9, 10, 11):
        assert as_int(require_returned(out[f"v-bad{code}"], f"code {code}")) == code
    assert as_bool(require_returned(out["sat1"], "satisfies exact"))
    assert not as_bool(require_returned(out["sat2"], "synopsis misses exact"))
    assert as_bool(require_returned(out["sat3"], "synopsis meets synopsis"))


def test_representation_cost_and_rank():
    reps = _encode_reps()
    exact = as_bytes(require_returned(reps["exact"], "exact"))
    rle = as_bytes(require_returned(reps["rle"], "rle"))
    syn = as_bytes(require_returned(reps["syn"], "syn"))
    heavy = as_bytes(require_returned(reps["heavy"], "heavy"))
    mid4 = as_bytes(require_returned(reps["mid4"], "mid4"))
    mid3 = as_bytes(require_returned(reps["mid3"], "mid3"))
    bad5 = as_bytes(require_returned(reps["bad5"], "bad5"))
    bad6 = as_bytes(require_returned(reps["bad6"], "bad6"))
    intents = call_many(
        "src/store/intent.mncs",
        "store.intent.v1",
        [
            ("dflt", "default_intent", []),
            ("tx", "encode_fields", _intent_args(5, 0, 0, 0, 100)),
            ("syn", "encode_fields", _intent_args(1)),
            ("ia", "encode_fields", _intent_args(5, 1)),
            ("huge", "encode_fields", _intent_args(5, 0, 0, 0, 2**63)),
        ],
        RESEARCH,
    )
    idef = as_bytes(require_returned(intents["dflt"], "default"))
    itx = as_bytes(require_returned(intents["tx"], "transfer"))
    isyn = as_bytes(require_returned(intents["syn"], "synopsis"))
    iia = as_bytes(require_returned(intents["ia"], "interactive"))
    ihuge = as_bytes(require_returned(intents["huge"], "huge"))
    out = call_many(
        "src/store/representation.mncs",
        "store.representation.v1",
        [
            ("c-exact", "estimate_cost", [BYTES(exact), BYTES(idef)]),
            ("c-rle", "estimate_cost", [BYTES(rle), BYTES(idef)]),
            ("c-syn", "estimate_cost", [BYTES(syn), BYTES(idef)]),
            ("c-rle-tx", "estimate_cost", [BYTES(rle), BYTES(itx)]),
            ("c-exact-tx", "estimate_cost", [BYTES(exact), BYTES(itx)]),
            ("c-huge", "estimate_cost", [BYTES(exact), BYTES(ihuge)]),
            ("r-def", "rank", [BYTES(exact), BYTES(rle), BYTES(idef)]),
            ("r-tx", "rank", [BYTES(exact), BYTES(rle), BYTES(itx)]),
            ("r-exact-req", "rank", [BYTES(syn), BYTES(rle), BYTES(idef)]),
            ("r-syn-req", "rank", [BYTES(syn), BYTES(rle), BYTES(isyn)]),
            ("r-ia", "rank", [BYTES(heavy), BYTES(exact), BYTES(iia)]),
            ("r-bad", "rank", [BYTES(bad5), BYTES(bad6), BYTES(idef)]),
            ("r-halfbad", "rank", [BYTES(bad5), BYTES(rle), BYTES(idef)]),
            ("r-degrade", "rank", [BYTES(mid4), BYTES(mid3), BYTES(idef)]),
            ("lp", "latency_permits", [BYTES(iia), BYTES(heavy)]),
            ("lp2", "latency_permits", [BYTES(iia), BYTES(exact)]),
        ],
        RESEARCH,
    )
    # Hand-computed: stored*tw + plain*mw + class*plain*cw, weights 1.
    assert as_int(require_returned(out["c-exact"], "cost exact")) == 16000
    assert as_int(require_returned(out["c-rle"], "cost rle")) == 17000
    assert as_int(require_returned(out["c-syn"], "cost syn")) == 400
    assert as_int(require_returned(out["c-rle-tx"], "cost rle tx")) == 1000 * 100 + 8000 + 8000
    assert as_int(require_returned(out["c-exact-tx"], "cost exact tx")) == 8000 * 100 + 8000
    assert as_int(require_returned(out["c-huge"], "cost saturates")) == MAX_U64
    # Default weights favor cheap decode; transfer weight favors density.
    assert as_int(require_returned(out["r-def"], "rank default")) == 0
    assert as_int(require_returned(out["r-tx"], "rank transfer")) == 1
    assert as_int(require_returned(out["r-exact-req"], "rank exact req")) == 1
    assert as_int(require_returned(out["r-syn-req"], "rank syn req")) == 0
    assert as_int(require_returned(out["r-ia"], "rank interactive")) == 1
    assert as_int(require_returned(out["r-bad"], "rank both bad")) == 2
    assert as_int(require_returned(out["r-halfbad"], "rank half bad")) == 1
    # Neither satisfies exact: degrade toward the richer fidelity.
    assert as_int(require_returned(out["r-degrade"], "rank degrade")) == 0
    assert not as_bool(require_returned(out["lp"], "heavy refused interactive"))
    assert as_bool(require_returned(out["lp2"], "exact allowed interactive"))


# ---------------------------------------------------------------------------
# Block: descriptors, tables, closure, and byte accounting.
# ---------------------------------------------------------------------------

DIGEST_A = bytes(range(1, 33))
DIGEST_B = bytes(range(33, 65))
DIGEST_C = bytes(range(65, 97))


def _block_args(index=0, digest=DIGEST_A, stored=65536, plain=65536, tag=7,
                start=0, length=65536, depcount=0, deps=(0, 0, 0, 0), flags=2):
    return [U64(index), BYTES(digest), U64(stored), U64(plain), U64(tag),
            U64(start), U64(length), U64(depcount),
            U64(deps[0]), U64(deps[1]), U64(deps[2]), U64(deps[3]), U64(flags)]


def _encode_blocks():
    return call_many(
        "src/store/block.mncs",
        "store.block.v1",
        [
            ("rt", "roundtrip_fields", _block_args(1, DIGEST_B, 100, 65536, 7, 65536, 65536, 1, (0, 0, 0, 0), 1)),
            ("b0", "encode_fields", _block_args()),
            ("b1", "encode_fields", _block_args(1, DIGEST_B, 100, 65536, 7, 65536, 65536, 1, (0, 0, 0, 0), 1)),
            ("b2", "encode_fields", _block_args(2, DIGEST_C, 50, 100, 9, 131072, 100, 1, (1, 0, 0, 0), 1)),
            ("b5", "encode_fields", _block_args(64)),
            ("b6", "encode_fields", _block_args(0, DIGEST_A, 8, 8, 0, 0, 8, 5, (0, 0, 0, 0), 1)),
            ("b7", "encode_fields", _block_args(2, DIGEST_A, 8, 8, 0, 16, 8, 1, (2, 0, 0, 0), 1)),
            ("b8", "encode_fields", _block_args(0, bytes(32))),
            ("b9", "encode_fields", _block_args(0, DIGEST_A, 8, 8, 0, 0, 8, 0, (0, 0, 0, 0), 1)),
            ("b0b", "encode_fields", _block_args(5, DIGEST_A, 8, 8, 0, 0, 65536)),
            ("bit63", "bit", [U64(63)]),
            ("bit0", "bit", [U64(0)]),
            ("mc", "mask_count", [U64(0b10111)]),
            ("bs", "bit_set", [U64(0b10111), U64(4)]),
            ("bs2", "bit_set", [U64(0b10111), U64(3)]),
            ("r0", "range_mask", [U64(0), U64(1)]),
            ("r3", "range_mask", [U64(2), U64(3)]),
            ("r0c", "range_mask", [U64(5), U64(0)]),
            ("rclamp", "range_mask", [U64(62), U64(8)]),
            ("rhi", "range_mask", [U64(64), U64(4)]),
            ("mw-in", "mask_within", [U64(0b11), U64(0), U64(2)]),
            ("mw-out", "mask_within", [U64(0b100), U64(0), U64(2)]),
            ("mw-empty", "mask_within", [U64(0), U64(0), U64(2)]),
        ],
        RESEARCH,
    )


def test_block_records_tables_and_closure():
    out = _encode_blocks()
    assert as_bool(require_returned(out["rt"], "roundtrip"))
    b0 = as_bytes(require_returned(out["b0"], "b0"))
    b1 = as_bytes(require_returned(out["b1"], "b1"))
    b2 = as_bytes(require_returned(out["b2"], "b2"))
    bads = {f"b{i}": as_bytes(require_returned(out[f"b{i}"], f"b{i}")) for i in (5, 6, 7, 8, 9)}
    assert as_int(require_returned(out["bit63"], "bit 63")) == 2**63
    assert as_int(require_returned(out["bit0"], "bit 0")) == 1
    assert as_int(require_returned(out["mc"], "popcount")) == 4
    assert as_bool(require_returned(out["bs"], "bit set"))
    assert not as_bool(require_returned(out["bs2"], "bit clear"))
    assert as_int(require_returned(out["r0"], "range 0..1")) == 1
    assert as_int(require_returned(out["r3"], "range 2..5")) == 0b11100
    assert as_int(require_returned(out["r0c"], "range empty")) == 0
    assert as_int(require_returned(out["rclamp"], "range clamp")) == 0b11 << 62
    assert as_int(require_returned(out["rhi"], "range high")) == 0
    assert as_bool(require_returned(out["mw-in"], "within"))
    assert not as_bool(require_returned(out["mw-out"], "outside"))
    assert as_bool(require_returned(out["mw-empty"], "empty within"))
    b0b = as_bytes(require_returned(out["b0b"], "b0b"))
    dirty_reserved = bytearray(b0)
    dirty_reserved[127] = 1
    table = b0 + b1
    chain = b0 + b1 + b2
    out = call_many(
        "src/store/block.mncs",
        "store.block.v1",
        [
            ("v0", "validate_exact", [BYTES(b0)]),
            ("v1", "validate_exact", [BYTES(b1)]),
            ("v2", "validate_exact", [BYTES(b2)]),
            ("v4", "validate_exact", [BYTES(bytes(dirty_reserved))]),
            ("v5", "validate_exact", [BYTES(bads["b5"])]),
            ("v6", "validate_exact", [BYTES(bads["b6"])]),
            ("v7", "validate_exact", [BYTES(bads["b7"])]),
            ("v8", "validate_exact", [BYTES(bads["b8"])]),
            ("v9", "validate_exact", [BYTES(bads["b9"])]),
            ("t-ok", "table_validate", [BYTES(table), U64(2)]),
            ("t-len", "table_validate", [BYTES(table), U64(3)]),
            ("t-count", "table_validate", [BYTES(table), U64(9)]),
            ("t-dup", "table_validate", [BYTES(b0 + b0), U64(2)]),
            ("t-over", "table_validate", [BYTES(b0 + b0b), U64(2)]),
            ("t-chain", "table_validate", [BYTES(chain), U64(3)]),
            ("sw-ok", "spans_within", [BYTES(table), U64(2), U64(131072)]),
            ("sw-bad", "spans_within", [BYTES(table), U64(2), U64(100000)]),
            ("cs1", "closure_step", [BYTES(table), U64(2), U64(0b10)]),
            ("cs0", "closure_step", [BYTES(table), U64(2), U64(0b01)]),
            ("cc0", "closure_closed", [BYTES(table), U64(2), U64(0b10)]),
            ("cc1", "closure_closed", [BYTES(table), U64(2), U64(0b11)]),
            ("ch1", "closure_step", [BYTES(chain), U64(3), U64(0b100)]),
            ("ch2", "closure_step", [BYTES(chain), U64(3), U64(0b110)]),
            ("chc", "closure_closed", [BYTES(chain), U64(3), U64(0b111)]),
            ("tag7", "blocks_with_tag", [BYTES(table), U64(2), U64(7)]),
            ("tag8", "blocks_with_tag", [BYTES(table), U64(2), U64(8)]),
            ("ms", "mask_stored", [BYTES(table), U64(2), U64(0b10)]),
            ("mp", "mask_plain", [BYTES(table), U64(2), U64(0b11)]),
            ("slot-oor", "slot_bytes", [BYTES(table), U64(2), U64(5)]),
            ("w", "table_word", [BYTES(table), U64(2), U64(1), U64(44)]),
            ("ss", "slot_start", [BYTES(table), U64(2), U64(1)]),
        ],
        RESEARCH,
    )
    assert as_int(require_returned(out["v0"], "b0")) == 0
    assert as_int(require_returned(out["v1"], "b1")) == 0
    assert as_int(require_returned(out["v2"], "b2")) == 0
    assert as_int(require_returned(out["v4"], "reserved tail")) == 4
    for code in (5, 6, 7, 8, 9):
        assert as_int(require_returned(out[f"v{code}"], f"code {code}")) == code
    assert as_int(require_returned(out["t-ok"], "table ok")) == 0
    assert as_int(require_returned(out["t-len"], "table len")) == 1
    assert as_int(require_returned(out["t-count"], "table count")) == 2
    assert as_int(require_returned(out["t-dup"], "table dup")) == 11
    assert as_int(require_returned(out["t-over"], "table overlap")) == 12
    assert as_int(require_returned(out["t-chain"], "chain ok")) == 0
    assert as_bool(require_returned(out["sw-ok"], "spans within"))
    assert not as_bool(require_returned(out["sw-bad"], "spans exceed"))
    # Block 1 depends on block 0; block 2 depends on block 1.
    assert as_int(require_returned(out["cs1"], "closure grows")) == 0b11
    assert as_int(require_returned(out["cs0"], "closure idempotent")) == 0b01
    assert not as_bool(require_returned(out["cc0"], "open mask"))
    assert as_bool(require_returned(out["cc1"], "closed mask"))
    assert as_int(require_returned(out["ch1"], "chain step 1")) == 0b110
    assert as_int(require_returned(out["ch2"], "chain step 2")) == 0b111
    assert as_bool(require_returned(out["chc"], "chain closed"))
    assert as_int(require_returned(out["tag7"], "tag 7")) == 0b11
    assert as_int(require_returned(out["tag8"], "tag 8")) == 0
    assert as_int(require_returned(out["ms"], "mask stored")) == 100
    assert as_int(require_returned(out["mp"], "mask plain")) == 131072
    assert as_bytes(require_returned(out["slot-oor"], "slot oob")) == bytes(128)
    assert as_int(require_returned(out["w"], "table word")) == 100
    assert as_int(require_returned(out["ss"], "slot start")) == 65536


# ---------------------------------------------------------------------------
# Plan: validated proposals from external authorities.
# ---------------------------------------------------------------------------


def test_plan_admission_and_block_checks():
    reps = _encode_reps()
    rle = as_bytes(require_returned(reps["rle"], "rle"))
    syn = as_bytes(require_returned(reps["syn"], "syn"))
    blocks = _encode_blocks()
    b0 = as_bytes(require_returned(blocks["b0"], "b0"))
    b1 = as_bytes(require_returned(blocks["b1"], "b1"))
    table = b0 + b1
    envs = call_many(
        "src/store/envelope.mncs", "store.envelope.v1",
        [
            ("enc", "encode_fields", _envelope_args()),
            ("dflt", "envelope_default_for",
             [BYTES(LOGICAL), BYTES(ENV_ROOT), U64(4096), U64(3)]),
        ],
        RESEARCH,
    )
    env5 = as_bytes(require_returned(envs["enc"], "envelope"))
    env_default = as_bytes(require_returned(envs["dflt"], "default envelope"))
    out = call_many(
        "src/store/plan.mncs",
        "store.plan.v1",
        [
            ("rt", "roundtrip_fields", [U64(5), BYTES(ROOTB), U64(0b11), U64(0), BYTES(bytes(32))]),
            ("p", "encode_fields", [U64(5), BYTES(ROOTB), U64(0b11), U64(0), BYTES(bytes(32))]),
            ("p3", "encode_fields", [U64(3), BYTES(ROOTB), U64(0b11), U64(0), BYTES(bytes(32))]),
            ("p0", "encode_fields", [U64(5), BYTES(ROOTB), U64(0), U64(0), BYTES(bytes(32))]),
            ("p1b", "encode_fields", [U64(5), BYTES(ROOTB), U64(0b10), U64(0), BYTES(bytes(32))]),
            ("p2b", "encode_fields", [U64(5), BYTES(ROOTB), U64(0b100), U64(0), BYTES(bytes(32))]),
            ("pcap", "encode_fields", [U64(5), BYTES(ROOTB), U64(0b11), U64(50), BYTES(bytes(32))]),
            ("pw", "encode_fields", [U64(5), BYTES(ROOTA), U64(0b11), U64(0), BYTES(bytes(32))]),
            ("psyn", "encode_fields", [U64(5), BYTES(ROOTC), U64(0b11), U64(0), BYTES(bytes(32))]),
            ("popaque", "encode_fields", [U64(1), BYTES(ROOTC), U64(0), U64(0), BYTES(bytes(32))]),
            ("pbadmask", "encode_fields", [U64(1), BYTES(ROOTC), U64(0b1), U64(0), BYTES(bytes(32))]),
        ],
        RESEARCH,
    )
    assert as_bool(require_returned(out["rt"], "roundtrip"))
    plans = {key: as_bytes(require_returned(out[key], key)) for key in (
        "p", "p3", "p0", "p1b", "p2b", "pcap", "pw", "psyn", "popaque", "pbadmask")}
    out = call_many(
        "src/store/plan.mncs",
        "store.plan.v1",
        [
            ("ok", "plan_validate", [BYTES(plans["p"]), BYTES(env5), BYTES(rle)]),
            ("root", "plan_validate", [BYTES(plans["pw"]), BYTES(env5), BYTES(rle)]),
            ("fid", "plan_validate", [BYTES(plans["psyn"]), BYTES(env5), BYTES(syn)]),
            ("mask", "plan_validate", [BYTES(plans["p0"]), BYTES(env5), BYTES(rle)]),
            ("unoffered", "plan_validate", [BYTES(plans["p3"]), BYTES(env_default), BYTES(rle)]),
            ("opaque-ok", "plan_validate", [BYTES(plans["popaque"]), BYTES(env5), BYTES(syn)]),
            ("opaque-mask", "plan_validate", [BYTES(plans["pbadmask"]), BYTES(env5), BYTES(syn)]),
            ("b-ok", "plan_blocks_validate", [BYTES(plans["p"]), BYTES(table), U64(2)]),
            ("b-open", "plan_blocks_validate", [BYTES(plans["p1b"]), BYTES(table), U64(2)]),
            ("b-undesc", "plan_blocks_validate", [BYTES(plans["p2b"]), BYTES(table), U64(2)]),
            ("b-cap", "plan_blocks_validate", [BYTES(plans["pcap"]), BYTES(table), U64(2)]),
            ("b-bad", "plan_blocks_validate", [BYTES(plans["p"]), BYTES(table), U64(3)]),
        ],
        RESEARCH,
    )
    assert as_int(require_returned(out["ok"], "admit")) == 0
    assert as_int(require_returned(out["root"], "root mismatch")) == 4
    assert as_int(require_returned(out["fid"], "fidelity")) == 5
    assert as_int(require_returned(out["mask"], "empty mask")) == 6
    assert as_int(require_returned(out["unoffered"], "unoffered")) == 7
    assert as_int(require_returned(out["opaque-ok"], "opaque admit")) == 0
    assert as_int(require_returned(out["opaque-mask"], "mask without coverage")) == 8
    assert as_int(require_returned(out["b-ok"], "blocks admit")) == 0
    assert as_int(require_returned(out["b-open"], "blocks open")) == 2
    assert as_int(require_returned(out["b-undesc"], "blocks undescribed")) == 4
    assert as_int(require_returned(out["b-cap"], "blocks cap")) == 3
    assert as_int(require_returned(out["b-bad"], "blocks bad table")) == 1


# ---------------------------------------------------------------------------
# Canonical: ordered forms for fixed-record tables.
# ---------------------------------------------------------------------------


def test_canonical_sort_and_equality():
    out = call_many(
        "src/store/canonical.mncs",
        "store.canonical.v1",
        [
            ("s-unsorted", "rows_sorted", [BYTES(bytes([3, 1, 2])), U64(1), U64(3)]),
            ("s-sorted", "rows_sorted", [BYTES(bytes([1, 2, 3])), U64(1), U64(3)]),
            ("s-badw", "rows_sorted", [BYTES(bytes([1, 2, 3])), U64(0), U64(3)]),
            ("s-badlen", "rows_sorted", [BYTES(bytes([1, 2, 3])), U64(1), U64(2)]),
            ("s-empty", "rows_sorted", [BYTES(b""), U64(1), U64(0)]),
            ("sort1", "canonical_sort", [BYTES(bytes([3, 1, 2])), U64(1), U64(3)], 262144),
            ("sort1b", "canonical_sort", [BYTES(bytes([3, 1, 2])), U64(1), U64(3)], 262144),
            ("sort2", "canonical_sort", [BYTES(bytes([2, 9, 1, 0, 2, 1])), U64(2), U64(3)], 262144),
            ("sort3", "canonical_sort", [BYTES(bytes([2, 2, 1, 1, 2, 2])), U64(2), U64(3)], 262144),
            ("sort-bad", "canonical_sort", [BYTES(bytes([3, 1, 2])), U64(1), U64(2)], 262144),
            ("eq-t", "canonical_equal", [BYTES(bytes([3, 1, 2])), BYTES(bytes([1, 2, 3])), U64(1), U64(3)], 524288),
            ("eq-f", "canonical_equal", [BYTES(bytes([3, 1, 2])), BYTES(bytes([1, 2, 4])), U64(1), U64(3)], 524288),
            ("eq-bad", "canonical_equal", [BYTES(bytes([3, 1, 2])), BYTES(bytes([1, 2, 3])), U64(2), U64(3)], 524288),
        ],
        RESEARCH,
    )
    assert as_int(require_returned(out["s-unsorted"], "unsorted")) == 2
    assert as_int(require_returned(out["s-sorted"], "sorted")) == 0
    assert as_int(require_returned(out["s-badw"], "bad width")) == 1
    assert as_int(require_returned(out["s-badlen"], "bad length")) == 1
    assert as_int(require_returned(out["s-empty"], "empty sorted")) == 0
    first = as_bytes(require_returned(out["sort1"], "sort"))
    assert first[:3] == bytes([1, 2, 3])
    assert first[3:] == bytes(253)
    # Deterministic: the same input sorts to identical bytes twice.
    assert as_bytes(require_returned(out["sort1b"], "sort again")) == first
    assert as_bytes(require_returned(out["sort2"], "sort pairs"))[:6] == bytes([1, 0, 2, 1, 2, 9])
    assert as_bytes(require_returned(out["sort3"], "sort dupes"))[:6] == bytes([1, 1, 2, 2, 2, 2])
    assert as_bytes(require_returned(out["sort-bad"], "bad passthrough"))[:3] == bytes([3, 1, 2])
    assert as_bool(require_returned(out["eq-t"], "equal"))
    assert not as_bool(require_returned(out["eq-f"], "unequal"))
    assert not as_bool(require_returned(out["eq-bad"], "bad bounds unequal"))


def test_canonical_bad_bounds_output_is_not_canonical():
    out = call_many(
        "src/store/canonical.mncs", "store.canonical.v1",
        [("sort-bad", "canonical_sort", [BYTES(bytes([3, 1, 2])), U64(1), U64(2)], 262144)],
        RESEARCH,
    )
    passthrough = as_bytes(require_returned(out["sort-bad"], "passthrough"))
    out = call_many(
        "src/store/canonical.mncs", "store.canonical.v1",
        [("check", "rows_sorted", [BYTES(passthrough[:3]), U64(1), U64(3)])],
        RESEARCH,
    )
    # The passthrough output provably fails the orderedness witness, so a
    # caller that checks cannot mistake it for a canonical form.
    assert as_int(require_returned(out["check"], "not sorted")) == 2


# ---------------------------------------------------------------------------
# Cross-backend agreement for the pure adaptive surface.
# ---------------------------------------------------------------------------


def test_adaptive_modules_agree_across_fast_backends():
    vectors = {
        "src/store/codec.mncs": ("store.codec.v1", [
            ("enc", "rle_encode_block", [BYTES(bytes([7, 7, 7, 7, 1]))]),
            ("dec", "rle_decode_block", [BYTES(bytes([129, 7, 0, 1])), U64(4), U64(5)]),
        ]),
        "src/store/intent.mncs": ("store.intent.v1", [
            ("norm", "normalize", [BYTES(bytes(
                [65, 73, 1, 0] + list((9).to_bytes(8, "big")) + [0] * 52))]),
        ]),
        "src/store/envelope.mncs": ("store.envelope.v1", [
            # wasm-mvp counts steps more expensively than research-bytecode
            # for this shape; agreement compares values, not budgets.
            ("rt", "roundtrip_fields", _envelope_args(), 131072),
            ("offers", "offers", [BYTES(bytes(
                [69, 86, 1, 7] + [0] * 220 + list((39).to_bytes(8, "big")) + [0] * 24)),
                U64(5)]),
        ]),
        "src/store/canonical.mncs": ("store.canonical.v1", [
            ("sort", "canonical_sort", [BYTES(bytes([3, 1, 2])), U64(1), U64(3)], 1048576),
        ]),
    }
    for source, (module, calls) in vectors.items():
        outs = {
            backend: call_many(source, module, calls, backend)
            for backend in FAST_BACKENDS
        }
        for cid, _fn, _args, *_rest in calls:
            first = outs[FAST_BACKENDS[0]][cid]
            assert first["status"] == "returned", (source, cid, first.get("failure_reason"))
            for backend in FAST_BACKENDS[1:]:
                other = outs[backend][cid]
                assert other["status"] == "returned", (source, cid, backend)
                assert other["returned"] == first["returned"], (source, cid, backend)


# ---------------------------------------------------------------------------
# Part 2: EmbeddedStore adaptive paths through a real retained MNCS session.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def session():
    with StoreSession() as held:
        yield held


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "adaptive-store"


def _segmented_payload(block_size: int = 4096, blocks: int = 8) -> tuple[bytes, list[bytes]]:
    parts = []
    for i in range(blocks):
        if i == 3:
            parts.append(bytes([i]) * block_size)
        else:
            parts.append(bytes((i * 37 + j * 11) % 251 for j in range(block_size)))
    return b"".join(parts), parts


def _commit_segmented(store: EmbeddedStore, payload: bytes, block_size: int = 4096,
                       synopsis: bytes | None = None, with_rle: bool = True):
    assert len(payload) % block_size == 0
    blocks = len(payload) // block_size
    block_inputs = [
        BlockInput(index=i, tag=100 + (i % 3), start=i * block_size, length=block_size)
        for i in range(blocks)
    ]
    reps = [RepresentationInput(fidelity=5, codec="rle", payload=payload)] if with_rle else []
    return store.put_bound_object(
        domain_schema=b"adaptive-test",
        domain_identity=b"obj-1",
        descriptor=b"adaptive-test-desc",
        payload=payload,
        expected_generation=store.current_generation,
        synopsis=b"adaptive-test synopsis" if synopsis is None else synopsis,
        representations=reps,
        blocks=block_inputs,
    )


def test_envelope_inspection_touches_no_payload(session, store_path):
    payload, _parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        result = _commit_segmented(store, payload)
        assert result.code == StoreResultCode.COMMITTED
        env = store.get_envelope(b"adaptive-test", b"obj-1")
        assert env.fields["rep_count"] == 3
        assert env.fields["block_count"] == 8
        assert env.fields["plain_bytes"] == len(payload)
        assert env.fields["synopsis_bytes"] == len(b"adaptive-test synopsis")
        # Inspection stays tiny while the payload is 32 KiB.
        assert env.stored_bytes_touched < 4096
        assert session.envelope_offers(env.raw, 5)
        assert session.envelope_offers(env.raw, 1)
        assert not session.envelope_offers(env.raw, 3)


def test_selective_block_read_fetches_one_block(session, store_path):
    payload, parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
        view = store.read_blocks(b"adaptive-test", b"obj-1", 0b1000)
        assert view.payload == parts[3]
        assert view.materialized_bytes == 4096
        assert view.mask == 0b1000
        # One 32 KiB chunk holds the whole object here, so the chunk still
        # dominates; the materialized bytes prove only block 3 was decoded
        # into the result (multi-chunk proofs live in the measurement).
        assert view.materialized_bytes == 4096
        empty = store.read_blocks(b"adaptive-test", b"obj-1", 0)
        assert empty.payload == b""
        assert empty.stored_bytes_touched == 0
        full = store.read_blocks(b"adaptive-test", b"obj-1", 0xFF)
        assert full.payload == payload
        assert full.exact_verified
        with pytest.raises(StoreError):
            store.read_blocks(b"adaptive-test", b"obj-1", 0x100)


def test_selective_read_spans_chunks(session, store_path):
    # 80 KiB over two 64 KiB chunks; the middle block crosses the chunk
    # boundary, so this covers pruned tree walks and span assembly.
    payload = bytes((i * 13 + (i // 700)) % 251 for i in range(80 * 1024))
    block_inputs = [
        BlockInput(index=0, tag=1, start=0, length=60000),
        BlockInput(index=1, tag=2, start=60000, length=10000),
        BlockInput(index=2, tag=1, start=70000, length=80 * 1024 - 70000),
    ]
    with EmbeddedStore(store_path, session=session) as store:
        result = store.put_bound_object(
            domain_schema=b"adaptive-test",
            domain_identity=b"chunked",
            descriptor=b"chunked-desc",
            payload=payload,
            expected_generation=store.current_generation,
            blocks=block_inputs,
        )
        assert result.code == StoreResultCode.COMMITTED
        metrics = store.object_metrics(b"adaptive-test", b"chunked")
        assert metrics["chunk_count"] == 2
        crossing = store.read_blocks(b"adaptive-test", b"chunked", 0b010)
        assert crossing.payload == payload[60000:70000]
        assert crossing.materialized_bytes == 10000
        assert crossing.mask == 0b010
        # Both chunks back the crossing span, plus at most one small
        # verified node on the traversed path — never unrelated bytes.
        assert crossing.stored_bytes_touched <= metrics["stored_chunk_bytes"] + 1024
        assert crossing.stored_bytes_touched > 10000
        tail = store.read_blocks(b"adaptive-test", b"chunked", 0b100)
        assert tail.payload == payload[70000:]
        assert tail.stored_bytes_touched < crossing.stored_bytes_touched
        full = store.read_blocks(b"adaptive-test", b"chunked", 0b111)
        assert full.payload == payload
        assert full.exact_verified


def test_selection_under_differing_intents(session, store_path):
    payload, _parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
        default = store.select_representation(b"adaptive-test", b"obj-1")
        assert default.index == 0  # no decode cost wins unconstrained
        assert default.satisfied
        transfer = store.select_representation(
            b"adaptive-test", b"obj-1", session.encode_intent(5, transfer=1000))
        assert transfer.index == 2  # density wins transfer-heavy
        assert transfer.satisfied
        synopsis = store.select_representation(
            b"adaptive-test", b"obj-1", session.encode_intent(1))
        assert synopsis.index == 1
        assert synopsis.satisfied
        assert store.read_synopsis(b"adaptive-test", b"obj-1") == b"adaptive-test synopsis"


def test_tag_materialization_and_rle_exactness(session, store_path):
    payload, parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
        tagged = store.materialize(
            b"adaptive-test", b"obj-1",
            intent=session.encode_intent(2), tag=101)
        assert tagged.mask == 0b10010010
        with pytest.raises(StoreError):
            store.materialize(b"adaptive-test", b"obj-1", tag=1 << 64)
        assert tagged.payload == parts[1] + parts[4] + parts[7]
        assert tagged.materialized_bytes == 3 * 4096
        assert tagged.satisfied
        coded = store.materialize(
            b"adaptive-test", b"obj-1", intent=session.encode_intent(5, transfer=1000))
        assert coded.representation_index == 2
        assert coded.payload == payload
        assert coded.exact_verified
        # Whole-object baseline still works on adaptive objects.
        assert store.get_bound_object(b"adaptive-test", b"obj-1").payload == payload


def test_mask_accounting_helpers_for_external_authorities(session, store_path):
    payload, _parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
        env = store.get_envelope(b"adaptive-test", b"obj-1")
        table_digest = env.fields["block_table"]
        assert isinstance(table_digest, bytes)
        table_raw = (store_path / "blocks" / f"{table_digest.hex()}.table").read_bytes()
        count = int.from_bytes(table_raw[:4], "big")
        table = table_raw[4:]
        assert count == 8
        stored, plain = session.mask_bytes(table, count, 0b1000)
        assert stored == 4096
        assert plain == 4096
        assert session.mask_count(0b10010010) == 3
        assert session.blocks_with_tag(table, count, 101) == 0b10010010
        assert session.range_mask(0, 8) == 0xFF
        assert session.mask_within(0xFF, 0, 8)
        assert not session.mask_within(0x100, 0, 8)


def test_rle_windows_chunk_across_batch_crossings(session):
    # 200 KiB needs 3125 windows: encode crosses twice (2 requests per
    # window against the 4096 cap) and decode crosses once.
    payload = bytes((i * 31 + (i // 64)) % 251 for i in range(200 * 1024))
    encoded = session.rle_encode_windows(payload)
    assert len(encoded) == (len(payload) + 63) // 64
    assert sum(len(window) for window in encoded) > 0
    framed = [(window, min(64, len(payload) - i * 64)) for i, window in enumerate(encoded)]
    assert session.rle_decode_windows(framed) == payload


def test_envelope_generation_survives_later_commits(session, store_path):
    payload, _parts = _segmented_payload(block_size=1024, blocks=8)
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload, block_size=1024)
        store.put_bound_object(
            domain_schema=b"adaptive-test",
            domain_identity=b"obj-2",
            descriptor=b"d2",
            payload=b"second",
            expected_generation=store.current_generation,
        )
        assert store.current_generation == 2
    # After reopen the head is generation 2 while obj-1's envelope claims
    # generation 1: resolution must verify against the binding's commit
    # generation, not the head.
    with EmbeddedStore(store_path, session=session) as store:
        assert store.current_generation == 2
        env = store.get_envelope(b"adaptive-test", b"obj-1")
        assert env.fields["generation"] == 1
        view = store.materialize(b"adaptive-test", b"obj-1")
        assert view.payload == payload
        assert view.exact_verified


def test_plan_admit_and_refuse(session, store_path):
    payload, parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
        reps = store.get_representations(b"adaptive-test", b"obj-1")
        assert [r.fields["fidelity"] for r in reps] == [5, 1, 5]
        rle_root = reps[2].fields["root"]
        assert isinstance(rle_root, bytes)
        plan = session.encode_plan(
            fidelity=5, root=rle_root, mask=0, cap=1 << 40, authority=bytes(32))
        admitted = store.materialize_plan(b"adaptive-test", b"obj-1", plan)
        assert admitted.payload == payload
        assert admitted.representation_index == 2
        base_root = reps[0].fields["root"]
        assert isinstance(base_root, bytes)
        block_plan = session.encode_plan(
            fidelity=5, root=base_root, mask=0b1000, cap=0, authority=bytes(32))
        blocked = store.materialize_plan(b"adaptive-test", b"obj-1", block_plan)
        assert blocked.payload == parts[3]
        # Unknown representation.
        with pytest.raises(StoreError):
            store.materialize_plan(
                b"adaptive-test", b"obj-1",
                session.encode_plan(
                    fidelity=5, root=bytes(range(32)), mask=0, cap=0,
                    authority=bytes(32)))
        # Transfer cap exceeded.
        with pytest.raises(StoreError):
            store.materialize_plan(
                b"adaptive-test", b"obj-1",
                session.encode_plan(
                    fidelity=5, root=base_root, mask=0xFF, cap=10,
                    authority=bytes(32)))
        # Mask on an opaque representation.
        with pytest.raises(StoreError):
            store.materialize_plan(
                b"adaptive-test", b"obj-1",
                session.encode_plan(
                    fidelity=1, root=reps[1].fields["root"], mask=0b1, cap=0,
                    authority=bytes(32)))


def test_legacy_v2_objects_synthesize_their_adaptive_view(session, store_path):
    with EmbeddedStore(store_path, session=session) as store:
        result = store.put_bound_object(
            domain_schema=b"adaptive-test",
            domain_identity=b"legacy",
            descriptor=b"d",
            payload=b"legacy-bytes",
            expected_generation=store.current_generation,
        )
        assert result.code == StoreResultCode.COMMITTED
        env = store.get_envelope(b"adaptive-test", b"legacy")
        assert env.fields["rep_count"] == 1
        assert env.fields["fidelity_bits"] == 33  # identity + exact
        assert env.fields["block_count"] == 1
        view = store.materialize(b"adaptive-test", b"legacy")
        assert view.payload == b"legacy-bytes"
        assert view.exact_verified
        whole = store.read_blocks(b"adaptive-test", b"legacy", 1)
        assert whole.payload == b"legacy-bytes"


def test_adaptive_corruption_fails_closed(session, store_path):
    payload, _parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
        logical = store.logical_id_for(b"adaptive-test", b"obj-1")
    manifest_raw = (store_path / "objects" / f"{logical.hex()}.manifest").read_bytes()
    assert len(manifest_raw) > 168  # v3 manifest carries sidecar digests
    envelope_digest = manifest_raw[-64:-32]
    envelope_path = store_path / "envelopes" / f"{envelope_digest.hex()}.envelope"
    pristine = envelope_path.read_bytes()
    raw = bytearray(pristine)
    raw[100] ^= 0xFF
    envelope_path.write_bytes(bytes(raw))
    with (
        EmbeddedStore(store_path, session=session) as store,
        pytest.raises(StoreIntegrityError),
    ):
        store.get_envelope(b"adaptive-test", b"obj-1")
    envelope_path.write_bytes(pristine)
    reps_digest = manifest_raw[-32:]
    reps_path = store_path / "representations" / f"{reps_digest.hex()}.reps"
    raw = bytearray(reps_path.read_bytes())
    raw[20] ^= 0x01
    reps_path.write_bytes(bytes(raw))
    with (
        EmbeddedStore(store_path, session=session) as store,
        pytest.raises(StoreIntegrityError),
    ):
        store.get_representations(b"adaptive-test", b"obj-1")


def test_adaptive_producer_input_is_rejected(session, store_path):
    payload, _parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        gen = store.current_generation
        overlapping = [
            BlockInput(index=0, tag=0, start=0, length=4096),
            BlockInput(index=1, tag=0, start=2048, length=4096),
        ]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-1",
                descriptor=b"d", payload=payload, expected_generation=gen,
                blocks=overlapping)
        gapped = [BlockInput(index=3, tag=0, start=0, length=4096)]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-2",
                descriptor=b"d", payload=payload, expected_generation=gen,
                blocks=gapped)
        negative = [BlockInput(index=0, tag=0, start=-8, length=4096)]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-2b",
                descriptor=b"d", payload=payload, expected_generation=gen,
                blocks=negative)
        overrun = [BlockInput(index=0, tag=0, start=0, length=len(payload) + 1)]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-2c",
                descriptor=b"d", payload=payload, expected_generation=gen,
                blocks=overrun)
        wild_tag = [BlockInput(index=0, tag=1 << 64, start=0, length=4096)]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-2d",
                descriptor=b"d", payload=payload, expected_generation=gen,
                blocks=wild_tag)
        wild_index = [BlockInput(index=64, tag=0, start=0, length=4096)]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-2e",
                descriptor=b"d", payload=payload, expected_generation=gen,
                blocks=wild_index)
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-3",
                descriptor=b"d", payload=payload, expected_generation=gen,
                representations=[RepresentationInput(
                    fidelity=5, codec="identity", payload=b"other-bytes")])
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-4",
                descriptor=b"d", payload=payload, expected_generation=gen,
                representations=[RepresentationInput(
                    fidelity=9, codec="identity", payload=payload)])
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-5",
                descriptor=b"d", payload=payload, expected_generation=gen,
                representations=[RepresentationInput(
                    fidelity=3, codec="zstd", payload=payload)])
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-6",
                descriptor=b"d", payload=payload, expected_generation=gen,
                synopsis=b"x" * (64 * 1024 + 1))
        duplicate_roots = [
            RepresentationInput(fidelity=3, codec="identity", payload=b"same"),
            RepresentationInput(fidelity=2, codec="identity", payload=b"same"),
        ]
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-7",
                descriptor=b"d", payload=payload, expected_generation=gen,
                representations=duplicate_roots)
        with pytest.raises(StoreError):
            store.put_bound_object(
                domain_schema=b"adaptive-test", domain_identity=b"bad-8",
                descriptor=b"d", payload=payload, expected_generation=gen,
                synopsis=b"")
        # Nothing was committed by the rejected puts.
        assert store.current_generation == gen


def test_adaptive_put_is_deterministic(session, store_path):
    payload, _parts = _segmented_payload()
    digests = []
    for attempt in range(2):
        path = store_path / f"det-{attempt}"
        with EmbeddedStore(path, session=session) as store:
            result = _commit_segmented(store, payload)
            assert result.code == StoreResultCode.COMMITTED
            logical = store.logical_id_for(b"adaptive-test", b"obj-1")
            manifest = (path / "objects" / f"{logical.hex()}.manifest").read_bytes()
            envelope = (path / "envelopes" / f"{manifest[-64:-32].hex()}.envelope").read_bytes()
            reps = (path / "representations" / f"{manifest[-32:].hex()}.reps").read_bytes()
            digests.append(hashlib.sha256(manifest + envelope + reps).hexdigest())
    assert digests[0] == digests[1]


def test_adaptive_commit_survives_crash_injection(session, store_path):
    payload, _parts = _segmented_payload(block_size=1024, blocks=8)

    def failpoint(name: str) -> None:
        if name == "during_chunk_staging":
            raise CrashInjected("injected")

    with (
        EmbeddedStore(store_path, session=session, failpoint=failpoint) as store,
        pytest.raises(CrashInjected),
    ):
        _commit_segmented(store, payload, block_size=1024)
    with EmbeddedStore(store_path, session=session) as store:
        assert store.current_generation == 0
        result = _commit_segmented(store, payload, block_size=1024)
        assert result.code == StoreResultCode.COMMITTED
        view = store.materialize(b"adaptive-test", b"obj-1")
        assert view.payload == payload
        assert view.exact_verified


def test_adaptive_reopen_round_trip(session, store_path):
    payload, parts = _segmented_payload()
    with EmbeddedStore(store_path, session=session) as store:
        _commit_segmented(store, payload)
    with EmbeddedStore(store_path, session=session) as store:
        assert store.verify()["ok"] is True
        view = store.read_blocks(b"adaptive-test", b"obj-1", 0b1000)
        assert view.payload == parts[3]
        coded = store.materialize(
            b"adaptive-test", b"obj-1", intent=session.encode_intent(5, transfer=1000))
        assert coded.payload == payload
        assert coded.exact_verified
