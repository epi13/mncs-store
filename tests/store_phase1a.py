"""Phase-1a local store lifecycle driver (host-side, single node).

SCOPE CONTRACT (see RFC 0016): this module owns ONLY transport, layout,
and atomicity mechanics — directories, files, fsync, atomic rename,
serial counters, and orchestration. EVERY semantic byte (descriptor
encodings, frames, digests, manifests, root identities, typed codecs) is
produced by executing mncs-language programs in src/store/*.mncs through
`mncs_exec.call_many`. This module never encodes, decodes, hashes, or
validates storage semantics itself; it passes opaque bytes between MNCS
calls and files, and compares MNCS-produced values for control flow.

Because mncs-language currently exposes no file-write, fsync, rename, or
directory primitive, the lifecycle mechanics below cannot yet be expressed
in-language (pressures P1-001..P1-003). When those primitives land, this
driver is the migration checklist: each method maps to a required
language capability documented in its docstring.
"""

import hashlib
import os

from mncs_exec import (
    BY,
    BYTES,
    U16,
    U32,
    U64,
    StoreHarnessError,
    as_bool,
    as_bytes,
    as_int,
    call_many,
    require_returned,
)

# Lifecycle executions run on research-bytecode: it is the only backend
# realizing host effects (sha256_digest), which digests depend on (P1-B02).
DEFAULT_BACKEND = "mncs-research-bytecode"

SRC_CHUNK = "src/store/chunk.mncs"
SRC_DESC = "src/store/descriptor.mncs"
SRC_MANIFEST = "src/store/manifest.mncs"
SRC_IDENTITY = "src/store/identity.mncs"
SRC_READ_VERIFY = "src/store/read_verify.mncs"

MOD_CHUNK = "store.chunk.v1"
MOD_DESC = "store.descriptor.v1"
MOD_MANIFEST = "store.manifest.v1"
MOD_IDENTITY = "store.identity.v1"
MOD_READ_VERIFY = "store.read_verify.v1"

GRANT_CHUNK = ["--grant-crypto", "store_chunk"]
GRANT_MANIFEST = ["--grant-crypto", "store_manifest"]

MAGIC_META = b"MS"
MAGIC_CURRENT = b"MC"
MAGIC_GEN = b"MG"
STORE_VERSION = 1
NAMESPACE_PHASE1A = 1

# Host value-class vocabulary: names a (type_tag, dim, payload_width,
# frame_width) tuple for orchestration. Canonical descriptor/frame bytes
# always come from MNCS; this table only selects WHICH mncs function to
# call and how many bytes to transport.
CLASSES = {
    "u32": {"tag": 1, "dim": 1, "payload": 4, "frame": 8,
            "frame_fn": "frame_u32", "digest_fn": "digest_u32",
            "verify_fn": "verify_frame4", "decode_fn": "get_u32"},
    "u64": {"tag": 2, "dim": 1, "payload": 8, "frame": 12,
            "frame_fn": "frame_u64", "digest_fn": "digest_u64",
            "verify_fn": "verify_frame8", "decode_fn": "get_u64"},
    "pair": {"tag": 4, "dim": 2, "payload": 8, "frame": 12,
             "frame_fn": "frame_pair", "digest_fn": "digest_pair",
             "verify_fn": "verify_frame8", "decode_fn": None},
    "blob8": {"tag": 3, "dim": 8, "payload": 8, "frame": 12,
              "frame_fn": "frame8", "digest_fn": "digest8",
              "verify_fn": "verify_frame8", "decode_fn": "payload8"},
    "blob16": {"tag": 3, "dim": 16, "payload": 16, "frame": 20,
               "frame_fn": "frame16", "digest_fn": "digest16",
               "verify_fn": "verify_frame16", "decode_fn": "payload16"},
    "blob32": {"tag": 3, "dim": 32, "payload": 32, "frame": 36,
               "frame_fn": "frame32", "digest_fn": "digest32",
               "verify_fn": "verify_frame32", "decode_fn": "payload32"},
    "empty": {"tag": 3, "dim": 0, "payload": 0, "frame": 4,
              "frame_fn": "frame_empty", "digest_fn": "digest_empty",
              "verify_fn": "verify_frame_empty", "decode_fn": None},
}


class StoreError(Exception):
    pass


class NotFoundError(StoreError):
    """Requested identity has no committed state (distinct from corruption)."""


class IntegrityError(StoreError):
    """Bytes failed digest, framing, or structural validation."""


class TypeMismatchError(StoreError):
    """Stored descriptor type differs from the requested typed read."""


class UnsupportedWidthError(StoreError):
    pass


class Engine:
    """Batched MNCS invocation with I/O instrumentation.

    Counts CLI invocations and transported bytes so the test report can
    state the (considerable) subprocess-shell-out cost honestly
    (pressure P1-016): there is currently no in-process call boundary.
    """

    def __init__(self, backend=DEFAULT_BACKEND):
        self.backend = backend
        self.invocations = 0
        self.bytes_out = 0  # argument bytes sent to MNCS
        self.bytes_in = 0  # result bytes received from MNCS

    def run(self, source, module, calls, grants=()):
        """Run one batched corpus; return {case_id: case_result}."""
        self.invocations += 1
        out = call_many(source, module, calls, self.backend, grants=grants)
        for cid, fn, args in calls:
            self.bytes_out += len(str(args))
        for case in out.values():
            self.bytes_in += len(str(case.get("returned")))
        return out

    @staticmethod
    def check(cases, cids):
        for cid in cids:
            case = cases[cid]
            if case.get("status") != "returned" or not case.get("returned"):
                raise StoreHarnessError(
                    f"batch case {cid}: status={case.get('status')} "
                    f"failure={case.get('failure_reason')}"
                )
        return cases


def _fsync_file(path):
    with open(path, "rb") as f:
        os.fsync(f.fileno())


def _write_sync(path, data):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_file(path)


def _write_create_exclusive(path, data):
    """Create-or-verify: never overwrite committed bytes with different bytes.

    Returns True when bytes were written, False when identical bytes already
    existed (idempotent re-put). Raises IntegrityError when different bytes
    already exist under the same content name (invariant 2 enforcement).
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        fd = os.open(path, flags, 0o644)
    except FileExistsError:
        with open(path, "rb") as f:
            existing = f.read()
        if existing == data:
            return False
        raise IntegrityError(
            f"{path}: committed bytes differ under identical content name; "
            "refusing to overwrite immutable content"
        )
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class StorePhase1a:
    """Single-node persistent object core, Phase 1a.

    Value classes: u32, u64, pair, blob8/16/32, empty. Each put mints a new
    logical ObjectId, stores one content-addressed chunk plus one manifest
    root, and commits a new generation. Objects are immutable after commit.
    """

    def __init__(self, path, engine=None):
        self.path = os.path.abspath(path)
        self.engine = engine or Engine()
        self._meta = None
        self._gen = None
        self._mapping = None

    # -- layout ---------------------------------------------------------
    def _p(self, *parts):
        return os.path.join(self.path, *parts)

    # -- lifecycle ------------------------------------------------------
    @classmethod
    def create(cls, path, engine=None):
        self = cls(path, engine)
        for sub in ("objects", "chunks", "generations", "temp"):
            os.makedirs(self._p(sub), exist_ok=True)
        meta = (MAGIC_META + bytes([STORE_VERSION, 0])
                + NAMESPACE_PHASE1A.to_bytes(4, "big")
                + (1).to_bytes(8, "big"))
        _write_sync(self._p("meta"), meta)
        self._write_generation(0, {})
        self._write_current(0)
        _fsync_dir(self.path)
        return cls._reopen(path, engine)

    @classmethod
    def _reopen(cls, path, engine=None):
        self = cls(path, engine)
        self._open()
        return self

    @classmethod
    def open(cls, path, engine=None):
        self = cls(path, engine)
        self._open()
        return self

    def _open(self):
        # Requires P1-001/P1-002/P1-003 in-language: directory/file reads,
        # durable barriers, and atomic publication are host-executed here.
        try:
            with open(self._p("meta"), "rb") as f:
                meta = f.read()
        except FileNotFoundError:
            raise NotFoundError(f"{self.path}: not a Phase-1a store (no meta)")
        if len(meta) != 16 or meta[0:2] != MAGIC_META:
            raise IntegrityError(f"{self.path}: bad store metadata magic")
        if meta[2] != STORE_VERSION:
            raise StoreError(
                f"{self.path}: unsupported store version {meta[2]}; "
                "refusing to guess (unknown versions fail explicitly)"
            )
        namespace = int.from_bytes(meta[4:8], "big")
        if namespace != NAMESPACE_PHASE1A:
            raise StoreError(f"{self.path}: unexpected namespace {namespace}")
        next_serial = int.from_bytes(meta[8:16], "big")
        try:
            with open(self._p("current"), "rb") as f:
                current = f.read()
        except FileNotFoundError:
            raise IntegrityError(f"{self.path}: missing current-generation pointer")
        if len(current) != 8 or current[0:2] != MAGIC_CURRENT or current[2] != 1:
            raise IntegrityError(f"{self.path}: bad current-generation file")
        gen = int.from_bytes(current[4:8], "big")
        mapping = self._read_generation(gen)
        # Self-healing serial counter: if objects exist beyond the persisted
        # counter (torn meta write), advance instead of reissuing serials.
        max_serial = 0
        for obj_hex in self._list_objects():
            serial = int.from_bytes(bytes.fromhex(obj_hex)[4:12], "big")
            max_serial = max(max_serial, serial)
        if max_serial >= next_serial:
            next_serial = max_serial + 1
        self._meta = {"namespace": namespace, "next_serial": next_serial}
        self._gen = gen
        self._mapping = mapping

    def _list_objects(self):
        d = self._p("objects")
        try:
            return [n for n in os.listdir(d) if len(n) == 24]
        except FileNotFoundError:
            return []

    def _read_generation(self, gen):
        try:
            with open(self._p("generations", f"{gen:08x}"), "rb") as f:
                data = f.read()
        except FileNotFoundError:
            raise IntegrityError(
                f"{self.path}: committed generation {gen} file missing"
            )
        if len(data) < 8 or data[0:2] != MAGIC_GEN or data[2] != 1:
            raise IntegrityError(
                f"{self.path}: generation {gen} header invalid"
            )
        count = int.from_bytes(data[4:8], "big")
        if len(data) != 8 + 44 * count:
            raise IntegrityError(
                f"{self.path}: generation {gen} truncated "
                f"(want {8 + 44 * count} bytes, have {len(data)})"
            )
        mapping = {}
        for i in range(count):
            rec = data[8 + 44 * i: 8 + 44 * (i + 1)]
            mapping[rec[0:12].hex()] = rec[12:44].hex()
        return mapping

    def _write_generation(self, gen, mapping):
        # Requires in-language atomic rename + fsync to migrate (P1-002/003).
        items = sorted(mapping.items())
        data = bytearray(MAGIC_GEN + bytes([STORE_VERSION, 0])
                         + len(items).to_bytes(4, "big"))
        for obj_hex, root_hex in items:
            data += bytes.fromhex(obj_hex) + bytes.fromhex(root_hex)
        _write_sync(self._p("generations", f"{gen:08x}"), bytes(data))

    def _write_current(self, gen):
        data = MAGIC_CURRENT + bytes([STORE_VERSION, 0]) + gen.to_bytes(4, "big")
        _write_sync(self._p("current"), data)

    def _persist_meta(self):
        meta = (MAGIC_META + bytes([STORE_VERSION, 0])
                + self._meta["namespace"].to_bytes(4, "big")
                + self._meta["next_serial"].to_bytes(8, "big"))
        _write_sync(self._p("meta"), meta)

    def close(self):
        # Requires deterministic resource release in-language (no file-handle
        # or mapping state may survive close; reopen must re-verify).
        if self._meta is None:
            return  # idempotent: double close is a no-op, not an error
        self._persist_meta()
        _fsync_dir(self.path)
        self._meta = None
        self._gen = None
        self._mapping = None

    # -- MNCS-backed semantic steps -------------------------------------
    def _mncs_put_plan(self, items):
        """Compute descriptor/frame/digest/manifest/root bytes via MNCS.

        `items`: list of (key, class_name, value_args). Returns dict key ->
        {oid, desc, frame, digest, root, root_id} with all bytes MNCS-made.
        Six batched invocations total regardless of item count.

        Serial discipline: serials are consumed (counter advanced) before
        the commit. A failed put therefore burns serials, leaving gaps.
        Gaps are safe: serials need uniqueness, not contiguity, and the
        open-time healing scan advances past any committed serial after a
        torn meta write. Reused serials cannot corrupt because object
        files are create-exclusive (same bytes: idempotent; different
        bytes: fatal).
        """
        engine = self.engine
        # 1. descriptors (one call per item, one invocation)
        desc_calls = []
        for key, cls, _ in items:
            info = CLASSES[cls]
            desc_calls.append(
                (f"desc:{key}", "describe_for_put",
                 [BY(info["tag"]), U16(info["dim"])]))
        descs = engine.check(
            engine.run(SRC_DESC, MOD_DESC, desc_calls), [c[0] for c in desc_calls])
        # 2. frames
        frame_calls = []
        for key, cls, value in items:
            info = CLASSES[cls]
            frame_calls.append((f"frame:{key}", info["frame_fn"], value))
        frames = engine.check(
            engine.run(SRC_CHUNK, MOD_CHUNK, frame_calls),
            [c[0] for c in frame_calls])
        # 3. digests
        digest_calls = []
        for key, cls, value in items:
            info = CLASSES[cls]
            digest_calls.append((f"digest:{key}", info["digest_fn"], value))
        digests = engine.check(
            engine.run(SRC_CHUNK, MOD_CHUNK, digest_calls, grants=GRANT_CHUNK),
            [c[0] for c in digest_calls])
        # 4. object encodings
        oid_calls = []
        for key, cls, _ in items:
            serial = self._meta["next_serial"]
            self._meta["next_serial"] += 1
            oid_calls.append(
                (f"oid:{key}", "object_encode",
                 [U32(self._meta["namespace"]), U64(serial)]))
        oids = engine.check(
            engine.run(SRC_IDENTITY, MOD_IDENTITY, oid_calls),
            [c[0] for c in oid_calls])
        # 5. manifests
        man_calls = []
        for key, cls, _ in items:
            info = CLASSES[cls]
            man_calls.append((
                f"man:{key}", "encode",
                [BY(info["tag"]),
                 BYTES(as_bytes(require_returned(descs[f"desc:{key}"],
                                                f"desc:{key}"))),
                 U32(info["payload"]), U32(info["payload"]),
                 BYTES(as_bytes(require_returned(digests[f"digest:{key}"],
                                                f"digest:{key}")))]))
        mans = engine.check(
            engine.run(SRC_MANIFEST, MOD_MANIFEST, man_calls),
            [c[0] for c in man_calls])
        # 6. root identities
        rid_calls = []
        for key, cls, _ in items:
            rid_calls.append((
                f"rid:{key}", "root_id",
                [BYTES(as_bytes(require_returned(mans[f"man:{key}"],
                                                f"man:{key}")))]))
        rids = engine.check(
            engine.run(SRC_MANIFEST, MOD_MANIFEST, rid_calls,
                       grants=GRANT_MANIFEST),
            [c[0] for c in rid_calls])
        plan = {}
        for key, cls, _ in items:
            info = CLASSES[cls]
            frame = as_bytes(require_returned(frames[f"frame:{key}"], key))
            digest = as_bytes(require_returned(digests[f"digest:{key}"], key))
            root = as_bytes(require_returned(mans[f"man:{key}"], key))
            root_id = as_bytes(require_returned(rids[f"rid:{key}"], key))
            assert len(root_id) == 32, f"{key}: root identity must be 32 bytes"
            assert len(frame) == info["frame"], f"{key}: frame width mismatch"
            plan[key] = {
                "oid": as_bytes(require_returned(oids[f"oid:{key}"], key)),
                "desc": as_bytes(require_returned(descs[f"desc:{key}"], key)),
                "frame": frame,
                "digest": digest,
                "root": root,
                "root_id": root_id,
            }
        return plan

    def _commit(self, plan):
        """Persist staged objects and publish a new generation atomically."""
        mapping = dict(self._mapping)
        for key, p in plan.items():
            chunk_path = self._p("chunks", p["digest"].hex())
            _write_create_exclusive(chunk_path, p["frame"])
            obj_path = self._p("objects", p["oid"].hex())
            _write_create_exclusive(obj_path, p["root"])
            mapping[p["oid"].hex()] = p["root_id"].hex()
        new_gen = self._gen + 1
        self._write_generation(new_gen, mapping)
        self._persist_meta()
        self._write_current(new_gen)
        _fsync_dir(self.path)
        _fsync_dir(self._p("generations"))
        self._gen = new_gen
        self._mapping = mapping
        return {k: v["oid"] for k, v in plan.items()}

    def put(self, cls, value_args):
        """Put one value; returns the logical ObjectId bytes (12)."""
        if self._mapping is None:
            raise StoreError("store is closed")
        if cls not in CLASSES:
            raise UnsupportedWidthError(f"unknown value class {cls!r}")
        plan = self._mncs_put_plan([("one", cls, value_args)])
        oids = self._commit(plan)
        return oids["one"]

    def put_many(self, items):
        """Put N (class_name, value_args) items in one commit; returns oids."""
        if self._mapping is None:
            raise StoreError("store is closed")
        plan = self._mncs_put_plan(
            [(f"item{i}", cls, val) for i, (cls, val) in enumerate(items)])
        oids = self._commit(plan)
        return [oids[f"item{i}"] for i in range(len(items))]

    # -- typed convenience puts ------------------------------------------
    def put_u32(self, value):
        return self.put("u32", [U32(value)])

    def put_u64(self, value):
        return self.put("u64", [U64(value)])

    def put_pair(self, first, second):
        return self.put("pair", [U32(first), U32(second)])

    def put_blob(self, data):
        data = bytes(data)
        try:
            cls = {8: "blob8", 16: "blob16", 32: "blob32"}[len(data)]
        except KeyError:
            raise UnsupportedWidthError(
                f"blob length {len(data)}: Phase 1a supports 8/16/32-byte "
                "payloads (explicit rejection, never silent padding)")
        return self.put(cls, [BYTES(data)])

    def put_empty(self):
        return self.put("empty", [])

    # -- verified read path ------------------------------------------------
    def load_committed(self, oid, mapping=None):
        """Host transport: mapping -> root file bytes + chunk file bytes.

        `mapping` selects the generation to resolve through (default: the
        live current mapping); snapshot reads pass the bound generation's
        mapping so concurrent publication cannot move the read (Phase 2).
        

        No semantic check here; use verify_batch before interpreting.
        Raises NotFoundError for absent logical state, IntegrityError for
        absent committed files (a generation references bytes that are not
        on disk: corruption, never "not found").
        """
        mapping = self._mapping if mapping is None else mapping
        if mapping is None:
            raise StoreError("store is closed")
        ohex = bytes(oid).hex()
        if ohex not in mapping:
            raise NotFoundError(f"unknown object {ohex}")
        root_hex = mapping[ohex]
        try:
            with open(self._p("objects", ohex), "rb") as f:
                root = f.read()
        except FileNotFoundError:
            raise IntegrityError(
                f"object {ohex}: committed in generation {self._gen} but "
                "object file is missing")
        # Transport-level framing gate (mirrors the generation length check
        # in _read_generation): roots are exactly 64 bytes. Anything else
        # is a torn or appended write. MNCS validate() would report code 1
        # for short views, but over-long views may not bind to the
        # [byte; up_to 64] parameter at all — reject here so every malformed
        # root surfaces as IntegrityError, never a harness transport error.
        if len(root) != 64:
            raise IntegrityError(
                f"object {ohex}: root file is {len(root)} bytes, want 64")
        chunk_digest = root[32:64] if len(root) >= 64 else None
        frame = None
        if chunk_digest is not None:
            try:
                with open(self._p("chunks", chunk_digest.hex()), "rb") as f:
                    frame = f.read()
            except FileNotFoundError:
                frame = None
        return {"ohex": ohex, "root_hex": root_hex, "root": root,
                "chunk_digest": chunk_digest, "frame": frame}

    def verify_batch(self, entries):
        """MNCS verification over loaded entries (4 invocations total).

        `entries`: list of load_committed dicts. Returns {ohex: entry} for
        verified entries. Raises IntegrityError on any failed check. Never
        raises NotFoundError (mapping misses fail in load_committed) or
        TypeMismatchError (type checks are the caller's decode-stage job).
        """
        engine = self.engine
        # 1. manifest structural validation (MNCS).
        out = engine.check(
            engine.run(SRC_MANIFEST, MOD_MANIFEST,
                       [(e["ohex"], "validate", [BYTES(e["root"])])
                        for e in entries]),
            [e["ohex"] for e in entries])
        for e in entries:
            code = as_int(require_returned(out[e["ohex"]], "manifest"))
            if code != 0:
                raise IntegrityError(
                    f"object {e['ohex']}: manifest validation code {code}")
        # 2. root identity match: MNCS recomputation vs generation mapping.
        out = engine.check(
            engine.run(SRC_MANIFEST, MOD_MANIFEST,
                       [(e["ohex"], "verify_root",
                         [BYTES(e["root"]),
                          BYTES(bytes.fromhex(e["root_hex"]))])
                        for e in entries],
                       grants=GRANT_MANIFEST),
            [e["ohex"] for e in entries])
        for e in entries:
            if not as_bool(require_returned(out[e["ohex"]], "root-id")):
                raise IntegrityError(
                    f"object {e['ohex']}: root identity mismatch (file bytes "
                    "differ from the committed generation mapping)")
        # 3. chunk presence + framing (MNCS).
        for e in entries:
            if e["frame"] is None:
                have = e["chunk_digest"].hex() if e["chunk_digest"] else "?"
                raise IntegrityError(
                    f"object {e['ohex']}: referenced chunk {have} is missing")
        out = engine.check(
            engine.run(SRC_CHUNK, MOD_CHUNK,
                       [(e["ohex"], "frame_valid", [BYTES(e["frame"])])
                        for e in entries]),
            [e["ohex"] for e in entries])
        for e in entries:
            fcode = as_int(require_returned(out[e["ohex"]], "framing"))
            if fcode != 0:
                raise IntegrityError(
                    f"object {e['ohex']}: chunk framing code {fcode}")
        # 4. chunk digest match (MNCS recomputation).
        width_fn = {4: "verify_frame4", 8: "verify_frame8",
                    16: "verify_frame16", 32: "verify_frame32",
                    0: "verify_frame_empty"}
        calls = []
        for e in entries:
            width = len(e["frame"]) - 4
            if width not in width_fn:
                raise IntegrityError(
                    f"object {e['ohex']}: unsupported frame width {width}")
            calls.append((e["ohex"], width_fn[width],
                          [BYTES(e["frame"]), BYTES(e["chunk_digest"])]))
        out = engine.check(
            engine.run(SRC_CHUNK, MOD_CHUNK, calls, grants=GRANT_CHUNK),
            [e["ohex"] for e in entries])
        for e in entries:
            if not as_bool(require_returned(out[e["ohex"]], "chunk-digest")):
                raise IntegrityError(
                    f"object {e['ohex']}: chunk identity does not match "
                    "contents (corrupted chunk)")
        return {e["ohex"]: e for e in entries}

    def _read_and_verify(self, oid, mapping=None):
        """Single-object convenience over load_committed + verify_batch."""
        entry = self.load_committed(oid, mapping=mapping)
        verified = self.verify_batch([entry])
        return verified[entry["ohex"]]

    def _get_one(self, oid, expected_tag, op, mapping=None):
        entry = self._read_and_verify(oid, mapping=mapping)
        self._require_types({entry["ohex"]: entry}, {entry["ohex"]: expected_tag})
        return entry

    def _require_types(self, verified, expected):
        """Check manifest type tags (MNCS) for verified entries.

        `expected`: {ohex: tag}. Raises TypeMismatchError on any mismatch.
        """
        out = self.engine.check(
            self.engine.run(SRC_MANIFEST, MOD_MANIFEST,
                            [(ohex, "root_type", [BYTES(e["root"])])
                             for ohex, e in verified.items()]),
            list(verified))
        for ohex, tag in expected.items():
            got = as_int(require_returned(out[ohex], "root-type"))
            if got != tag:
                raise TypeMismatchError(
                    f"object {ohex}: stored type tag {got} != requested {tag}")

    def get_u32(self, oid, mapping=None):
        entry = self._get_one(oid, 1, "get_u32", mapping=mapping)
        out = self.engine.check(
            self.engine.run(SRC_CHUNK, MOD_CHUNK,
                            [("g", "get_u32", [BYTES(entry["frame"])])]), ["g"])
        return as_int(require_returned(out["g"], "get_u32"))

    def get_u64(self, oid, mapping=None):
        entry = self._get_one(oid, 2, "get_u64", mapping=mapping)
        out = self.engine.check(
            self.engine.run(SRC_CHUNK, MOD_CHUNK,
                            [("g", "get_u64", [BYTES(entry["frame"])])]), ["g"])
        return as_int(require_returned(out["g"], "get_u64"))

    def get_pair(self, oid, mapping=None):
        entry = self._get_one(oid, 4, "get_pair", mapping=mapping)
        out = self.engine.check(
            self.engine.run(
                SRC_CHUNK, MOD_CHUNK,
                [("a", "get_pair_first", [BYTES(entry["frame"])]),
                 ("b", "get_pair_second", [BYTES(entry["frame"])])]), ["a", "b"])
        return (as_int(require_returned(out["a"], "pair-first")),
                as_int(require_returned(out["b"], "pair-second")))

    def get_blob(self, oid, mapping=None):
        entry = self._get_one(oid, 3, "get_blob", mapping=mapping)
        width = len(entry["frame"]) - 4
        if width == 0:
            # A verified empty frame carries zero payload bytes; returning
            # b"" involves no interpretation (the frame was validated and
            # digest-matched above).
            return b""
        try:
            decode_fn = {8: "payload8", 16: "payload16", 32: "payload32"}[width]
        except KeyError:
            raise IntegrityError(
                f"object {entry['ohex']}: verified frame has unexpected "
                f"payload width {width}")
        out = self.engine.check(
            self.engine.run(SRC_CHUNK, MOD_CHUNK,
                            [("g", decode_fn, [BYTES(entry["frame"])])]), ["g"])
        return as_bytes(require_returned(out["g"], "get_blob"))

    def get_many(self, wants, mapping=None):
        """Batched typed read: `wants` = list of (key, oid, getter-name).

        Verifies all entries in 4 MNCS invocations, checks types in 1,
        then decodes grouped by class. Returns {key: value}.
        """
        entries = [self.load_committed(oid, mapping=mapping)
                   for _, oid, _ in wants]
        verified = self.verify_batch(entries)
        self._require_types(
            verified,
            {e["ohex"]: {"u32": 1, "u64": 2, "pair": 4, "pair-first": 4,
                         "blob": 3}[g]
             for (_, _, g), e in zip(wants, entries)})
        decode_calls = []
        for (key, _, getter), e in zip(wants, entries):
            frame = verified[e["ohex"]]["frame"]
            if getter == "u32":
                decode_calls.append((key, "get_u32", [BYTES(frame)]))
            elif getter == "u64":
                decode_calls.append((key, "get_u64", [BYTES(frame)]))
            elif getter == "pair-first":
                decode_calls.append((key + ":first", "get_pair_first",
                                     [BYTES(frame)]))
                decode_calls.append((key + ":second", "get_pair_second",
                                     [BYTES(frame)]))
            elif getter == "blob":
                width = len(frame) - 4
                if width == 0:
                    continue  # verified empty frame: value is b"", no call
                try:
                    fn = {8: "payload8", 16: "payload16",
                          32: "payload32"}[width]
                except KeyError:
                    raise IntegrityError(
                        f"object {e['ohex']}: verified frame has unexpected "
                        f"payload width {width}")
                decode_calls.append((key, fn, [BYTES(frame)]))
            else:
                raise StoreError(f"unknown getter {getter!r}")
        out = self.engine.check(
            self.engine.run(SRC_CHUNK, MOD_CHUNK, decode_calls),
            [c[0] for c in decode_calls])
        values = {}
        for (key, _, getter), e in zip(wants, entries):
            if getter in ("u32", "u64"):
                values[key] = as_int(require_returned(out[key], key))
            elif getter == "blob":
                if len(verified[e["ohex"]]["frame"]) == 4:
                    values[key] = b""
                else:
                    values[key] = as_bytes(require_returned(out[key], key))
            elif getter == "pair-first":
                values[key] = (
                    as_int(require_returned(out[key + ":first"], key)),
                    as_int(require_returned(out[key + ":second"], key)))
        return values

    def verify(self, oid):
        """First-class integrity verification; True or raises."""
        self._read_and_verify(oid)
        return True

    def verify_all(self):
        for ohex in sorted(self._mapping):
            self.verify(bytes.fromhex(ohex))
        return len(self._mapping)

    # -- independent hash oracle (verification only) -----------------------
    @staticmethod
    def sha256_oracle(data):
        """Independent SHA-256 cross-check. Test oracle only: canonical
        digests always come from MNCS sha256_digest executions."""
        return hashlib.sha256(bytes(data)).digest()

    # -- stats --------------------------------------------------------------
    def chunk_files(self):
        return sorted(os.listdir(self._p("chunks")))

    def generation(self):
        return self._gen
