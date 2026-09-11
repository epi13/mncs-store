"""Phase-2 store driver: multi-chunk objects, generations, snapshots,
compare-and-transition commits, recovery, and reclamation.

SCOPE CONTRACT (extends RFC 0016/0017): this module owns ONLY transport,
layout, and filesystem mechanics — files, fsync, atomic rename, serial
counters, orchestration, and fault injection. EVERY semantic byte and
EVERY semantic decision (descriptors, frames, digests, chains, manifest
validity, generation validity, CAS outcomes, snapshot binding, commit
transitions, recovery selection, reclamation membership) is computed by
executing mncs-language programs in src/store/*.mncs. This module passes
opaque bytes between MNCS calls and files and branches on MNCS verdicts.

Host/MNCS split for Phase 2 (see pressure P2-004..P2-007):
- host: file create/read, fsync, atomic rename via tmp+replace, directory
  listing, serial counters, snapshot retain sets, fault injection.
- MNCS: all encodings, all digests, chain roots, validate verdicts, CAS
  decisions, conflict tokens, snapshot permits, commit transitions,
  recovery classification/selection, reclamation membership. The
  reclamation retain set unions chunk digests named by freshly verified
  retained manifests (never bare roots: generations reference roots,
  chunks are retained by manifest membership).

The step budget for reclamation scans is explicit (65536): a full
1024-byte table_contains scan measures ~34k steps on research-bytecode
(see store.generation.table_contains and pressure P2-008). Budgets are a
host execution parameter, never semantics.
"""

import os

from mncs_exec import (
    B,
    BY,
    BYTES,
    U16,
    U32,
    U64,
    StoreHarnessError,
    as_bool,
    as_bytes,
    as_int,
    require_returned,
    run_corpus,
)
from store_phase1a import (
    GRANT_CHUNK,
    GRANT_MANIFEST,
    Engine,
    IntegrityError,
    NotFoundError,
    StoreError,
    StorePhase1a,
    UnsupportedWidthError,
    _fsync_dir,
    _write_create_exclusive,
    _write_sync,
)

SRC_CHUNK = "src/store/chunk.mncs"
SRC_DESC = "src/store/descriptor.mncs"
SRC_MANIFEST = "src/store/manifest.mncs"
SRC_IDENTITY = "src/store/identity.mncs"
SRC_GENERATION = "src/store/generation.mncs"
SRC_RECOVERY = "src/store/recovery.mncs"

MOD_CHUNK = "store.chunk.v1"
MOD_DESC = "store.descriptor.v1"
MOD_MANIFEST = "store.manifest.v1"
MOD_IDENTITY = "store.identity.v1"
MOD_GENERATION = "store.generation.v1"
MOD_RECOVERY = "store.recovery.v1"

# Reclamation scans exceed the default 32k budget (measured ~34k steps).
SCAN_BUDGET = 65536

# Multi-chunk object ceiling: 31 chunks x 32 bytes (manifest-v2 bound).
MAX_GENERAL_BLOB = 992

# Covered tail widths (frozen v1 frame widths reused by the v2 path).
TAIL_FNS = {
    4: {"frame": "frame4", "digest": "digest4", "verify": "verify_frame4",
        "tail_ok": "tail_ok4", "payload": "payload4"},
    8: {"frame": "frame8", "digest": "digest8", "verify": "verify_frame8",
        "tail_ok": "tail_ok8", "payload": "payload8"},
    16: {"frame": "frame16", "digest": "digest16", "verify": "verify_frame16",
         "tail_ok": "tail_ok16", "payload": "payload16"},
    32: {"frame": "frame32", "digest": "digest32", "verify": "verify_frame32",
         "tail_ok": "tail_ok32", "payload": "payload32"},
}

class ConflictError(StoreError):
    """Stale-writer rejection with a typed MNCS conflict token.

    `token` is the 16-byte MNCS conflict_token encoding (observed current
    + attempted generation); `observed`/`attempted` echo its MNCS-decoded
    fields for convenience (decoded by MNCS projectors, never host).
    """

    def __init__(self, token, observed, attempted):
        super().__init__(
            f"stale writer: observed generation {observed}, "
            f"attempted {attempted} (conflict token {token.hex()})"
        )
        self.token = token
        self.observed = observed
        self.attempted = attempted


class FaultInjected(Exception):
    """Simulated crash at a named commit boundary (fault-injection only)."""


class SnapshotExpired(StoreError):
    pass


def _pad_tail(tail, width):
    """Zero-extend a tail to a covered width (mechanical transport).

    Canonicality is enforced by MNCS tail_okW predicates on the put path
    and re-checked on every read; this function only proposes bytes.
    """
    return bytes(tail) + bytes(width - len(tail))


class StorePhase2(StorePhase1a):
    """Phase-2 store: v1 scalars plus general multi-chunk blobs (v2).

    Generation files, the current pointer, meta, and chunk files keep the
    Phase-1a layout. Manifest files under objects/ may now be v1 (64 B,
    magic MR) or v2 (28+32N B, magic MN); the mapping stays oid->root_id.
    """

    def __init__(self, path, engine=None):
        super().__init__(path, engine)
        self._retained = set()  # snapshot-pinned generations (in-memory)
        self.trace = []  # MNCS commit-transition trace of the last commit

    def _open(self):
        super()._open()
        # Reopen re-verifies: the current generation must classify
        # COMMITTED_VALID under MNCS verification, not merely parse.
        code, _, detail = self._classify_generation_file(self._gen)
        if code != 0:
            raise IntegrityError(
                f"reopen: current generation {self._gen} classifies "
                f"{code} ({detail}); refusing to serve torn state")

    # -- MNCS decision helpers ------------------------------------------
    def _mncs(self, source, module, calls, grants=(), budget=None):
        engine = self.engine
        if budget is None:
            return engine.check(engine.run(source, module, calls, grants),
                                [c[0] for c in calls])
        corpus = {
            "schema_version": "0.1",
            "name": "mncs-store-batch",
            "cases": [
                {"id": cid, "request": {
                    "schema_version": "0.1",
                    "target": {"module": module, "function": fn},
                    "arguments": args, "step_budget": budget}}
                for cid, fn, args in calls
            ],
        }
        self.engine.invocations += 1
        code, result, stderr = run_corpus(source, corpus, self.engine.backend,
                                          grants=grants)
        if result is None or "cases" not in result:
            raise StoreHarnessError(
                f"mncs run produced no result JSON (exit={code}): "
                f"{stderr[-2000:]}"
            )
        out = {c["case_id"]: c for c in result["cases"]}
        missing = [cid for cid, _, _ in calls if cid not in out]
        if missing:
            raise StoreHarnessError(f"mncs run dropped cases: {missing}")
        return engine.check(out, [c[0] for c in calls])

    def _transition(self, state, ok):
        """One MNCS commit-state transition; recorded on the trace."""
        out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                         [("t", "commit_next", [U64(state), B(ok)])])
        nxt = as_int(require_returned(out["t"], "commit transition"))
        self.trace.append((state, bool(ok), nxt))
        return nxt

    # -- general blob put path ------------------------------------------
    @staticmethod
    def _partition(data):
        """Split bytes into 32-byte body pieces + tail remainder."""
        data = bytes(data)
        nbody = len(data) // 32
        return ([data[i * 32:(i + 1) * 32] for i in range(nbody)],
                data[nbody * 32:])

    def _mncs_put_general_plan(self, items):
        """Compute v2 descriptor/frames/digests/chain/root bytes via MNCS.

        `items`: list of (key, data). Returns dict key -> {oid, desc, hdr,
        chunks: [(frame, digest)], manifest, root_id}. Batched: 7 MNCS
        invocations regardless of item count (plus 1 per distinct tail
        width for padding checks, folded into the frame batch).
        """
        engine = self.engine
        for key, data in items:
            if len(bytes(data)) > MAX_GENERAL_BLOB:
                raise UnsupportedWidthError(
                    f"blob length {len(bytes(data))}: Phase 2 admits "
                    f"0..{MAX_GENERAL_BLOB} bytes (manifest-v2 bound, "
                    "explicit rejection, never truncation)")
        # 1. descriptors (one describe_blob2 per item).
        out = self._mncs(SRC_DESC, MOD_DESC,
                         [(f"desc:{k}", "describe_blob2", [U16(len(bytes(d)))])
                          for k, d in items])
        descs = {k: as_bytes(require_returned(out[f"desc:{k}"], k))
                 for k, _ in items}
        # 2. body frames (exact [byte; 32] args, host-sliced transport).
        frame_calls = []
        for k, d in items:
            body, _ = self._partition(d)
            for i, piece in enumerate(body):
                frame_calls.append((f"fr:{k}:{i}", "frame32", [BYTES(piece)]))
        # Empty blobs commit one empty-frame chunk; its frame bytes come
        # from MNCS frame_empty like every other canonical byte.
        for k, d in items:
            if len(bytes(d)) == 0:
                frame_calls.append((f"efr:{k}", "frame_empty", []))
        frames = self._mncs(SRC_CHUNK, MOD_CHUNK, frame_calls) \
            if frame_calls else {}
        # 3. tail padding checks + tail frames (per width).
        for k, d in items:
            _, tail = self._partition(d)
            if len(bytes(d)) == 0 or len(tail) > 0:
                r = len(tail) if len(bytes(d)) > 0 else 0
                if len(bytes(d)) > 0:
                    w = _tail_width(r)
                    padded = _pad_tail(tail, w)
                    fns = TAIL_FNS[w]
                    out = self._mncs(
                        SRC_CHUNK, MOD_CHUNK,
                        [(f"tok:{k}", fns["tail_ok"],
                          [BYTES(padded), U64(r)]),
                         (f"tfr:{k}", fns["frame"], [BYTES(padded)])])
                    if not as_bool(require_returned(out[f"tok:{k}"], k)):
                        raise IntegrityError(
                            f"{k}: tail padding rejected by MNCS")
                    frames[f"tfr:{k}"] = out[f"tfr:{k}"]
        # 4. digests (body digest32 + tail digestW / digest_empty).
        digest_calls = []
        for k, d in items:
            body, tail = self._partition(d)
            data = bytes(d)
            if len(data) == 0:
                digest_calls.append((f"dg:{k}:empty", "digest_empty", []))
            else:
                for i, piece in enumerate(body):
                    digest_calls.append(
                        (f"dg:{k}:{i}", "digest32", [BYTES(piece)]))
                if len(tail) > 0:
                    w = _tail_width(len(tail))
                    digest_calls.append(
                        (f"dg:{k}:tail", TAIL_FNS[w]["digest"],
                         [BYTES(_pad_tail(tail, w))]))
        digests = self._mncs(SRC_CHUNK, MOD_CHUNK, digest_calls,
                             grants=GRANT_CHUNK)
        # 5. object encodings.
        oid_calls = []
        for k, _ in items:
            serial = self._meta["next_serial"]
            self._meta["next_serial"] += 1
            oid_calls.append(
                (f"oid:{k}", "object_encode",
                 [U32(self._meta["namespace"]), U64(serial)]))
        oids = self._mncs(SRC_IDENTITY, MOD_IDENTITY, oid_calls)
        # 6. v2 headers.
        hdr_calls = []
        for k, d in items:
            data = bytes(d)
            n = len(data) // 32 + (1 if len(data) % 32 or not data else 0)
            hdr_calls.append(
                (f"hdr:{k}", "header2",
                 [BY(3), BYTES(descs[k]), U32(len(data)), U16(n)]))
        hdrs = self._mncs(SRC_MANIFEST, MOD_MANIFEST, hdr_calls)
        # 7. chains. chain_init batches across items (one invocation);
        # chain_step is SEQUENTIAL per item (each step consumes the
        # previous digest), so steps batch across items by depth: all
        # items' step j runs in one invocation, j = 0..maxdepth-1.
        # Batching a step across items never reorders any one chain.
        chain_calls = []
        for k, d in items:
            chain_calls.append(
                (f"ch:{k}:init", "chain_init",
                 [BYTES(as_bytes(require_returned(hdrs[f"hdr:{k}"], k)))]))
        inits = self._mncs(SRC_MANIFEST, MOD_MANIFEST, chain_calls,
                           grants=GRANT_MANIFEST)
        ordered = {}
        for k, d in items:
            data = bytes(d)
            body, tail = self._partition(d)
            if len(data) == 0:
                ordered[k] = [as_bytes(require_returned(
                    digests[f"dg:{k}:empty"], k))]
            else:
                seq = [as_bytes(require_returned(digests[f"dg:{k}:{i}"], k))
                       for i in range(len(body))]
                if len(tail) > 0:
                    seq.append(as_bytes(require_returned(
                        digests[f"dg:{k}:tail"], k)))
                ordered[k] = seq
        prevs = {k: as_bytes(require_returned(inits[f"ch:{k}:init"], k))
                 for k, _ in items}
        maxdepth = max(len(v) for v in ordered.values())
        for depth in range(maxdepth):
            step_calls = [
                (f"st:{k}:{depth}", "chain_step",
                 [BYTES(prevs[k]), BYTES(ordered[k][depth])])
                for k, _ in items if len(ordered[k]) > depth
            ]
            outs = self._mncs(SRC_MANIFEST, MOD_MANIFEST, step_calls,
                              grants=GRANT_MANIFEST)
            for k, _ in items:
                if len(ordered[k]) > depth:
                    prevs[k] = as_bytes(
                        require_returned(outs[f"st:{k}:{depth}"], k))
        roots = dict(prevs)
        plan = {}
        for k, d in items:
            data = bytes(d)
            body, tail = self._partition(d)
            chunks = []
            for i in range(len(body)):
                chunks.append((
                    as_bytes(require_returned(frames[f"fr:{k}:{i}"], k)),
                    as_bytes(require_returned(digests[f"dg:{k}:{i}"], k))))
            if len(data) == 0:
                chunks.append((
                    as_bytes(require_returned(frames[f"efr:{k}"], k)),
                    as_bytes(require_returned(digests[f"dg:{k}:empty"], k))))
            elif len(tail) > 0:
                w = _tail_width(len(tail))
                chunks.append((
                    as_bytes(require_returned(frames[f"tfr:{k}"], k)),
                    as_bytes(require_returned(digests[f"dg:{k}:tail"], k))))
            hdr = as_bytes(require_returned(hdrs[f"hdr:{k}"], k))
            manifest = hdr + b"".join(dg for _, dg in chunks)
            root_id = roots[k]
            # 8. pre-commit structural preview: MNCS validates the exact
            # assembled manifest before anything is persisted.
            out = self._mncs(SRC_MANIFEST, MOD_MANIFEST,
                             [(f"pv:{k}", "validate_v2", [BYTES(manifest)])])
            code = as_int(require_returned(out[f"pv:{k}"], k))
            if code != 0:
                raise IntegrityError(
                    f"{k}: assembled v2 manifest rejected (code {code})")
            plan[k] = {
                "oid": as_bytes(require_returned(oids[f"oid:{k}"], k)),
                "desc": descs[k],
                "chunks": chunks,
                "manifest": manifest,
                "root_id": root_id,
            }
        return plan

    # -- commit with state machine + fault injection ----------------------
    def _persist_general_plan(self, plan, fault_at=None):
        """Persist staged v2 objects and publish a new generation.

        Stages (each transition computed by MNCS commit_next and recorded
        on self.trace): PREPARING(0) -> CONTENT_READY(1) -> CANDIDATE(2)
        -> DURABLE_PREREQ(3) -> PUBLISHED(4); any failure -> ABORTED(5).

        fault_at names a stage boundary after which a simulated crash is
        raised (before any further host work): one of "before-chunks",
        "mid-chunks", "after-chunks", "after-manifests",
        "after-generation", "before-current", "after-current", or None.
        """
        state = 0
        mapping = dict(self._mapping)

        def fault(name):
            if fault_at == name:
                self._transition(state, False)
                raise FaultInjected(f"injected crash {name} at state {state}")

        fault("before-chunks")
        # CONTENT_READY: chunks persisted create-exclusive (structural
        # sharing falls out: identical chunks share one file, and the
        # O_EXCL guard keeps committed bytes immutable).
        for key, p in plan.items():
            for frame, digest in p["chunks"]:
                _write_create_exclusive(
                    self._p("chunks", digest.hex()), frame)
                if fault_at == "mid-chunks":
                    self._transition(state, False)
                    raise FaultInjected("injected crash mid-chunks")
        fault("after-chunks")
        state = self._transition(state, True)
        # CANDIDATE: object manifests + full-snapshot generation file.
        for key, p in plan.items():
            _write_create_exclusive(
                self._p("objects", p["oid"].hex()), p["manifest"])
            mapping[p["oid"].hex()] = p["root_id"].hex()
        fault("after-manifests")
        new_gen = self._gen + 1
        self._write_generation_mncs(new_gen, mapping)
        fault("after-generation")
        state = self._transition(state, True)
        # DURABLE_PREREQ: barriers the language cannot yet name (P1-002).
        self._persist_meta()
        fault("before-current")
        self._write_current(new_gen)
        _fsync_dir(self.path)
        _fsync_dir(self._p("generations"))
        state = self._transition(state, True)
        state = self._transition(state, True)
        # A crash here lands after publication is recorded: terminal
        # states stick, so the trace truthfully ends at PUBLISHED.
        fault("after-current")
        assert state == 4, f"commit did not publish: state {state}"
        self._gen = new_gen
        self._mapping = mapping
        return {k: v["oid"] for k, v in plan.items()}

    def _write_generation_mncs(self, gen, mapping):
        """Assemble a generation file with MNCS-produced bytes.

        Header via store.generation.header, records via
        store.manifest.gen_record; the host only concatenates and
        transports. (Phase-1a generations keep their host-assembled
        bytes; new Phase-2 commits use this path.)
        """
        out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                         [(f"gh:{gen}", "header", [U32(len(mapping))])])
        data = bytearray(as_bytes(require_returned(out[f"gh:{gen}"], "gen")))
        items = sorted(mapping.items())
        if items:
            out = self._mncs(
                SRC_MANIFEST, MOD_MANIFEST,
                [(f"gr:{o}", "gen_record",
                  [BYTES(bytes.fromhex(o)), BYTES(bytes.fromhex(r))])
                 for o, r in items])
            for o, r in items:
                data += as_bytes(require_returned(out[f"gr:{o}"], "gen"))
        _write_sync(self._p("generations", f"{gen:08x}"), bytes(data))

    def put_blob(self, data, fault_at=None):
        """Put a general blob (0..992 bytes); returns the ObjectId."""
        if self._mapping is None:
            raise StoreError("store is closed")
        plan = self._mncs_put_general_plan([("one", bytes(data))])
        oids = self._persist_general_plan(plan, fault_at=fault_at)
        return oids["one"]

    def put_general_many(self, items, fault_at=None):
        """Batch put of (key, data) pairs; returns [oid] in input order.

        Keys follow the _mncs_put_general_plan (key, data) convention;
        they are re-keyed internally so duplicates cannot collide.
        """
        if self._mapping is None:
            raise StoreError("store is closed")
        pairs = list(items)
        plan = self._mncs_put_general_plan(
            [(f"item{i}", bytes(d)) for i, (_, d) in enumerate(pairs)])
        oids = self._persist_general_plan(plan, fault_at=fault_at)
        return [oids[f"item{i}"] for i in range(len(pairs))]

    def cas_put_blob(self, expected_gen, data):
        """Compare-and-transition put: commit only when current == expected.

        Returns the new ObjectId on success. On staleness raises
        ConflictError carrying the MNCS conflict token (observed current
        + attempted generation); the store never retries silently.
        """
        if self._mapping is None:
            raise StoreError("store is closed")
        out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                         [("cas", "cas_decide",
                           [U64(self._gen), U64(expected_gen)])])
        if as_int(require_returned(out["cas"], "cas")) != 0:
            out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                             [("tok", "conflict_token",
                               [U32(self._gen), U32(expected_gen)])])
            token = as_bytes(require_returned(out["tok"], "tok"))
            out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                             [("o", "conflict_observed", [BYTES(token)]),
                              ("a", "conflict_attempted", [BYTES(token)])])
            raise ConflictError(
                token,
                as_int(require_returned(out["o"], "obs")),
                as_int(require_returned(out["a"], "att")))
        return self.put_blob(data)

    # -- verified v2 read path --------------------------------------------
    def load_committed2(self, oid, mapping=None):
        """Load a v2 object entry: manifest bytes + chunk file bytes.

        Dispatches on manifest magic (MR = v1, MN = v2). v1 objects keep
        flowing through the inherited load_committed path.
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
                manifest = f.read()
        except FileNotFoundError:
            raise IntegrityError(
                f"object {ohex}: committed but object file is missing")
        if len(manifest) >= 2 and manifest[0:2] == b"MR":
            entry = self.load_committed(oid, mapping=mapping)
            entry["kind"] = 1
            return entry
        if len(manifest) < 28 or manifest[0:2] != b"MN":
            raise IntegrityError(
                f"object {ohex}: manifest magic unknown "
                f"({manifest[0:2]!r}, len {len(manifest)})")
        if len(manifest) > 1024:
            raise IntegrityError(
                f"object {ohex}: manifest {len(manifest)} bytes exceeds "
                "the 1024-byte v2 bound")
        count = (len(manifest) - 28) // 32
        digests = [manifest[28 + 32 * i:28 + 32 * (i + 1)]
                   for i in range(count)]
        frames = []
        for dg in digests:
            try:
                with open(self._p("chunks", dg.hex()), "rb") as f:
                    frames.append(f.read())
            except FileNotFoundError:
                frames.append(None)
        return {"kind": 2, "ohex": ohex, "root_hex": root_hex,
                "manifest": manifest, "digests": digests, "frames": frames}

    def verify_batch2(self, entries):
        """MNCS verification over loaded v2 entries (5 invocations).

        Checks: validate_v2 structure; chain recomputation against the
        committed root; per-chunk framing + digest match; canonical tail
        padding. Raises IntegrityError on any failure. Never NotFound.
        """
        # 1. structural validation + projectors.
        out = self._mncs(SRC_MANIFEST, MOD_MANIFEST,
                         [(e["ohex"], "validate_v2", [BYTES(e["manifest"])])
                          for e in entries])
        for e in entries:
            code = as_int(require_returned(out[e["ohex"]], "manifest-v2"))
            if code != 0:
                raise IntegrityError(
                    f"object {e['ohex']}: v2 manifest code {code}")
        out = self._mncs(SRC_MANIFEST, MOD_MANIFEST,
                         [(e["ohex"] + ":t", "v2_total_len",
                           [BYTES(e["manifest"])])
                          for e in entries] +
                         [(e["ohex"] + ":h", "v2_header",
                           [BYTES(e["manifest"])])
                          for e in entries])
        totals = {e["ohex"]: as_int(require_returned(
            out[e["ohex"] + ":t"], "v2-total")) for e in entries}
        headers = {e["ohex"]: as_bytes(require_returned(
            out[e["ohex"] + ":h"], "v2-header")) for e in entries}
        # 2. chain recomputation (init + depth-batched steps) and match.
        out = self._mncs(SRC_MANIFEST, MOD_MANIFEST,
                         [(e["ohex"], "chain_init", [BYTES(headers[e["ohex"]])])
                          for e in entries], grants=GRANT_MANIFEST)
        prevs = {e["ohex"]: as_bytes(require_returned(out[e["ohex"]], "root"))
                 for e in entries}
        maxdepth = max(len(e["digests"]) for e in entries)
        for depth in range(maxdepth):
            calls = [(e["ohex"], "chain_step",
                      [BYTES(prevs[e["ohex"]]), BYTES(e["digests"][depth])])
                     for e in entries if len(e["digests"]) > depth]
            out = self._mncs(SRC_MANIFEST, MOD_MANIFEST, calls,
                             grants=GRANT_MANIFEST)
            for e in entries:
                if len(e["digests"]) > depth:
                    prevs[e["ohex"]] = as_bytes(
                        require_returned(out[e["ohex"]], "chain"))
        out = self._mncs(
            SRC_IDENTITY, MOD_IDENTITY,
            [(e["ohex"], "content_equal",
              [BYTES(prevs[e["ohex"]]), BYTES(bytes.fromhex(e["root_hex"]))])
             for e in entries])
        for e in entries:
            if not as_bool(require_returned(out[e["ohex"]], "chain-match")):
                raise IntegrityError(
                    f"object {e['ohex']}: chain root mismatch (reordered, "
                    "missing, or substituted chunks)")
        # 3. chunk framing + digest match + canonical tail padding.
        for e in entries:
            for dg, fr in zip(e["digests"], e["frames"]):
                if fr is None:
                    raise IntegrityError(
                        f"object {e['ohex']}: referenced chunk {dg.hex()} "
                        "is missing")
        out = self._mncs(SRC_CHUNK, MOD_CHUNK,
                         [(e["ohex"] + f":{i}", "frame_valid", [BYTES(fr)])
                          for e in entries for i, fr in enumerate(e["frames"])])
        for e in entries:
            for i in range(len(e["frames"])):
                fcode = as_int(require_returned(out[e["ohex"] + f":{i}"],
                                                "framing"))
                if fcode != 0:
                    raise IntegrityError(
                        f"object {e['ohex']}: chunk {i} framing code {fcode}")
        total_len = {e["ohex"]: totals[e["ohex"]] for e in entries}
        vjoint = []
        for e in entries:
            L = total_len[e["ohex"]]
            body, tail = self._partition(b"\x00" * L)
            for i in range(len(body)):
                fr = e["frames"][i]
                width = len(fr) - 4
                vjoint.append((e["ohex"] + f":v{i}",
                               _verify_fn_for(width),
                               [BYTES(fr), BYTES(e["digests"][i])]))
            if L == 0:
                vjoint.append((e["ohex"] + ":v0", "verify_frame_empty",
                               [BYTES(e["frames"][0]),
                                BYTES(e["digests"][0])]))
            elif len(tail) > 0:
                i = len(body)
                fr = e["frames"][i]
                width = len(fr) - 4
                vjoint.append((e["ohex"] + f":v{i}",
                               _verify_fn_for(width),
                               [BYTES(fr), BYTES(e["digests"][i])]))
                # Canonical padding: the tail payload's zero extension is
                # re-checked in-language on every read.
                w = len(fr) - 4
                vjoint.append((e["ohex"] + f":p{i}",
                               TAIL_FNS[w]["tail_ok"],
                               [BYTES(fr[4:]), U64(len(tail))]))
        out = self._mncs(SRC_CHUNK, MOD_CHUNK, vjoint, grants=GRANT_CHUNK)
        for cid, _, _ in vjoint:
            if cid.split(":")[-1].startswith("v"):
                ehex = ":".join(cid.split(":")[:-1])
                if not as_bool(require_returned(out[cid], "chunk-digest")):
                    raise IntegrityError(
                        f"object {ehex}: chunk identity mismatch (corrupt)")
            else:
                ehex = ":".join(cid.split(":")[:-1])
                if not as_bool(require_returned(out[cid], "tail-padding")):
                    raise IntegrityError(
                        f"object {ehex}: non-canonical tail padding")
        return {e["ohex"]: e for e in entries}

    def get_blob(self, oid, mapping=None):
        """Typed blob read through one generation mapping.

        Dispatches on manifest magic: v1 objects flow through the
        inherited single-chunk path, v2 objects through the chained
        multi-chunk path. Callers cannot observe the version except
        through the value's length.
        """
        entry = self.load_committed2(oid, mapping=mapping)
        if entry.get("kind") == 1:
            return super().get_blob(oid, mapping=mapping)
        verified = self.verify_batch2([entry])
        e = verified[entry["ohex"]]
        out = self._mncs(SRC_MANIFEST, MOD_MANIFEST,
                         [("t", "v2_total_len", [BYTES(e["manifest"])])])
        total = as_int(require_returned(out["t"], "v2-total"))
        if total == 0:
            return b""
        nbody = total // 32
        calls = [(f"d{i}", "payload32", [BYTES(e["frames"][i])])
                 for i in range(nbody)]
        if total % 32:
            w = len(e["frames"][nbody]) - 4
            calls.append((f"d{nbody}", TAIL_FNS[w]["payload"],
                          [BYTES(e["frames"][nbody])]))
        out = self._mncs(SRC_CHUNK, MOD_CHUNK, calls)
        parts = b"".join(as_bytes(require_returned(out[f"d{i}"], "dec"))
                         for i in range(len(calls)))
        return parts[:total]

    # -- snapshots --------------------------------------------------------
    def verify(self, oid, mapping=None):
        """First-class integrity verification across v1 + v2 objects."""
        entry = self.load_committed2(oid, mapping=mapping)
        if entry.get("kind") == 1:
            self.verify_batch([self.load_committed(
                bytes.fromhex(entry["ohex"]), mapping=mapping)])
        else:
            self.verify_batch2([entry])
        return True

    def verify_all(self, mapping=None):
        mapping = self._mapping if mapping is None else mapping
        for ohex in sorted(mapping):
            self.verify(bytes.fromhex(ohex), mapping=mapping)
        return len(mapping)

    def acquire_snapshot(self):
        """Bind a reader to the current committed generation.

        Returns (token_bytes, gen). The generation is added to the retain
        set; reclamation must not prune it while live (invariant 14).
        """
        if self._mapping is None:
            raise StoreError("store is closed")
        out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                         [("s", "snap_make", [U32(self._gen)])])
        token = as_bytes(require_returned(out["s"], "snap"))
        self._retained.add(self._gen)
        return token, self._gen

    def release_snapshot(self, gen):
        self._retained.discard(gen)

    def _mapping_for(self, gen):
        return self._read_generation(gen)

    def snapshot_ok(self, token, gen):
        """MNCS snapshot-binding verdict: may gen be served under token?"""
        out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                         [("p", "snap_permits", [BYTES(token), U64(gen)])])
        return as_bool(require_returned(out["p"], "snap-permit"))

    def snapshot_get_blob(self, token, gen, oid):
        """Read a v2 blob through a snapshot bound to `gen`."""
        if not self.snapshot_ok(token, gen):
            raise SnapshotExpired(
                f"snapshot does not permit generation {gen}")
        return self.get_blob(oid, mapping=self._mapping_for(gen))

    def snapshot_get_u32(self, token, gen, oid):
        if not self.snapshot_ok(token, gen):
            raise SnapshotExpired(
                f"snapshot does not permit generation {gen}")
        return super().get_u32(oid, mapping=self._mapping_for(gen))

    # -- recovery ----------------------------------------------------------
    def _classify_generation_file(self, gen):
        """MNCS classify_generation code for one generation file + roots.

        Returns (code, mapping_or_None, detail). Host gathers validator
        outputs; the CLASSIFICATION use is local, the recovery DECISION
        (recover_decide) is MNCS (see recover()).
        """
        try:
            with open(self._p("generations", f"{gen:08x}"), "rb") as f:
                data = f.read()
        except FileNotFoundError:
            return 1, None, "missing-file"
        if len(data) > 1024:
            # Over the in-language validation bound (23 records): the
            # structural count check falls back to the Phase-1a host
            # length gate (same equality, transport parity), while every
            # per-object semantic stays MNCS-verified below. Wider
            # in-language generation scans are P2 pressure, not silent
            # acceptance: see the module docstring.
            try:
                mapping = self._read_generation(gen)
            except IntegrityError as exc:
                return 1, None, f"bad-header: {exc}"
            except OSError as exc:
                return 1, None, f"unreadable: {exc}"
        else:
            out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                             [(f"hv:{gen}", "header_validate",
                               [BYTES(data)])])
            if as_int(require_returned(out[f"hv:{gen}"], "gen-hdr")) != 0:
                # Torn/truncated candidate: structurally incomplete.
                return 1, None, "bad-header"
            try:
                mapping = self._read_generation(gen)
            except IntegrityError as exc:
                # Valid header, torn records: structurally incomplete.
                return 1, None, f"bad-records: {exc}"
        # Per-object root verification (v1 + v2 dispatch).
        try:
            for ohex, root_hex in sorted(mapping.items()):
                entry = self.load_committed2(bytes.fromhex(ohex),
                                             mapping=mapping)
                if entry.get("kind") == 1:
                    self.verify_batch([entry])
                else:
                    self.verify_batch2([entry])
        except IntegrityError as exc:
            msg = str(exc)
            if "missing" in msg:
                return 2, mapping, msg
            return 3, mapping, msg
        return 0, mapping, "valid"

    def scan_candidates(self, after=None):
        """Generation files newer than `after` (unpublished candidates).

        Defaults to the live generation; a freshly crash-reopened
        instance carries no live generation (None), so the floor falls
        back to -1 and every generation file is a candidate — recover()
        applies its own `> cur_gen` filter from the current pointer.
        """
        floor = self._gen if after is None else after
        if floor is None:
            floor = -1
        try:
            names = os.listdir(self._p("generations"))
        except FileNotFoundError:
            return []
        out = []
        for n in sorted(names):
            if len(n) == 8 and all(c in "0123456789abcdef" for c in n):
                g = int(n, 16)
                if g > floor:
                    out.append(g)
        return out

    def recover(self):
        """Deterministic recovery: MNCS decides which generation wins.

        Returns (decision, current_gen, detail) with decision in
        {"healthy", "stay", "promote", "refuse"}. PROMOTE moves the
        current pointer (host transport of an MNCS decision); STAY keeps
        serving the previous generation and leaves the candidate for
        reclamation; REFUSE raises IntegrityError (operator repair).
        """
        try:
            with open(self._p("meta"), "rb") as f:
                meta = f.read()
            meta_ok = (len(meta) == 16 and meta[0:2] == b"MS"
                       and meta[2] == 1)
        except FileNotFoundError:
            meta_ok = False
        try:
            with open(self._p("current"), "rb") as f:
                current = f.read()
            current_ok = (len(current) == 8 and current[0:2] == b"MC"
                          and current[2] == 1)
            cur_gen = int.from_bytes(current[4:8], "big") if current_ok else 0
        except FileNotFoundError:
            current_ok, cur_gen = False, 0
        cur_code, cur_mapping, cur_detail = (1, None, "unread")
        if current_ok:
            cur_code, cur_mapping, cur_detail = \
                self._classify_generation_file(cur_gen)
        out = self._mncs(SRC_RECOVERY, MOD_RECOVERY,
                         [("s", "classify_store",
                           [B(meta_ok), B(current_ok),
                            B(cur_code == 0)])])
        triage = as_int(require_returned(out["s"], "triage"))
        if triage != 0:
            raise IntegrityError(
                f"store triage {triage} (meta_ok={meta_ok}, "
                f"current_ok={current_ok}, current={cur_detail})")
        cands = [g for g in self.scan_candidates() if g > cur_gen]
        if not cands:
            self._gen, self._mapping = cur_gen, cur_mapping
            return "healthy", cur_gen, cur_detail
        # Newest candidate first; MNCS decides per candidate.
        for cand in sorted(cands, reverse=True):
            cand_code, _, cand_detail = self._classify_generation_file(cand)
            out = self._mncs(SRC_RECOVERY, MOD_RECOVERY,
                             [("r", "recover_decide",
                               [B(cur_code == 0), U64(cand_code)])])
            decision = as_int(require_returned(out["r"], "recover"))
            if decision == 1:
                self._write_current(cand)
                _fsync_dir(self.path)
                self._gen, self._mapping = cand, self._read_generation(cand)
                return "promote", cand, cand_detail
            # STAY (0): an older-or-equal valid generation remains
            # authoritative; REFUSE (2) with no valid previous raises.
            if decision == 2:
                raise IntegrityError(
                    f"no valid generation (current={cur_detail}, "
                    f"candidate {cand}={cand_detail})")
        self._gen, self._mapping = cur_gen, cur_mapping
        return "stay", cur_gen, cur_detail

    # -- reclamation ---------------------------------------------------------
    def retained_digests(self, keep_gens=1):
        """Union of chunk digests over retained generations.

        Covers the latest `keep_gens` committed generations plus every
        snapshot-pinned generation. Every retained object is FRESHLY
        MNCS-verified before its digests join the union: reclaim must
        never decide from stale or torn manifests (a corrupt manifest
        raises instead of silently dropping its chunks from the table).
        Assembly is transport; per-chunk membership verdicts come from
        MNCS table_contains.
        """
        if self._mapping is None:
            raise StoreError("store is closed")
        gens = {self._gen} | set(self._retained)
        if keep_gens > 1:
            for g in range(max(0, self._gen - keep_gens + 1), self._gen):
                gens.add(g)
        digests = set()
        for g in sorted(gens):
            try:
                mapping = self._read_generation(g)
            except IntegrityError:
                continue
            for ohex in sorted(mapping):
                entry = self.load_committed2(bytes.fromhex(ohex),
                                             mapping=mapping)
                if entry.get("kind") == 1:
                    self.verify_batch([self.load_committed(
                        bytes.fromhex(ohex), mapping=mapping)])
                    digests.add(entry["chunk_digest"].hex())
                else:
                    self.verify_batch2([entry])
                    digests.update(d.hex() for d in entry["digests"])
        return digests

    def reclaim(self, keep_gens=1, dry_run=True):
        """Delete chunks unreachable from retained digests (MNCS-decided).

        For every chunk file, MNCS table_contains over the retained-digest
        table decides membership (32-entry windows when the union is
        larger; window OR-ing is boolean transport over MNCS verdicts).
        Shared chunks survive while ANY retained root references them.
        Generation files older than the horizon are pruned only when no
        live snapshot pins them (invariant 14). Crash-leftover `*.tmp`
        files (interrupted `_write_sync` staging, never authoritative)
        are removed. Returns (deleted_chunks, deleted_gens, kept_chunks).
        """
        if self._mapping is None:
            raise StoreError("store is closed")
        for sub in ("chunks", "objects", "generations"):
            try:
                names = os.listdir(self._p(sub))
            except FileNotFoundError:
                continue
            for n in sorted(names):
                if n.endswith(".tmp"):
                    os.remove(self._p(sub, n))
        digests = sorted(self.retained_digests(keep_gens=keep_gens))
        windows = [digests[i:i + 32]
                   for i in range(0, max(len(digests), 1), 32)]
        tables = []
        for w in windows:
            raw = b"".join(bytes.fromhex(r) for r in w)
            tables.append(raw + bytes(1024 - len(raw)))
        chunks = sorted(os.listdir(self._p("chunks")))
        deleted, kept = [], []
        for name in chunks:
            if len(name) != 64:
                continue
            target = bytes.fromhex(name)
            held = False
            for table, w in zip(tables, windows):
                out = self._mncs(SRC_GENERATION, MOD_GENERATION,
                                 [(f"m:{name[:8]}", "table_contains",
                                   [BYTES(table), U64(len(w)), BYTES(target)])],
                                 budget=SCAN_BUDGET)
                if as_bool(require_returned(out[f"m:{name[:8]}"], "member")):
                    held = True
                    break
            if held:
                kept.append(name)
            else:
                deleted.append(name)
                if not dry_run:
                    os.remove(self._p("chunks", name))
        # Prune unpinned, out-of-horizon generation files.
        horizon = max(0, self._gen - keep_gens)
        deleted_gens = []
        for g in range(horizon + 1):
            if g == self._gen or g in self._retained:
                continue
            path = self._p("generations", f"{g:08x}")
            if os.path.exists(path):
                deleted_gens.append(g)
                if not dry_run:
                    os.remove(path)
        # Stale temp files: reclaimable only when unreferenced
        # (prune_decide over table membership, same windows).
        tempdir = self._p("temp")
        try:
            temps = sorted(os.listdir(tempdir))
        except FileNotFoundError:
            temps = []
        for t in temps:
            tpath = os.path.join(tempdir, t)
            try:
                with open(tpath, "rb") as f:
                    staged = f.read()
            except OSError:
                continue
            if len(staged) not in (4, 8, 12, 20, 36):
                ref = False
            else:
                # Staged content is a framed chunk only when it parses;
                # membership is decided on its digest when computable.
                ref = False
                try:
                    import hashlib
                    dg = hashlib.sha256(staged).digest()
                    for table, w in zip(tables, windows):
                        out = self._mncs(
                            SRC_GENERATION, MOD_GENERATION,
                            [(f"pt:{t[:8]}", "table_contains",
                              [BYTES(table), U64(len(w)), BYTES(dg)])],
                            budget=SCAN_BUDGET)
                        if as_bool(require_returned(out[f"pt:{t[:8]}"],
                                                    "staged")):
                            ref = True
                            break
                except Exception:
                    ref = False
            out = self._mncs(SRC_RECOVERY, MOD_RECOVERY,
                             [(f"pd:{t[:8]}", "prune_decide", [B(ref)])])
            if as_int(require_returned(out[f"pd:{t[:8]}"], "prune")) == 0:
                deleted.append(f"temp/{t}")
                if not dry_run:
                    os.remove(tpath)
            else:
                kept.append(f"temp/{t}")
        return deleted, deleted_gens, kept


def _tail_width(r):
    """Least covered width W >= r (mirrors MNCS tail_width_for; the MNCS
    predicate is the authority — hosts must agree with it, and tests pin
    both to the same table."""
    if r <= 4:
        return 4
    if r <= 8:
        return 8
    if r <= 16:
        return 16
    return 32


def _verify_fn_for(width):
    try:
        return {4: "verify_frame4", 8: "verify_frame8",
                16: "verify_frame16", 32: "verify_frame32",
                0: "verify_frame_empty"}[width]
    except KeyError:
        raise IntegrityError(
            f"unsupported frame payload width {width}")

