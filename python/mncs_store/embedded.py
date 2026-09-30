"""Durable local realization of the Store application contract.

The implementation deliberately separates semantic calls from platform
mechanics.  MNCS decides hashes, compare-and-transition results, and typed
artifact validity through :mod:`store.application.v1`; this module performs
bounded staging, locking, fsync, atomic replacement, and directory sync.
"""

from __future__ import annotations

import errno
import fcntl
import os
import struct
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Sequence

from .errors import BatchCommitResult, CommitResult, StoreError, StoreIntegrityError, StoreResultCode
from .session import StoreSession, as_bytes, as_int, u32, u64, _returned


CHUNK_SIZE = 64 * 1024
FANOUT = 31
META_MAGIC = b"MS"
HEAD_MAGIC = b"MH"
GENERATION_MAGIC = b"MG"
JOURNAL_MAGIC = b"SJ"
MANIFEST_MAGIC = b"SM"
NODE_MAGIC = b"SN"
BINDING_MAGIC = b"SB"
VERSION = 2
MANIFEST_VERSION_V3 = 3

# 20-byte node header + 31 * 32-byte child identities = 1012 bytes.
NODE_HEADER = struct.Struct(">2sBBIIQ")
MANIFEST = struct.Struct(">2sBBQIQI32s32s32s32s12s")
MANIFEST_V2 = MANIFEST
# V3 extends the v2 manifest with the content digests of the adaptive
# sidecars (envelope + representation table). Block tables and opaque
# representation payloads are addressed from the envelope and
# representation records themselves, so no further manifest growth is
# needed. V2 manifests remain readable; their adaptive view is
# synthesized deterministically through MNCS.
MANIFEST_V3 = struct.Struct(">2sBBQIQI32s32s32s32s12s32s32s")
GENERATION_HEADER = struct.Struct(">2sBBQIII")
GENERATION_ENTRY = struct.Struct(">12s32s32s32sQQ")
BINDING_HEADER = struct.Struct(">2sBBII")
JOURNAL = struct.Struct(">2sBBQQ32s")
RELATION_SIZE = 172
PROVENANCE_SIZE = 112
ENVELOPE_SIZE = 256
REPRESENTATION_SIZE = 128
BLOCK_SIZE = 128
REPS_HEADER = struct.Struct(">I")
BLOCKS_HEADER = struct.Struct(">I")
IDENTITY_CODEC_CODE = 0
RLE_CODEC_CODE = 1
RLE_WINDOW = 64
RLE_FRAME = struct.Struct(">HH")
MAX_BLOCKS_PER_TABLE = 8
MAX_SYNOPSIS_BYTES = 64 * 1024


class CrashInjected(RuntimeError):
    """Fault-injection signal; a real process may terminate at the same hook."""


Failpoint = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class StoredObject:
    domain_schema: bytes
    domain_identity: bytes
    logical_id: bytes
    content_id: bytes
    representation_root: bytes
    binding_id: bytes
    generation: int
    ordinal: int
    descriptor: bytes
    payload: bytes


@dataclass(frozen=True, slots=True)
class BoundObjectInput:
    """Generic immutable material for one object in an atomic Store batch."""

    domain_schema: bytes
    domain_identity: bytes
    descriptor: bytes
    payload: bytes
    relations: tuple[bytes, ...] = ()
    provenance: tuple[bytes, ...] = ()


@dataclass(frozen=True, slots=True)
class RepresentationInput:
    """Producer material for one non-default representation.

    ``payload`` is the plain (pre-codec) bytes. The codec, sizes, decode
    class, and exactness flag are Store-computed through MNCS; a
    fidelity-5 claim is admitted only when the payload hashes to the
    object's content identity.
    """

    fidelity: int
    codec: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class BlockInput:
    """Producer material for one semantic block of the base payload."""

    index: int
    tag: int
    start: int
    length: int


@dataclass(frozen=True, slots=True)
class EnvelopeView:
    """A verified semantic envelope with its decoded fields."""

    raw: bytes
    fields: dict
    stored_bytes_touched: int


@dataclass(frozen=True, slots=True)
class RepresentationView:
    """One verified representation record with its decoded fields."""

    index: int
    raw: bytes
    fields: dict


@dataclass(frozen=True, slots=True)
class Selection:
    """An MNCS-tournament representation selection."""

    index: int
    record: bytes
    fields: dict
    satisfied: bool
    estimated_cost: int


@dataclass(frozen=True, slots=True)
class MaterializedView:
    """Materialized bytes with honest acquisition accounting.

    ``stored_bytes_touched`` counts durable payload bytes read (chunks,
    blobs, tables); ``materialized_bytes`` counts plain bytes produced.
    Envelope/manifest/binding bytes are counted separately as
    ``inspect_bytes`` so decode amplification is measurable.
    """

    payload: bytes
    representation_index: int
    fidelity: int
    satisfied: bool
    mask: int
    stored_bytes_touched: int
    materialized_bytes: int
    inspect_bytes: int
    exact_verified: bool


@dataclass(frozen=True, slots=True)
class _Entry:
    logical_id: bytes
    binding_id: bytes
    representation_root: bytes
    content_id: bytes
    total_length: int
    ordinal: int


def _sync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EBADF, errno.EINVAL, errno.ENOTSUP}:
            return
        raise
    try:
        os.fsync(descriptor)
    except OSError as exc:
        if exc.errno not in {errno.EBADF, errno.EINVAL, errno.ENOTSUP}:
            raise
    finally:
        os.close(descriptor)


def _durable_write(path: Path, value: bytes, *, exclusive: bool = False) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC | getattr(os, "O_BINARY", 0)
    if exclusive:
        flags |= os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        view = memoryview(value)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short Store write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, value: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.stage")
    _durable_write(temporary, value, exclusive=True)
    os.replace(temporary, path)
    _sync_directory(path.parent)


class EmbeddedStore:
    """Supported retained-session Store API for local native consumers."""

    def __init__(
        self,
        path: Path,
        *,
        session: StoreSession | None = None,
        command_executor: object | None = None,
        failpoint: Failpoint | None = None,
        verify_on_open: bool = True,
    ) -> None:
        if session is not None and command_executor is not None:
            raise ValueError("provide either a Store session or a command executor, not both")
        self.path = Path(path).expanduser().resolve()
        self.session = session or StoreSession(command_executor=command_executor)
        self._owns_session = session is None
        self._failpoint = failpoint
        self._closed = False
        # A verified current-generation projection is a rebuildable read cache.
        # The generation head remains the authority; a publication invalidates
        # this cache and a changed head causes the next read to rebuild it.
        self._projection_generation: int | None = None
        self._projection: tuple[StoredObject, ...] = ()
        self._objects_by_binding: dict[bytes, StoredObject] = {}
        self._logical_ids_by_domain: dict[tuple[bytes, bytes], bytes] = {}
        self._logical_ids_by_identity: dict[bytes, tuple[bytes, ...]] = {}
        self._commit_feed_generation: int | None = None
        self._commit_feed: bytes | None = None
        self._domain_index_generation: int | None = None
        self._domain_index: tuple[tuple[_Entry, bytes, bytes], ...] = ()
        for name in (
            "chunks",
            "nodes",
            "objects",
            "descriptors",
            "bindings",
            "generations",
            "staging",
            "envelopes",
            "representations",
            "blocks",
            "reps",
        ):
            (self.path / name).mkdir(mode=0o700, parents=True, exist_ok=True)
        self._initialize_files()
        self.recovery_result = self._recover()
        if verify_on_open:
            generation = self.current_generation
            self._verified_projection(generation)

    # ---- platform boundary -------------------------------------------

    def _initialize_files(self) -> None:
        meta = self.path / "meta"
        if not meta.exists():
            _durable_write(meta, META_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", 0))
            _sync_directory(self.path)
        elif meta.read_bytes() != META_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", 0):
            raise StoreIntegrityError("Store meta descriptor is unknown or malformed")
        generation = self.path / "generations" / "0000000000000000"
        if not generation.exists():
            _durable_write(
                generation,
                GENERATION_HEADER.pack(GENERATION_MAGIC, VERSION, 0, 0, 0, 0, 0),
            )
            _sync_directory(generation.parent)
        head = self.path / "head"
        if not head.exists():
            _durable_write(head, HEAD_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", 0))
            _sync_directory(self.path)

    @contextmanager
    def _publication_lock(self) -> Iterator[None]:
        lock_path = self.path / "publication.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _trip(self, name: str) -> None:
        if self._failpoint is not None:
            self._failpoint(name)

    def _write_immutable(self, path: Path, value: bytes) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.exists():
            try:
                current = path.read_bytes()
            except OSError as exc:
                raise StoreIntegrityError(f"cannot inspect immutable Store content: {path}") from exc
            if current != value:
                raise StoreIntegrityError(f"immutable Store content differs at {path.name}")
            return
        _durable_write(path, value, exclusive=True)
        _sync_directory(path.parent)

    # ---- native semantic boundary ------------------------------------

    def _hash(self, value: bytes) -> bytes:
        return self.session.sha256(value)

    def _verify_digest(self, value: bytes) -> bytes:
        """Use Store's optimized canonical digest path for resident read data."""

        fast_hash = getattr(self.session, "sha256_buffer", None)
        return bytes(fast_hash(value)) if callable(fast_hash) else self._hash(value)

    def _cas(self, observed: int, expected: int) -> int:
        output = self.session.call(
            "store.generation.v1",
            "cas_decide",
            [u64(observed), u64(expected)],
            step_budget=32_768,
        )
        return as_int(_returned(output))

    def _conflict_token(self, observed: int, expected: int) -> bytes:
        if observed > 0xFFFFFFFF or expected > 0xFFFFFFFF:
            return self._hash(struct.pack(">QQ", observed, expected))[:16]
        output = self.session.call(
            "store.generation.v1",
            "conflict_token",
            [u32(observed), u32(expected)],
            step_budget=32_768,
        )
        return as_bytes(_returned(output))

    # ---- identity and canonical structural records ------------------

    def _binding_id(self, domain_schema: bytes, domain_identity: bytes) -> bytes:
        return self._hash(domain_schema + b"\x00" + domain_identity)

    def _logical_id(self, binding_id: bytes) -> bytes:
        # Store's native logical-object identity is a 12-byte opaque value;
        # domain bindings remain the authority for resolving it back to a
        # consumer identity.
        return self._hash(b"mncs.store.logical.v1\x00" + binding_id)[:12]

    def logical_id_for(self, domain_schema: bytes, domain_identity: bytes) -> bytes:
        """Return the Store logical identity for an opaque domain binding."""

        return self._logical_id(self._binding_id(bytes(domain_schema), bytes(domain_identity)))

    def content_identity(self, payload: bytes) -> bytes:
        """Return a Store content identity through the native SHA boundary."""

        return self._hash(bytes(payload))

    def make_relation(
        self,
        *,
        relation_type: bytes,
        source: bytes,
        target: bytes,
        generation: int,
        provenance: bytes,
        ordinal: int,
        metadata_type: bytes | None = None,
        metadata_root: bytes | None = None,
    ) -> bytes:
        return self.session.encode_relation(
            relation_type,
            source,
            target,
            generation,
            provenance,
            ordinal,
            metadata_type,
            metadata_root,
        )

    def make_provenance(
        self,
        *,
        source: bytes,
        producer: bytes,
        transformation: bytes,
        generation: int,
        evidence: bytes,
        ancestry: bytes,
    ) -> bytes:
        return self.session.encode_provenance(
            source,
            producer,
            transformation,
            generation,
            evidence,
            ancestry,
        )

    def _node(self, level: int, start: int, children: list[bytes]) -> tuple[bytes, bytes]:
        if not children or len(children) > FANOUT:
            raise StoreIntegrityError("invalid Store content-tree fanout")
        raw = NODE_HEADER.pack(NODE_MAGIC, VERSION, 0, level, start, len(children))
        raw += b"".join(children)
        return raw, self._hash(raw)

    def _manifest(
        self,
        *,
        total_length: int,
        chunk_count: int,
        depth: int,
        content_id: bytes,
        tree_root: bytes,
        descriptor_id: bytes,
        binding_id: bytes,
        logical_id: bytes,
        envelope_digest: bytes | None = None,
        reps_digest: bytes | None = None,
    ) -> bytes:
        if (envelope_digest is None) != (reps_digest is None):
            raise StoreIntegrityError("Store manifest v3 sidecars must be paired")
        if envelope_digest is None or reps_digest is None:
            return MANIFEST_V2.pack(
                MANIFEST_MAGIC,
                VERSION,
                0,
                total_length,
                CHUNK_SIZE,
                chunk_count,
                depth,
                content_id,
                tree_root,
                descriptor_id,
                binding_id,
                logical_id,
            )
        return MANIFEST_V3.pack(
            MANIFEST_MAGIC,
            MANIFEST_VERSION_V3,
            0,
            total_length,
            CHUNK_SIZE,
            chunk_count,
            depth,
            content_id,
            tree_root,
            descriptor_id,
            binding_id,
            logical_id,
            envelope_digest,
            reps_digest,
        )

    def _build_content(
        self,
        *,
        payload: bytes,
        descriptor: bytes,
        binding_id: bytes,
        logical_id: bytes,
        envelope_digest: bytes | None = None,
        reps_digest: bytes | None = None,
    ) -> tuple[bytes, bytes, bytes, int, int]:
        """Stage immutable chunks/nodes and return content/root metadata."""

        chunks = [payload[offset : offset + CHUNK_SIZE] for offset in range(0, len(payload), CHUNK_SIZE)]
        if not chunks:
            chunks = [b""]
        content_id = self._hash(payload)
        chunk_ids: list[bytes] = []
        for index, chunk in enumerate(chunks):
            self._trip("during_chunk_staging")
            digest = self._hash(chunk)
            self._write_immutable(self.path / "chunks" / f"{digest.hex()}.chunk", chunk)
            chunk_ids.append(digest)
        self._sync_content_directory("chunks")
        self._trip("after_content_durability")

        level = 0
        start = 0
        nodes: list[tuple[bytes, bytes, int]] = []
        for offset in range(0, len(chunk_ids), FANOUT):
            raw, digest = self._node(level, offset, chunk_ids[offset : offset + FANOUT])
            self._write_immutable(self.path / "nodes" / f"{digest.hex()}.node", raw)
            nodes.append((raw, digest, offset))
        self._sync_content_directory("nodes")

        while len(nodes) > 1:
            level += 1
            parents: list[tuple[bytes, bytes, int]] = []
            for offset in range(0, len(nodes), FANOUT):
                children = [item[1] for item in nodes[offset : offset + FANOUT]]
                # Node starts are expressed in leaf-chunk coordinates.  This
                # keeps structural validation independent of the level at
                # which a node is reached.
                start = nodes[offset][2]
                raw, digest = self._node(level, start, children)
                self._write_immutable(self.path / "nodes" / f"{digest.hex()}.node", raw)
                parents.append((raw, digest, start))
            nodes = parents
        tree_root = nodes[0][1]
        descriptor_id = self._hash(descriptor)
        manifest = self._manifest(
            total_length=len(payload),
            chunk_count=len(chunk_ids),
            depth=level,
            content_id=content_id,
            tree_root=tree_root,
            descriptor_id=descriptor_id,
            binding_id=binding_id,
            logical_id=logical_id,
            envelope_digest=envelope_digest,
            reps_digest=reps_digest,
        )
        representation_root = self._hash(manifest)
        self._write_immutable(self.path / "descriptors" / f"{descriptor_id.hex()}.desc", descriptor)
        self._trip("during_structural_metadata_staging")
        self._write_immutable(self.path / "objects" / f"{logical_id.hex()}.manifest", manifest)
        self._sync_content_directory("objects")
        return content_id, representation_root, descriptor_id, len(chunk_ids), level

    def _sync_content_directory(self, name: str) -> None:
        _sync_directory(self.path / name)

    def _publish_head(self, generation: int) -> None:
        """Publish the head with an observable post-rename durability point."""

        head = self.path / "head"
        temporary = head.with_name(f".{head.name}.{os.getpid()}.{time.time_ns()}.stage")
        value = HEAD_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", generation)
        _durable_write(temporary, value, exclusive=True)
        os.replace(temporary, head)
        self._trip("after_head_replace_before_directory_sync")
        _sync_directory(head.parent)
        self._trip("during_head_publication")

    def _write_journal_state(
        self,
        transaction: Path,
        *,
        state: int,
        old: int,
        new: int,
        digest: bytes,
    ) -> None:
        if state < 0 or state > 3:
            raise StoreIntegrityError("invalid Store recovery journal state")
        _atomic_write(
            transaction / "journal",
            JOURNAL.pack(JOURNAL_MAGIC, VERSION, state, old, new, digest),
        )

    # ---- generation and recovery -------------------------------------

    @property
    def current_generation(self) -> int:
        raw = (self.path / "head").read_bytes()
        if len(raw) != 12 or raw[:2] != HEAD_MAGIC or raw[2] != VERSION or raw[3] != 0:
            raise StoreIntegrityError("Store head is malformed")
        return struct.unpack(">Q", raw[4:12])[0]

    @property
    def provenance(self) -> dict[str, str]:
        """Artifact/toolchain provenance for diagnostics and audit receipts."""

        return {
            "backend": self.session.backend,
            "artifact_sha256": self.session.artifact_sha256,
            "toolchain": self.session.toolchain,
        }

    def _validate_typed_records(
        self,
        relations: list[bytes],
        provenance: list[bytes],
        generation: int,
        *,
        require_current_generation: bool = False,
    ) -> None:
        for raw in relations:
            if len(raw) != RELATION_SIZE:
                raise StoreIntegrityError("Store relation has an invalid width")
            relation_generation = self.session.validate_relation(raw, expected_generation=generation)
            if require_current_generation and relation_generation != generation:
                raise StoreIntegrityError("new Store relation is not bound to its generation")
        for raw in provenance:
            if len(raw) != PROVENANCE_SIZE:
                raise StoreIntegrityError("Store provenance has an invalid width")
            provenance_generation = self.session.validate_provenance(raw, expected_generation=generation)
            if require_current_generation and provenance_generation != generation:
                raise StoreIntegrityError("new Store provenance is not bound to its generation")

    def _generation_bytes(
        self,
        generation: int,
        entries: list[_Entry],
        relations: list[bytes],
        provenance: list[bytes],
    ) -> bytes:
        ordered = sorted(entries, key=lambda item: item.logical_id)
        raw = GENERATION_HEADER.pack(
            GENERATION_MAGIC,
            VERSION,
            0,
            generation,
            len(ordered),
            len(relations),
            len(provenance),
        )
        raw += b"".join(
            GENERATION_ENTRY.pack(
                entry.logical_id,
                entry.binding_id,
                entry.representation_root,
                entry.content_id,
                entry.total_length,
                entry.ordinal,
            )
            for entry in ordered
        )
        raw += b"".join(relations)
        raw += b"".join(provenance)
        return raw

    def _parse_generation_parts(
        self,
        raw: bytes,
        expected_generation: int,
    ) -> tuple[list[_Entry], list[bytes], list[bytes]]:
        if len(raw) < GENERATION_HEADER.size:
            raise StoreIntegrityError("Store generation is truncated")
        magic, version, reserved, generation, count, relation_count, provenance_count = GENERATION_HEADER.unpack_from(raw)
        if magic != GENERATION_MAGIC or version != VERSION or reserved != 0:
            raise StoreIntegrityError("Store generation header is unknown")
        if generation != expected_generation:
            raise StoreIntegrityError("Store generation identity does not match its path")
        expected = (
            GENERATION_HEADER.size
            + count * GENERATION_ENTRY.size
            + relation_count * RELATION_SIZE
            + provenance_count * PROVENANCE_SIZE
        )
        if len(raw) != expected:
            raise StoreIntegrityError("Store generation has a torn record tail")
        entries: list[_Entry] = []
        seen: set[bytes] = set()
        offset = GENERATION_HEADER.size
        for _ in range(count):
            values = GENERATION_ENTRY.unpack_from(raw, offset)
            offset += GENERATION_ENTRY.size
            entry = _Entry(*values)
            if entry.logical_id in seen:
                raise StoreIntegrityError("Store generation repeats a logical object")
            seen.add(entry.logical_id)
            entries.append(entry)
        relations_start = offset
        relations_end = relations_start + relation_count * RELATION_SIZE
        relations = [
            raw[start : start + RELATION_SIZE]
            for start in range(relations_start, relations_end, RELATION_SIZE)
        ]
        provenance = [
            raw[start : start + PROVENANCE_SIZE]
            for start in range(relations_end, len(raw), PROVENANCE_SIZE)
        ]
        self._validate_typed_records(relations, provenance, expected_generation)
        return entries, relations, provenance

    def _parse_generation(self, raw: bytes, expected_generation: int) -> list[_Entry]:
        entries, _relations, _provenance = self._parse_generation_parts(raw, expected_generation)
        return entries

    def _read_generation(self, generation: int, *, verify_objects: bool) -> list[_Entry]:
        path = self.path / "generations" / f"{generation:016x}"
        try:
            entries = self._parse_generation(path.read_bytes(), generation)
        except FileNotFoundError as exc:
            raise StoreIntegrityError(f"committed Store generation is missing: {generation}") from exc
        if verify_objects:
            for entry in entries:
                self._read_entry(entry, generation, verify_payload=True)
        return entries

    def _invalidate_projection(self) -> None:
        self._projection_generation = None
        self._projection = ()
        self._objects_by_binding = {}
        self._logical_ids_by_domain = {}
        self._logical_ids_by_identity = {}
        self._commit_feed_generation = None
        self._commit_feed = None
        self._domain_index_generation = None
        self._domain_index = ()

    def _rebuild_lookup_maps(self, projection: tuple[StoredObject, ...]) -> None:
        by_binding = {item.binding_id: item for item in projection}
        by_domain = {
            (item.domain_schema, item.domain_identity): item.logical_id for item in projection
        }
        by_identity: dict[bytes, list[bytes]] = {}
        for item in projection:
            by_identity.setdefault(item.domain_identity, []).append(item.logical_id)
        self._objects_by_binding = by_binding
        self._logical_ids_by_domain = by_domain
        self._logical_ids_by_identity = {
            identity: tuple(logical_ids) for identity, logical_ids in by_identity.items()
        }

    def _extend_lookup_maps(self, item: StoredObject) -> None:
        self._objects_by_binding[item.binding_id] = item
        self._logical_ids_by_domain[(item.domain_schema, item.domain_identity)] = item.logical_id
        matches = list(self._logical_ids_by_identity.get(item.domain_identity, ()))
        if item.logical_id not in matches:
            matches.append(item.logical_id)
        self._logical_ids_by_identity[item.domain_identity] = tuple(matches)

    def _verified_projection(self, generation: int | None = None) -> list[StoredObject]:
        """Return one fully verified projection for an exact Store generation.

        Store remains authoritative through the generation head and immutable
        object identities.  This cache only avoids repeating the same complete
        verification within one resident Store process; publication or a head
        change makes it ineligible and the projection is rebuilt from Store.
        """

        selected_generation = self.current_generation if generation is None else generation
        if self._projection_generation == selected_generation:
            return list(self._projection)
        entries = self._read_generation(selected_generation, verify_objects=False)
        projection = tuple(
            self._read_entry(entry, selected_generation, verify_payload=True)
            for entry in sorted(entries, key=lambda item: item.ordinal)
        )
        self._projection_generation = selected_generation
        self._projection = projection
        self._rebuild_lookup_maps(projection)
        return list(projection)

    def _projection_matches_entries(self, generation: int, entries: list[_Entry]) -> bool:
        if self._projection_generation != generation:
            return False
        return [
            (item.logical_id, item.binding_id, item.ordinal) for item in self._projection
        ] == [(item.logical_id, item.binding_id, item.ordinal) for item in sorted(entries, key=lambda item: item.ordinal)]

    def _read_generation_parts(self, generation: int) -> tuple[list[_Entry], list[bytes], list[bytes]]:
        path = self.path / "generations" / f"{generation:016x}"
        try:
            return self._parse_generation_parts(path.read_bytes(), generation)
        except FileNotFoundError as exc:
            raise StoreIntegrityError(f"committed Store generation is missing: {generation}") from exc

    def typed_records(self, kind: str, generation: int | None = None) -> list[bytes]:
        """Return validated current-generation typed Store records."""

        selected_generation = self.current_generation if generation is None else generation
        _entries, relations, provenance = self._read_generation_parts(selected_generation)
        if kind == "relations":
            return relations
        if kind == "provenance":
            return provenance
        if kind == "feeds":
            return [self.commit_feed(selected_generation)]
        raise StoreError(StoreResultCode.DENIED, f"unknown Store typed record kind: {kind}")

    def commit_feed(self, generation: int | None = None) -> bytes:
        """Return the native deterministic feed for one verified generation."""

        selected_generation = self.current_generation if generation is None else generation
        if self._commit_feed_generation == selected_generation and self._commit_feed is not None:
            return self._commit_feed
        path = self.path / "generations" / f"{selected_generation:016x}"
        raw = path.read_bytes()
        entries, relations, provenance = self._parse_generation_parts(raw, selected_generation)
        feed = self.session.encode_commit_feed(
            selected_generation,
            len(entries),
            len(relations),
            len(provenance),
            self._hash(raw),
        )
        self._commit_feed_generation = selected_generation
        self._commit_feed = feed
        return feed

    def _valid_generation_bytes(self, generation: int, raw: bytes) -> bool:
        try:
            entries = self._parse_generation(raw, generation)
            for entry in entries:
                self._read_entry(entry, generation, verify_payload=True)
            return True
        except (StoreError, OSError, struct.error, ValueError):
            return False

    def _read_head_checked(self) -> int | None:
        try:
            return self.current_generation
        except (OSError, StoreError, struct.error):
            return None

    def _recover(self) -> StoreResultCode:
        with self._publication_lock():
            current = self._read_head_checked()
            result = StoreResultCode.RECOVERED_OLD
            for transaction in sorted(self.path.joinpath("staging").iterdir()):
                if not transaction.is_dir():
                    continue
                journal_path = transaction / "journal"
                try:
                    raw_journal = journal_path.read_bytes()
                    magic, version, state, old, new, digest = JOURNAL.unpack(raw_journal)
                    if magic != JOURNAL_MAGIC or version != VERSION or state > 3:
                        raise StoreIntegrityError("unknown Store recovery journal")
                    staged = transaction / "generation.stage"
                    staged_raw = staged.read_bytes() if staged.exists() else None
                    target = self.path / "generations" / f"{new:016x}"
                    if target.exists():
                        candidate = target.read_bytes()
                    else:
                        candidate = staged_raw
                    candidate_valid = candidate is not None and self._hash(candidate) == digest and self._valid_generation_bytes(new, candidate)
                    old_valid = self._generation_is_valid(old)
                    recovery_decision = self.session.recovery_decide(
                        old_valid,
                        0 if candidate_valid else 3,
                    )
                    if current == new and candidate_valid:
                        result = StoreResultCode.RECOVERED_NEW
                    elif current == old and old_valid:
                        result = StoreResultCode.RECOVERED_OLD
                    elif recovery_decision == 1 and candidate_valid:
                        _atomic_write(self.path / "head", HEAD_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", new))
                        current = new
                        result = StoreResultCode.RECOVERED_NEW
                    elif recovery_decision == 0 and old_valid:
                        _atomic_write(self.path / "head", HEAD_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", old))
                        current = old
                        result = StoreResultCode.RECOVERED_OLD
                    else:
                        raise StoreError(
                            StoreResultCode.RECOVERY_REQUIRED,
                            "neither old nor new committed Store generation verifies",
                        )
                except FileNotFoundError:
                    raise StoreError(
                        StoreResultCode.RECOVERY_REQUIRED,
                        f"incomplete Store recovery journal: {transaction.name}",
                    )
                # Cleanup is never used to choose authority.  A valid head
                # and verified generation have already made that decision.
                # Leave malformed/incomplete transactions in place so a
                # caller receives RECOVERY_REQUIRED with the durable evidence
                # still available for diagnosis.
                for child in transaction.iterdir():
                    if child.is_file():
                        child.unlink()
                transaction.rmdir()
            if current is None:
                if self._generation_is_valid(0):
                    _atomic_write(self.path / "head", HEAD_MAGIC + bytes([VERSION, 0]) + struct.pack(">Q", 0))
                    result = StoreResultCode.RECOVERED_OLD
                else:
                    raise StoreError(
                        StoreResultCode.RECOVERY_REQUIRED,
                        "Store head and generation are both unverifiable",
                    )
            return result

    def _generation_is_valid(self, generation: int) -> bool:
        try:
            self._read_generation(generation, verify_objects=True)
            return True
        except (StoreError, OSError, struct.error, ValueError):
            return False

    # ---- generic binding/object operations ---------------------------

    def _binding_path(self, binding_id: bytes) -> Path:
        return self.path / "bindings" / f"{binding_id.hex()}.bind"

    def _write_binding(
        self,
        *,
        domain_schema: bytes,
        domain_identity: bytes,
        binding_id: bytes,
        logical_id: bytes,
        content_id: bytes,
        representation_root: bytes,
        generation: int,
    ) -> None:
        if len(domain_schema) > 0xFFFFFFFF or len(domain_identity) > 0xFFFFFFFF:
            raise StoreError(StoreResultCode.DENIED, "domain binding identity is too large")
        raw = BINDING_HEADER.pack(BINDING_MAGIC, VERSION, 0, len(domain_schema), len(domain_identity))
        raw += domain_schema + domain_identity
        raw += logical_id + content_id + representation_root + struct.pack(">Q", generation)
        self._write_immutable(self._binding_path(binding_id), raw)

    def _read_binding(self, binding_id: bytes) -> tuple[bytes, bytes, bytes, bytes, bytes, int]:
        raw = self._binding_path(binding_id).read_bytes()
        if len(raw) < BINDING_HEADER.size + 12 + 32 + 32 + 8:
            raise StoreIntegrityError("Store binding is truncated")
        magic, version, reserved, schema_length, identity_length = BINDING_HEADER.unpack_from(raw)
        if magic != BINDING_MAGIC or version != VERSION or reserved != 0:
            raise StoreIntegrityError("Store binding header is unknown")
        offset = BINDING_HEADER.size
        end = offset + schema_length + identity_length
        if end + 12 + 32 + 32 + 8 != len(raw):
            raise StoreIntegrityError("Store binding has an invalid length")
        schema = raw[offset : offset + schema_length]
        identity = raw[offset + schema_length : end]
        logical = raw[end : end + 12]
        content = raw[end + 12 : end + 44]
        root = raw[end + 44 : end + 76]
        generation = struct.unpack(">Q", raw[end + 76 : end + 84])[0]
        return schema, identity, logical, content, root, generation

    def _read_manifest(
        self, entry: _Entry
    ) -> tuple[bytes, int, int, int, bytes, bytes, bytes, tuple[bytes, bytes] | None]:
        raw = (self.path / "objects" / f"{entry.logical_id.hex()}.manifest").read_bytes()
        adaptive: tuple[bytes, bytes] | None = None
        if len(raw) == MANIFEST_V2.size:
            values = MANIFEST_V2.unpack(raw)
            (
                magic,
                version,
                reserved,
                total_length,
                chunk_size,
                chunk_count,
                depth,
                content_id,
                tree_root,
                descriptor_id,
                binding_id,
                logical_id,
            ) = values
            if magic != MANIFEST_MAGIC or version != VERSION or reserved != 0:
                raise StoreIntegrityError("Store object manifest is unknown")
        elif len(raw) == MANIFEST_V3.size:
            values = MANIFEST_V3.unpack(raw)
            (
                magic,
                version,
                reserved,
                total_length,
                chunk_size,
                chunk_count,
                depth,
                content_id,
                tree_root,
                descriptor_id,
                binding_id,
                logical_id,
                envelope_digest,
                reps_digest,
            ) = values
            if magic != MANIFEST_MAGIC or version != MANIFEST_VERSION_V3 or reserved != 0:
                raise StoreIntegrityError("Store object manifest is unknown")
            adaptive = (envelope_digest, reps_digest)
        else:
            raise StoreIntegrityError("Store object manifest has an invalid size")
        if logical_id != entry.logical_id or binding_id != entry.binding_id:
            raise StoreIntegrityError("Store object manifest binding is inconsistent")
        if content_id != entry.content_id or self._verify_digest(raw) != entry.representation_root:
            raise StoreIntegrityError("Store object manifest root identity mismatch")
        if chunk_size != CHUNK_SIZE or chunk_count < 1:
            raise StoreIntegrityError("Store object manifest geometry is invalid")
        descriptor = (self.path / "descriptors" / f"{descriptor_id.hex()}.desc").read_bytes()
        if self._verify_digest(descriptor) != descriptor_id:
            raise StoreIntegrityError("Store descriptor integrity failure")
        return raw, total_length, chunk_count, depth, content_id, tree_root, descriptor, adaptive

    def _read_node(self, digest: bytes, *, level: int, start: int, expected_count: int) -> list[bytes]:
        raw = (self.path / "nodes" / f"{digest.hex()}.node").read_bytes()
        if self._verify_digest(raw) != digest or len(raw) < NODE_HEADER.size:
            raise StoreIntegrityError("Store content node integrity failure")
        magic, version, reserved, actual_level, actual_start, count = NODE_HEADER.unpack_from(raw)
        if (
            magic != NODE_MAGIC
            or version != VERSION
            or reserved != 0
            or actual_level != level
            or actual_start != start
            or count != expected_count
            or len(raw) != NODE_HEADER.size + count * 32
        ):
            raise StoreIntegrityError("Store content node structure is invalid")
        return [raw[NODE_HEADER.size + index * 32 : NODE_HEADER.size + (index + 1) * 32] for index in range(count)]

    def _read_tree(
        self,
        *,
        tree_root: bytes,
        depth: int,
        chunk_count: int,
        total_length: int,
    ) -> bytes:
        def visit(digest: bytes, level: int, start: int) -> list[bytes]:
            span = FANOUT**level
            remaining = chunk_count - start
            expected_count = min(FANOUT, (remaining + span - 1) // span)
            if expected_count < 1:
                raise StoreIntegrityError("Store content tree contains an empty branch")
            children = self._read_node(digest, level=level, start=start, expected_count=expected_count)
            if level == 0:
                result: list[bytes] = []
                for index, child in enumerate(children):
                    chunk_index = start + index
                    raw = (self.path / "chunks" / f"{child.hex()}.chunk").read_bytes()
                    if self._verify_digest(raw) != child:
                        raise StoreIntegrityError(f"Store content chunk {chunk_index} failed integrity")
                    expected_length = min(CHUNK_SIZE, total_length - chunk_index * CHUNK_SIZE)
                    if expected_length < 0 or len(raw) != expected_length:
                        raise StoreIntegrityError("Store content chunk length binding failed")
                    result.append(raw)
                return result
            result = []
            for index, child in enumerate(children):
                result.extend(visit(child, level - 1, start + index * span))
            return result

        chunks = visit(tree_root, depth, 0)
        payload = b"".join(chunks)
        if len(payload) != total_length:
            raise StoreIntegrityError("Store content tree total length failed")
        return payload

    def _read_entry(self, entry: _Entry, generation: int, *, verify_payload: bool) -> StoredObject:
        schema, identity, logical, content, root, binding_generation = self._read_binding(entry.binding_id)
        if logical != entry.logical_id or content != entry.content_id or root != entry.representation_root:
            raise StoreIntegrityError("Store binding does not match generation entry")
        if binding_generation > generation:
            raise StoreIntegrityError("Store binding points into a future generation")
        _raw, total_length, chunk_count, depth, content_id, tree_root, descriptor, _adaptive = (
            self._read_manifest(entry)
        )
        if total_length != entry.total_length:
            raise StoreIntegrityError("Store generation length does not match its manifest")
        payload = self._read_tree(
            tree_root=tree_root,
            depth=depth,
            chunk_count=chunk_count,
            total_length=total_length,
        )
        if verify_payload and (len(payload) != total_length or self._verify_digest(payload) != content_id):
            raise StoreIntegrityError("Store content identity verification failed")
        return StoredObject(
            domain_schema=schema,
            domain_identity=identity,
            logical_id=logical,
            content_id=content,
            representation_root=root,
            binding_id=entry.binding_id,
            generation=generation,
            ordinal=entry.ordinal,
            descriptor=descriptor,
            payload=payload,
        )

    # ---- adaptive representations ------------------------------------
    #
    # Logical identity stays stable while physical form varies: one object
    # may own several representations (base chunks, coded blobs, synopses)
    # selected under a generic access intent, with independently
    # retrievable semantic blocks. All record bytes, validation verdicts,
    # cost estimates, selection ranks, closure masks, and codec windows
    # are MNCS-computed through the retained session; this layer only
    # transports bytes, stages immutable files, and folds total MNCS
    # verdicts. Objects stored without adaptive parameters keep the exact
    # v2 manifest bytes; their adaptive view is synthesized
    # deterministically through MNCS at read time.

    def _frame_rle(self, payload: bytes) -> bytes:
        """Encode payload windows through MNCS RLE into framed blob bytes."""

        encoded = self.session.rle_encode_windows(bytes(payload))
        framed = bytearray()
        offset = 0
        for window in encoded:
            plain_length = min(RLE_WINDOW, len(payload) - offset)
            framed += RLE_FRAME.pack(len(window), plain_length)
            framed += window
            offset += plain_length
        if offset != len(payload):
            raise StoreIntegrityError("Store RLE framing does not cover its payload")
        return bytes(framed)

    def _parse_rle_framed(self, blob: bytes) -> list[tuple[bytes, int]]:
        """Split framed RLE bytes into (window, plain length) transport units."""

        blob = bytes(blob)
        windows: list[tuple[bytes, int]] = []
        offset = 0
        while offset < len(blob):
            if offset + RLE_FRAME.size > len(blob):
                raise StoreIntegrityError("Store RLE framing is truncated")
            encoded_length, plain_length = RLE_FRAME.unpack_from(blob, offset)
            offset += RLE_FRAME.size
            if encoded_length > 65 or plain_length > RLE_WINDOW:
                raise StoreIntegrityError("Store RLE framing lengths are out of range")
            if offset + encoded_length > len(blob):
                raise StoreIntegrityError("Store RLE framing overruns its blob")
            windows.append((blob[offset : offset + encoded_length], plain_length))
            offset += encoded_length
        return windows

    def _decode_rle_blob(self, blob: bytes) -> bytes:
        """Decode framed RLE bytes through MNCS; fail closed on any window."""

        return self.session.rle_decode_windows(self._parse_rle_framed(blob))

    def _check_block_inputs(
        self, blocks: list[BlockInput], total_length: int
    ) -> None:
        """Validate producer block spans as input (DENIED), before MNCS."""

        if len(blocks) > MAX_BLOCKS_PER_TABLE:
            raise StoreError(
                StoreResultCode.DENIED,
                f"Store block tables hold at most {MAX_BLOCKS_PER_TABLE} blocks",
            )
        seen: set[int] = set()
        for block in blocks:
            if not 0 <= block.index < 64:
                raise StoreError(StoreResultCode.DENIED, "Store block index is out of range")
            if block.index in seen:
                raise StoreError(StoreResultCode.DENIED, "Store block indices must be unique")
            seen.add(block.index)
            if block.start < 0 or block.length < 0:
                raise StoreError(StoreResultCode.DENIED, "Store block span is negative")
            if block.start + block.length > total_length:
                raise StoreError(
                    StoreResultCode.DENIED, "Store block span exceeds its payload"
                )
            if block.tag < 0 or block.tag >= 1 << 64:
                raise StoreError(StoreResultCode.DENIED, "Store block tag is out of range")
        # V1 covers one table whose indices tile 0..N-1, so a
        # representation range always names exactly the described blocks.
        # The MNCS layer stays general (sparse multi-table indices);
        # lifting this restriction is transport work, not new semantics.
        if seen != set(range(len(blocks))):
            raise StoreError(
                StoreResultCode.DENIED,
                "Store v1 block indices must tile 0..N-1",
            )
        for left_pos, left in enumerate(blocks):
            for right in blocks[left_pos + 1 :]:
                if left.length == 0 or right.length == 0:
                    continue
                if left.start < right.start + right.length and right.start < left.start + left.length:
                    raise StoreError(StoreResultCode.DENIED, "Store block spans overlap")

    def _build_adaptive_sidecars(
        self,
        *,
        payload: bytes,
        descriptor_id: bytes,
        logical_id: bytes,
        content_id: bytes,
        new_generation: int,
        synopsis: bytes | None,
        representations: list[RepresentationInput],
        blocks: list[BlockInput],
        relations: list[bytes],
        provenance: list[bytes],
    ) -> tuple[bytes, bytes]:
        """Stage immutable adaptive sidecars; return (envelope, reps) digests.

        Every record is MNCS-encoded and MNCS-validated before staging.
        Producer-input violations raise DENIED; a failure after successful
        validation indicates a Store defect and raises INTEGRITY_FAILURE.
        """

        payload = bytes(payload)
        total_length = len(payload)
        self._check_block_inputs(blocks, total_length)
        if synopsis is not None and len(synopsis) == 0:
            raise StoreError(StoreResultCode.DENIED, "Store synopsis must be nonempty")
        if synopsis is not None and len(synopsis) > MAX_SYNOPSIS_BYTES:
            raise StoreError(StoreResultCode.DENIED, "Store synopsis exceeds its bound")
        for rep in representations:
            if rep.fidelity < 0 or rep.fidelity > 5:
                raise StoreError(StoreResultCode.DENIED, "Store representation fidelity is unknown")
            if rep.codec not in ("identity", "rle"):
                raise StoreError(
                    StoreResultCode.DENIED, f"Store representation codec is unknown: {rep.codec}"
                )

        identity_codec = self.session.codec_identity_for(IDENTITY_CODEC_CODE)
        rle_codec = self.session.codec_identity_for(RLE_CODEC_CODE)
        if not self.session.codec_is_supported(
            IDENTITY_CODEC_CODE
        ) or not self.session.codec_is_supported(RLE_CODEC_CODE):
            raise StoreIntegrityError("Store well-known codecs are unsupported")

        # Opaque representation blobs: synopsis plus producer payloads.
        records: list[bytes] = []
        synopsis_root = bytes(32)
        synopsis_bytes = 0
        if synopsis is not None:
            synopsis = bytes(synopsis)
            synopsis_root = self._hash(synopsis)
            synopsis_bytes = len(synopsis)
            self._write_immutable(self.path / "reps" / f"{synopsis_root.hex()}.payload", synopsis)
            records.append(
                self.session.encode_representation(
                    fidelity=1,
                    codec=IDENTITY_CODEC_CODE,
                    codec_identity=identity_codec,
                    stored=len(synopsis),
                    plain=len(synopsis),
                    decode_class=0,
                    first_block=0,
                    block_count=0,
                    root=synopsis_root,
                    flags=0,
                )
            )
        for rep in representations:
            plain = bytes(rep.payload)
            plain_id = self._hash(plain)
            if rep.fidelity == 5 and plain_id != content_id:
                raise StoreError(
                    StoreResultCode.DENIED,
                    "Store fidelity-5 representation does not match its object content",
                )
            if rep.codec == "rle":
                framed = self._frame_rle(plain)
                # Codec correctness is verified before admission: the MNCS
                # decode of the staged bytes must reproduce the payload.
                if self._hash(self._decode_rle_blob(framed)) != plain_id:
                    raise StoreIntegrityError("Store RLE round trip failed before admission")
                stored = framed
                codec_code = RLE_CODEC_CODE
                codec_identity = rle_codec
                decode_class = 1
            else:
                stored = plain
                codec_code = IDENTITY_CODEC_CODE
                codec_identity = identity_codec
                decode_class = 0
            root = self._hash(stored)
            self._write_immutable(self.path / "reps" / f"{root.hex()}.payload", stored)
            exact = rep.fidelity == 5 and plain_id == content_id
            records.append(
                self.session.encode_representation(
                    fidelity=rep.fidelity,
                    codec=codec_code,
                    codec_identity=codec_identity,
                    stored=len(stored),
                    plain=len(plain),
                    decode_class=decode_class,
                    first_block=0,
                    block_count=0,
                    root=root,
                    flags=1 if exact else 0,
                )
            )

        # Object block table over base-payload spans.
        table = b""
        table_digest = bytes(32)
        if blocks:
            encoded_blocks = []
            for block in blocks:
                span = payload[block.start : block.start + block.length]
                digest = self._hash(span)
                if digest == bytes(32):
                    raise StoreError(StoreResultCode.DENIED, "Store block digest is degenerate")
                encoded_blocks.append(
                    self.session.encode_block(
                        index=block.index,
                        digest=digest,
                        stored=len(span),
                        plain=len(span),
                        tag=block.tag,
                        start=block.start,
                        length=block.length,
                        flags=2,
                    )
                )
            table = b"".join(encoded_blocks)
            self.session.validate_block_table(table, len(blocks))
            if not self.session.block_spans_within(table, len(blocks), total_length):
                raise StoreError(
                    StoreResultCode.DENIED, "Store block spans exceed their payload"
                )
            table_file = BLOCKS_HEADER.pack(len(blocks)) + table
            table_digest = self._hash(table_file)
            self._write_immutable(self.path / "blocks" / f"{table_digest.hex()}.table", table_file)

        # Base representation first by convention: exact identity bytes
        # over the chunk tree, with block coverage when a table exists.
        base = self.session.encode_representation(
            fidelity=5,
            codec=IDENTITY_CODEC_CODE,
            codec_identity=identity_codec,
            stored=total_length,
            plain=total_length,
            decode_class=0,
            first_block=0,
            block_count=len(blocks) if blocks else 1,
            root=content_id,
            flags=1 | (2 if blocks else 0),
        )
        records.insert(0, base)
        for record in records:
            self.session.validate_representation(record)
        fields = [self.session.representation_fields(record) for record in records]
        # Representation roots must be unique: plans address
        # representations by root, and duplicates would make that
        # lookup ambiguous.
        roots = [entry["root"] for entry in fields]
        if len(set(roots)) != len(roots):
            raise StoreError(
                StoreResultCode.DENIED, "Store representation roots must be unique"
            )
        reps_file = REPS_HEADER.pack(len(records)) + b"".join(records)
        reps_digest = self._hash(reps_file)
        self._write_immutable(
            self.path / "representations" / f"{reps_digest.hex()}.reps", reps_file
        )

        # Envelope: fidelity bitmap folded through MNCS over validated
        # representation fidelities plus the structural level.
        bitmap = 1
        bitmap = self.session.fidelity_bitmap_with(bitmap, 2)
        for entry in fields:
            fidelity = entry["fidelity"]
            assert isinstance(fidelity, int)
            bitmap = self.session.fidelity_bitmap_with(bitmap, fidelity)
        provenance_digest = (
            self._hash(b"".join(relations) + b"".join(provenance))
            if relations or provenance
            else bytes(32)
        )
        envelope = self.session.encode_envelope(
            logical=logical_id,
            type_identity=descriptor_id,
            synopsis=synopsis_root,
            synopsis_bytes=synopsis_bytes,
            rep_count=len(records),
            block_count=len(blocks) if blocks else 1,
            stored_bytes=total_length,
            plain_bytes=total_length,
            default_root=content_id,
            provenance=provenance_digest,
            block_table=table_digest,
            generation=new_generation,
            fidelity_bits=bitmap,
            flags=(1 if synopsis is not None else 0)
            | (2 if len(records) > 1 else 0)
            | (4 if blocks else 0),
        )
        self.session.validate_envelope(envelope)
        envelope_digest = self._hash(envelope)
        self._write_immutable(
            self.path / "envelopes" / f"{envelope_digest.hex()}.envelope", envelope
        )
        return envelope_digest, reps_digest

    def put_bound_object(
        self,
        *,
        domain_schema: bytes,
        domain_identity: bytes,
        descriptor: bytes,
        payload: bytes,
        expected_generation: int,
        relations: list[bytes] | tuple[bytes, ...] = (),
        provenance: list[bytes] | tuple[bytes, ...] = (),
        synopsis: bytes | None = None,
        representations: list[RepresentationInput] | tuple[RepresentationInput, ...] = (),
        blocks: list[BlockInput] | tuple[BlockInput, ...] = (),
    ) -> CommitResult:
        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        if expected_generation < 0:
            raise StoreError(StoreResultCode.DENIED, "expected generation must be non-negative")
        domain_schema = bytes(domain_schema)
        domain_identity = bytes(domain_identity)
        descriptor = bytes(descriptor)
        payload = bytes(payload)
        relations = [bytes(value) for value in relations]
        provenance = [bytes(value) for value in provenance]
        representations = list(representations)
        blocks = list(blocks)
        binding_id = self._binding_id(domain_schema, domain_identity)
        logical_id = self._logical_id(binding_id)
        with self._publication_lock():
            observed = self.current_generation
            if self._cas(observed, expected_generation) != 0:
                token = self._conflict_token(observed, expected_generation)
                return CommitResult(
                    StoreResultCode.STALE_GENERATION,
                    observed,
                    logical_id,
                    b"",
                    b"",
                    binding_id,
                    expected_generation,
                    observed,
                    token,
                )
            old_entries, old_relations, old_provenance = self._read_generation_parts(observed)
            # A cleanly opened generation has already been fully verified and
            # is still exact when the generation/feed identity is unchanged.
            # Reuse that proof for publication validation; a missing or
            # mismatched projection falls back to a complete Store read.
            if not self._projection_matches_entries(observed, old_entries):
                self._verified_projection(observed)
            verified_projection = self._projection
            active_bindings = {entry.binding_id for entry in old_entries}
            existing_path = self._binding_path(binding_id)
            if existing_path.exists():
                existing = self._read_binding(binding_id)
                if binding_id in active_bindings:
                    candidate_content = self._hash(payload)
                    if existing[3] != candidate_content:
                        raise StoreError(
                            StoreResultCode.IDENTITY_CONFLICT,
                            "domain identity already names different Store content",
                        )
                    return CommitResult(
                        StoreResultCode.DUPLICATE,
                        observed,
                        existing[2],
                        existing[3],
                        existing[4],
                        binding_id,
                        expected_generation,
                        observed,
                    )
                # A binding can survive a process death before generation
                # publication.  It is not authoritative until the current
                # committed generation names it, so remove only this orphan
                # while holding the publication lock.
                existing_path.unlink()
                _sync_directory(existing_path.parent)

            new_generation = observed + 1
            self._validate_typed_records(
                relations,
                provenance,
                new_generation,
                require_current_generation=True,
            )
            adaptive = synopsis is not None or bool(representations) or bool(blocks)
            envelope_digest: bytes | None = None
            reps_digest: bytes | None = None
            if adaptive:
                # Content identity is known before chunk staging; the
                # sidecars bind to it and the manifest binds to them.
                staged_content = self._hash(payload)
                envelope_digest, reps_digest = self._build_adaptive_sidecars(
                    payload=payload,
                    descriptor_id=self._hash(descriptor),
                    logical_id=logical_id,
                    content_id=staged_content,
                    new_generation=new_generation,
                    synopsis=synopsis,
                    representations=representations,
                    blocks=blocks,
                    relations=relations,
                    provenance=provenance,
                )
            content_id, root, _descriptor_id, _chunks, _depth = self._build_content(
                payload=payload,
                descriptor=descriptor,
                binding_id=binding_id,
                logical_id=logical_id,
                envelope_digest=envelope_digest,
                reps_digest=reps_digest,
            )
            next_ordinal = max((entry.ordinal for entry in old_entries), default=observed) + 1
            new_entry = _Entry(logical_id, binding_id, root, content_id, len(payload), next_ordinal)
            entries = old_entries + [new_entry]
            generation_raw = self._generation_bytes(
                new_generation,
                entries,
                old_relations + relations,
                old_provenance + provenance,
            )
            transaction = self.path / "staging" / f"{observed:016x}-{new_generation:016x}"
            transaction.mkdir(mode=0o700, parents=True, exist_ok=False)
            staged_generation = transaction / "generation.stage"
            _durable_write(staged_generation, generation_raw, exclusive=True)
            generation_digest = self._hash(generation_raw)
            _durable_write(
                transaction / "journal",
                JOURNAL.pack(JOURNAL_MAGIC, VERSION, 0, observed, new_generation, generation_digest),
                exclusive=True,
            )
            _sync_directory(transaction)
            # The binding and all immutable representation material are
            # durable before either the generation or head can become
            # authoritative.  Recovery can therefore verify old-or-new
            # without relying on filenames or write order.
            self._write_binding(
                domain_schema=domain_schema,
                domain_identity=domain_identity,
                binding_id=binding_id,
                logical_id=logical_id,
                content_id=content_id,
                representation_root=root,
                generation=new_generation,
            )
            self._trip("after_binding_durability")
            self._trip("before_generation_publication")
            os.replace(staged_generation, self.path / "generations" / f"{new_generation:016x}")
            _sync_directory(self.path / "generations")
            self._write_journal_state(
                transaction,
                state=1,
                old=observed,
                new=new_generation,
                digest=generation_digest,
            )
            self._trip("after_generation_publication")
            self._publish_head(new_generation)
            self._write_journal_state(
                transaction,
                state=2,
                old=observed,
                new=new_generation,
                digest=generation_digest,
            )
            self._trip("after_head_publication")
            self._trip("during_cleanup")
            (transaction / "journal").unlink(missing_ok=True)
            transaction.rmdir()
            # The publication is already fully validated above.  Extend the
            # resident generation-bound read projection with the just-written
            # object instead of forcing the next Forge query to reread every
            # unchanged object.  Store's generation head and immutable object
            # identities remain authoritative; this is only a rebuildable
            # in-process cache.
            self._commit_feed_generation = None
            self._commit_feed = None
            self._projection_generation = new_generation
            committed_object = StoredObject(
                domain_schema=domain_schema,
                domain_identity=domain_identity,
                logical_id=logical_id,
                content_id=content_id,
                representation_root=root,
                binding_id=binding_id,
                generation=new_generation,
                ordinal=new_entry.ordinal,
                descriptor=descriptor,
                payload=payload,
            )
            self._projection = (*verified_projection, committed_object)
            self._extend_lookup_maps(committed_object)
            return CommitResult(
                StoreResultCode.COMMITTED,
                new_generation,
                logical_id,
                content_id,
                root,
                binding_id,
                expected_generation,
                observed,
            )

    def put_bound_objects(
        self,
        objects: Sequence[BoundObjectInput],
        *,
        expected_generation: int,
    ) -> BatchCommitResult:
        """Publish several new bound objects as one generation.

        Immutable content may be staged before publication, but the generation
        journal and head are advanced only once.  A stale expected generation
        returns without retrying; an accepted batch is therefore visible as
        the old complete generation or the new complete generation after
        recovery.
        """

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        if expected_generation < 0:
            raise StoreError(StoreResultCode.DENIED, "expected generation must be non-negative")
        if not objects:
            raise StoreError(StoreResultCode.DENIED, "Store batch must contain at least one object")

        normalized: list[BoundObjectInput] = []
        bindings: list[bytes] = []
        seen_bindings: set[bytes] = set()
        all_relations: list[bytes] = []
        all_provenance: list[bytes] = []
        for item in objects:
            if not isinstance(item, BoundObjectInput):
                raise StoreError(StoreResultCode.DENIED, "Store batch object has an invalid type")
            domain_schema = bytes(item.domain_schema)
            domain_identity = bytes(item.domain_identity)
            descriptor = bytes(item.descriptor)
            payload = bytes(item.payload)
            relations = tuple(bytes(value) for value in item.relations)
            provenance = tuple(bytes(value) for value in item.provenance)
            binding_id = self._binding_id(domain_schema, domain_identity)
            if binding_id in seen_bindings:
                raise StoreError(StoreResultCode.DENIED, "Store batch repeats a domain binding")
            seen_bindings.add(binding_id)
            bindings.append(binding_id)
            all_relations.extend(relations)
            all_provenance.extend(provenance)
            normalized.append(
                BoundObjectInput(
                    domain_schema,
                    domain_identity,
                    descriptor,
                    payload,
                    relations,
                    provenance,
                )
            )

        with self._publication_lock():
            observed = self.current_generation
            if self._cas(observed, expected_generation) != 0:
                token = self._conflict_token(observed, expected_generation)
                return BatchCommitResult(
                    StoreResultCode.STALE_GENERATION,
                    observed,
                    (),
                    expected_generation,
                    observed,
                    token,
                )
            old_entries, old_relations, old_provenance = self._read_generation_parts(observed)
            if not self._projection_matches_entries(observed, old_entries):
                self._verified_projection(observed)
            verified_projection = self._projection
            active_bindings = {entry.binding_id for entry in old_entries}
            duplicate_results: list[CommitResult] = []
            new_items: list[tuple[BoundObjectInput, bytes, bytes]] = []
            for item, binding_id in zip(normalized, bindings, strict=True):
                existing_path = self._binding_path(binding_id)
                if not existing_path.exists():
                    new_items.append((item, binding_id, self._logical_id(binding_id)))
                    continue
                existing = self._read_binding(binding_id)
                if binding_id in active_bindings:
                    candidate_content = self._hash(item.payload)
                    if existing[3] != candidate_content:
                        raise StoreError(
                            StoreResultCode.IDENTITY_CONFLICT,
                            "domain identity already names different Store content",
                        )
                    duplicate_results.append(
                        CommitResult(
                            StoreResultCode.DUPLICATE,
                            observed,
                            existing[2],
                            existing[3],
                            existing[4],
                            binding_id,
                            expected_generation,
                            observed,
                        )
                    )
                    continue
                # A binding left by a failed prior publication is not
                # authoritative until a committed generation names it.
                existing_path.unlink()
                _sync_directory(existing_path.parent)
                new_items.append((item, binding_id, self._logical_id(binding_id)))

            if duplicate_results and new_items:
                raise StoreError(
                    StoreResultCode.IDENTITY_CONFLICT,
                    "Store batch cannot mix duplicate and new domain bindings",
                )
            if duplicate_results:
                return BatchCommitResult(
                    StoreResultCode.DUPLICATE,
                    observed,
                    tuple(duplicate_results),
                    expected_generation,
                    observed,
                )

            new_generation = observed + 1
            self._validate_typed_records(
                all_relations,
                all_provenance,
                new_generation,
                require_current_generation=True,
            )
            next_ordinal = max((entry.ordinal for entry in old_entries), default=observed) + 1
            entries = list(old_entries)
            built: list[
                tuple[BoundObjectInput, bytes, bytes, bytes, bytes, int]
            ] = []
            for index, (item, binding_id, logical_id) in enumerate(new_items):
                content_id, root, _descriptor_id, _chunks, _depth = self._build_content(
                    payload=item.payload,
                    descriptor=item.descriptor,
                    binding_id=binding_id,
                    logical_id=logical_id,
                )
                ordinal = next_ordinal + index
                entry = _Entry(logical_id, binding_id, root, content_id, len(item.payload), ordinal)
                entries.append(entry)
                built.append((item, binding_id, logical_id, content_id, root, ordinal))
            generation_raw = self._generation_bytes(
                new_generation,
                entries,
                old_relations + all_relations,
                old_provenance + all_provenance,
            )
            transaction = self.path / "staging" / f"{observed:016x}-{new_generation:016x}"
            transaction.mkdir(mode=0o700, parents=True, exist_ok=False)
            staged_generation = transaction / "generation.stage"
            _durable_write(staged_generation, generation_raw, exclusive=True)
            generation_digest = self._hash(generation_raw)
            _durable_write(
                transaction / "journal",
                JOURNAL.pack(JOURNAL_MAGIC, VERSION, 0, observed, new_generation, generation_digest),
                exclusive=True,
            )
            _sync_directory(transaction)
            for item, binding_id, logical_id, content_id, root, _ordinal in built:
                self._write_binding(
                    domain_schema=item.domain_schema,
                    domain_identity=item.domain_identity,
                    binding_id=binding_id,
                    logical_id=logical_id,
                    content_id=content_id,
                    representation_root=root,
                    generation=new_generation,
                )
            self._trip("after_binding_durability")
            self._trip("before_generation_publication")
            os.replace(staged_generation, self.path / "generations" / f"{new_generation:016x}")
            _sync_directory(self.path / "generations")
            self._write_journal_state(
                transaction,
                state=1,
                old=observed,
                new=new_generation,
                digest=generation_digest,
            )
            self._trip("after_generation_publication")
            self._publish_head(new_generation)
            self._write_journal_state(
                transaction,
                state=2,
                old=observed,
                new=new_generation,
                digest=generation_digest,
            )
            self._trip("after_head_publication")
            self._trip("during_cleanup")
            (transaction / "journal").unlink(missing_ok=True)
            transaction.rmdir()

            self._commit_feed_generation = None
            self._commit_feed = None
            self._projection_generation = new_generation
            committed_objects: list[StoredObject] = []
            commit_results: list[CommitResult] = []
            for item, binding_id, logical_id, content_id, root, ordinal in built:
                committed_objects.append(
                    StoredObject(
                        domain_schema=item.domain_schema,
                        domain_identity=item.domain_identity,
                        logical_id=logical_id,
                        content_id=content_id,
                        representation_root=root,
                        binding_id=binding_id,
                        generation=new_generation,
                        ordinal=ordinal,
                        descriptor=item.descriptor,
                        payload=item.payload,
                    )
                )
                commit_results.append(
                    CommitResult(
                        StoreResultCode.COMMITTED,
                        new_generation,
                        logical_id,
                        content_id,
                        root,
                        binding_id,
                        expected_generation,
                        observed,
                    )
                )
            self._projection = (*verified_projection, *committed_objects)
            for item in committed_objects:
                self._extend_lookup_maps(item)
            return BatchCommitResult(
                StoreResultCode.COMMITTED,
                new_generation,
                tuple(commit_results),
                expected_generation,
                observed,
            )

    def _resolve_entry(
        self, domain_schema: bytes, domain_identity: bytes
    ) -> tuple[_Entry, int, int, int]:
        """Resolve one current entry without reading any payload.

        Returns (entry, head generation, commit generation, bytes read
        for resolution). Binding consistency is checked exactly as in
        the whole-object path. The commit generation comes from the
        verified binding: envelopes claim the generation that committed
        their object version, which may precede the current head.
        """

        binding_id = self._binding_id(bytes(domain_schema), bytes(domain_identity))
        generation = self.current_generation
        generation_path = self.path / "generations" / f"{generation:016x}"
        generation_raw = generation_path.read_bytes()
        resolve_bytes = len(generation_raw)
        entries = self._parse_generation(generation_raw, generation)
        try:
            entry = next(item for item in entries if item.binding_id == binding_id)
        except StopIteration as exc:
            raise StoreError(
                StoreResultCode.DENIED, "Store object binding is not current"
            ) from exc
        binding_path = self._binding_path(binding_id)
        binding_raw = binding_path.read_bytes()
        resolve_bytes += len(binding_raw)
        schema, identity, logical, content, root, binding_generation = self._read_binding(
            binding_id
        )
        if logical != entry.logical_id or content != entry.content_id or root != entry.representation_root:
            raise StoreIntegrityError("Store binding does not match generation entry")
        if binding_generation > generation:
            raise StoreIntegrityError("Store binding points into a future generation")
        if schema != bytes(domain_schema) or identity != bytes(domain_identity):
            raise StoreIntegrityError("Store binding does not match its domain identity")
        return entry, generation, binding_generation, resolve_bytes

    def _read_envelope_part(
        self, entry: _Entry, commit_generation: int
    ) -> dict[str, object]:
        """Read manifest + envelope only; never chunks, blobs, or tables."""

        (
            manifest_raw,
            total_length,
            chunk_count,
            depth,
            content_id,
            tree_root,
            descriptor,
            adaptive,
        ) = self._read_manifest(entry)
        if total_length != entry.total_length:
            raise StoreIntegrityError("Store generation length does not match its manifest")
        inspect_bytes = len(manifest_raw)
        if adaptive is None:
            envelope = self.session.default_envelope_for(
                entry.logical_id, content_id, total_length, commit_generation
            )
            self.session.validate_envelope(envelope)
            envelope_digest: bytes | None = None
            reps_digest: bytes | None = None
        else:
            envelope_digest, reps_digest = adaptive
            envelope_path = self.path / "envelopes" / f"{envelope_digest.hex()}.envelope"
            envelope = envelope_path.read_bytes()
            inspect_bytes += len(envelope)
            if self._verify_digest(envelope) != envelope_digest:
                raise StoreIntegrityError("Store envelope integrity failure")
            self.session.validate_envelope(envelope)
            if not self.session.envelope_consistent(
                envelope, entry.logical_id, content_id, total_length, commit_generation
            ):
                raise StoreIntegrityError("Store envelope does not match its manifest")
        fields = self.session.envelope_fields(envelope)
        return {
            "manifest": manifest_raw,
            "total_length": total_length,
            "chunk_count": chunk_count,
            "depth": depth,
            "content_id": content_id,
            "tree_root": tree_root,
            "descriptor": descriptor,
            "adaptive": adaptive is not None,
            "envelope_digest": envelope_digest,
            "reps_digest": reps_digest,
            "envelope": envelope,
            "envelope_fields": fields,
            "inspect_bytes": inspect_bytes,
        }

    def _read_rep_tables(self, part: dict[str, object]) -> dict[str, object]:
        """Load representation + block tables named by a verified envelope."""

        envelope = part["envelope"]
        assert isinstance(envelope, bytes)
        fields = part["envelope_fields"]
        assert isinstance(fields, dict)
        content_id = part["content_id"]
        assert isinstance(content_id, bytes)
        total_length = part["total_length"]
        assert isinstance(total_length, int)
        inspect_bytes = part["inspect_bytes"]
        assert isinstance(inspect_bytes, int)
        representations: list[bytes] = []
        rep_fields: list[dict] = []
        tables: list[tuple[bytes, int]] = []
        if not part["adaptive"]:
            identity_codec = self.session.codec_identity_for(IDENTITY_CODEC_CODE)
            base = self.session.encode_representation(
                fidelity=5,
                codec=IDENTITY_CODEC_CODE,
                codec_identity=identity_codec,
                stored=total_length,
                plain=total_length,
                decode_class=0,
                first_block=0,
                block_count=1,
                root=content_id,
                flags=1,
            )
            self.session.validate_representation(base)
            representations.append(base)
            rep_fields.append(self.session.representation_fields(base))
        else:
            reps_digest = part["reps_digest"]
            assert isinstance(reps_digest, bytes)
            reps_path = self.path / "representations" / f"{reps_digest.hex()}.reps"
            reps_raw = reps_path.read_bytes()
            inspect_bytes += len(reps_raw)
            if self._verify_digest(reps_raw) != reps_digest:
                raise StoreIntegrityError("Store representation table integrity failure")
            if len(reps_raw) < REPS_HEADER.size:
                raise StoreIntegrityError("Store representation table is truncated")
            (count,) = REPS_HEADER.unpack_from(reps_raw)
            if count < 1 or len(reps_raw) != REPS_HEADER.size + count * REPRESENTATION_SIZE:
                raise StoreIntegrityError("Store representation table has a torn tail")
            for index in range(count):
                record = reps_raw[
                    REPS_HEADER.size + index * REPRESENTATION_SIZE : REPS_HEADER.size
                    + (index + 1) * REPRESENTATION_SIZE
                ]
                self.session.validate_representation(record)
                representations.append(record)
                rep_fields.append(self.session.representation_fields(record))
            if fields["rep_count"] != count:
                raise StoreIntegrityError("Store envelope representation count mismatch")
            base_fields = rep_fields[0]
            if (
                base_fields["root"] != content_id
                or base_fields["fidelity"] != 5
                or base_fields["codec"] != IDENTITY_CODEC_CODE
            ):
                raise StoreIntegrityError("Store base representation violates its convention")
            for record_fields in rep_fields:
                fidelity = record_fields["fidelity"]
                assert isinstance(fidelity, int)
                if not self.session.envelope_offers(envelope, fidelity):
                    raise StoreIntegrityError("Store envelope omits a stored fidelity")
            table_digest = fields["block_table"]
            assert isinstance(table_digest, bytes)
            if table_digest != bytes(32):
                table_path = self.path / "blocks" / f"{table_digest.hex()}.table"
                table_raw = table_path.read_bytes()
                inspect_bytes += len(table_raw)
                if self._verify_digest(table_raw) != table_digest:
                    raise StoreIntegrityError("Store block table integrity failure")
                if len(table_raw) < BLOCKS_HEADER.size:
                    raise StoreIntegrityError("Store block table is truncated")
                (table_count,) = BLOCKS_HEADER.unpack_from(table_raw)
                table = table_raw[BLOCKS_HEADER.size :]
                if table_count < 1 or len(table) != table_count * BLOCK_SIZE:
                    raise StoreIntegrityError("Store block table has a torn tail")
                self.session.validate_block_table(table, table_count)
                if not self.session.block_spans_within(table, table_count, total_length):
                    raise StoreIntegrityError("Store block spans exceed their payload")
                if fields["block_count"] != table_count:
                    raise StoreIntegrityError("Store envelope block count mismatch")
                tables.append((table, table_count))
        if not tables:
            # The implicit whole-payload block becomes a real one-block
            # table, synthesized deterministically through MNCS. Every
            # downstream path (closure, caps, plans, digests) then runs
            # uniformly whether or not a table was stored.
            implicit = self.session.encode_block(
                index=0,
                digest=content_id,
                stored=total_length,
                plain=total_length,
                tag=0,
                start=0,
                length=total_length,
                flags=2,
            )
            self.session.validate_block_table(implicit, 1)
            if not self.session.block_spans_within(implicit, 1, total_length):
                raise StoreIntegrityError("Store implicit block span is invalid")
            tables.append((implicit, 1))
        part["representations"] = representations
        part["rep_fields"] = rep_fields
        part["tables"] = tables
        part["inspect_bytes"] = inspect_bytes
        return part

    def _read_tree_ranges(
        self,
        *,
        tree_root: bytes,
        depth: int,
        chunk_count: int,
        total_length: int,
        wanted: frozenset[int],
    ) -> tuple[dict[int, bytes], int]:
        """Fetch only wanted chunks, verifying every node and chunk on path."""

        for chunk_index in wanted:
            if chunk_index < 0 or chunk_index >= chunk_count:
                raise StoreIntegrityError("Store selective read names an unknown chunk")
        found: dict[int, bytes] = {}
        touched = 0
        if not wanted:
            return found, touched

        def visit(digest: bytes, level: int, start: int) -> None:
            nonlocal touched
            span = FANOUT**level
            remaining = chunk_count - start
            expected_count = min(FANOUT, (remaining + span - 1) // span)
            if expected_count < 1:
                raise StoreIntegrityError("Store content tree contains an empty branch")
            node_path = self.path / "nodes" / f"{digest.hex()}.node"
            touched += node_path.stat().st_size
            children = self._read_node(
                digest, level=level, start=start, expected_count=expected_count
            )
            if level == 0:
                for index, child in enumerate(children):
                    chunk_index = start + index
                    if chunk_index not in wanted:
                        continue
                    chunk_path = self.path / "chunks" / f"{child.hex()}.chunk"
                    raw = chunk_path.read_bytes()
                    touched += len(raw)
                    if self._verify_digest(raw) != child:
                        raise StoreIntegrityError(
                            f"Store content chunk {chunk_index} failed integrity"
                        )
                    expected_length = min(CHUNK_SIZE, total_length - chunk_index * CHUNK_SIZE)
                    if expected_length < 0 or len(raw) != expected_length:
                        raise StoreIntegrityError("Store content chunk length binding failed")
                    found[chunk_index] = raw
                return
            for index, child in enumerate(children):
                child_start = start + index * span
                child_end = min(child_start + span, chunk_count)
                if any(child_start <= item < child_end for item in wanted):
                    visit(child, level - 1, child_start)

        visit(tree_root, depth, 0)
        if set(found) != set(wanted):
            raise StoreIntegrityError("Store selective read missed wanted chunks")
        return found, touched

    def _span_bytes(
        self,
        chunks: dict[int, bytes],
        total_length: int,
        start: int,
        length: int,
    ) -> bytes:
        """Assemble one span from fetched chunks (spans may cross chunks)."""

        if length == 0:
            return b""
        assembled = bytearray()
        cursor = start
        while cursor < start + length:
            chunk_index = cursor // CHUNK_SIZE
            chunk = chunks.get(chunk_index)
            if chunk is None:
                raise StoreIntegrityError("Store span references an unfetched chunk")
            within = cursor - chunk_index * CHUNK_SIZE
            take = min(len(chunk) - within, start + length - cursor)
            assembled += chunk[within : within + take]
            cursor += take
        if len(assembled) != length or start + length > total_length:
            raise StoreIntegrityError("Store span assembly failed its bounds")
        return bytes(assembled)

    def _fetch_block_spans(
        self,
        part: dict[str, object],
        mask: int,
    ) -> tuple[bytes, list[tuple[int, int, int]], int]:
        """Fetch in-mask block spans in index order; return (payload, spans, touched)."""

        tables = part["tables"]
        assert isinstance(tables, list)
        total_length = part["total_length"]
        assert isinstance(total_length, int)
        tree_root = part["tree_root"]
        assert isinstance(tree_root, bytes)
        depth = part["depth"]
        assert isinstance(depth, int)
        chunk_count = part["chunk_count"]
        assert isinstance(chunk_count, int)
        slots: list[tuple[int, int, int, bytes]] = []
        for table, count in tables:
            for slot in range(count):
                projected = self.session.slot_span(table, count, slot)
                index = projected["index"]
                assert isinstance(index, int)
                if (mask >> index) & 1 == 0:
                    continue
                start = projected["start"]
                length = projected["length"]
                digest = projected["digest"]
                assert isinstance(start, int) and isinstance(length, int)
                assert isinstance(digest, bytes)
                slots.append((index, start, length, digest))
        slots.sort(key=lambda item: item[0])
        wanted: set[int] = set()
        for _index, start, length, _digest in slots:
            if length == 0:
                continue
            wanted.update(range(start // CHUNK_SIZE, (start + length - 1) // CHUNK_SIZE + 1))
        chunks, touched = self._read_tree_ranges(
            tree_root=tree_root,
            depth=depth,
            chunk_count=chunk_count,
            total_length=total_length,
            wanted=frozenset(wanted),
        )
        payload_parts: list[bytes] = []
        spans: list[tuple[int, int, int]] = []
        for index, start, length, digest in slots:
            span_bytes = self._span_bytes(chunks, total_length, start, length)
            if self._verify_digest(span_bytes) != digest:
                raise StoreIntegrityError(f"Store block {index} failed its digest check")
            payload_parts.append(span_bytes)
            spans.append((index, start, length))
        return b"".join(payload_parts), spans, touched

    @staticmethod
    def _spans_tile_full(spans: list[tuple[int, int, int]], total_length: int) -> bool:
        """Decide whether fetched spans tile [0, total) exactly once."""

        if total_length == 0:
            return all(length == 0 for _index, _start, length in spans)
        ordered = sorted(spans, key=lambda item: item[1])
        cursor = 0
        for _index, start, length in ordered:
            if start != cursor or length <= 0:
                return False
            cursor += length
        return cursor == total_length

    def get_envelope(
        self, domain_schema: bytes, domain_identity: bytes
    ) -> EnvelopeView:
        """Inspect an object without touching any payload, blob, or table."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        entry, _generation, commit_generation, resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_envelope_part(entry, commit_generation)
        envelope = part["envelope"]
        assert isinstance(envelope, bytes)
        fields = part["envelope_fields"]
        assert isinstance(fields, dict)
        inspect_bytes = part["inspect_bytes"]
        assert isinstance(inspect_bytes, int)
        return EnvelopeView(
            raw=envelope, fields=fields, stored_bytes_touched=resolve_bytes + inspect_bytes
        )

    def get_representations(
        self, domain_schema: bytes, domain_identity: bytes
    ) -> list[RepresentationView]:
        """Return every verified representation of one current object."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        entry, _generation, commit_generation, _resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_rep_tables(self._read_envelope_part(entry, commit_generation))
        representations = part["representations"]
        rep_fields = part["rep_fields"]
        assert isinstance(representations, list) and isinstance(rep_fields, list)
        return [
            RepresentationView(index=index, raw=record, fields=record_fields)
            for index, (record, record_fields) in enumerate(zip(representations, rep_fields))
        ]

    def read_synopsis(self, domain_schema: bytes, domain_identity: bytes) -> bytes:
        """Fetch only the producer synopsis blob, never the payload."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        entry, _generation, commit_generation, _resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_envelope_part(entry, commit_generation)
        fields = part["envelope_fields"]
        assert isinstance(fields, dict)
        synopsis = fields["synopsis"]
        synopsis_bytes = fields["synopsis_bytes"]
        assert isinstance(synopsis, bytes) and isinstance(synopsis_bytes, int)
        if synopsis == bytes(32):
            raise StoreError(StoreResultCode.DENIED, "Store object has no synopsis")
        blob = (self.path / "reps" / f"{synopsis.hex()}.payload").read_bytes()
        if self._verify_digest(blob) != synopsis or len(blob) != synopsis_bytes:
            raise StoreIntegrityError("Store synopsis integrity failure")
        return blob

    def select_representation(
        self, domain_schema: bytes, domain_identity: bytes, intent: bytes | None = None
    ) -> Selection:
        """Select the winning representation under an intent (default: exact)."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        entry, _generation, commit_generation, _resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_rep_tables(self._read_envelope_part(entry, commit_generation))
        representations = part["representations"]
        assert isinstance(representations, list)
        if intent is None:
            intent_bytes = self.session.default_intent()
        else:
            self.session.validate_intent(bytes(intent))
            intent_bytes = self.session.normalize_intent(bytes(intent))
        winner, satisfied, cost = self.session.select_representation(representations, intent_bytes)
        rep_fields = part["rep_fields"]
        assert isinstance(rep_fields, list)
        return Selection(
            index=winner,
            record=representations[winner],
            fields=rep_fields[winner],
            satisfied=satisfied,
            estimated_cost=cost,
        )

    def read_blocks(
        self, domain_schema: bytes, domain_identity: bytes, mask: int
    ) -> MaterializedView:
        """Fetch base-representation blocks by mask with verified closure."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        if not isinstance(mask, int) or mask < 0 or mask >= 1 << 64:
            raise StoreError(StoreResultCode.DENIED, "Store block mask is out of range")
        entry, _generation, commit_generation, resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_rep_tables(self._read_envelope_part(entry, commit_generation))
        tables = part["tables"]
        rep_fields = part["rep_fields"]
        assert isinstance(tables, list) and isinstance(rep_fields, list)
        base_fields = rep_fields[0]
        first_block = base_fields["first_block"]
        block_count = base_fields["block_count"]
        base_fidelity = base_fields["fidelity"]
        assert isinstance(first_block, int) and isinstance(block_count, int)
        assert isinstance(base_fidelity, int)
        content_id = part["content_id"]
        assert isinstance(content_id, bytes)
        total_length = part["total_length"]
        assert isinstance(total_length, int)
        inspect_bytes = part["inspect_bytes"]
        assert isinstance(inspect_bytes, int)
        if not self.session.mask_within(mask, first_block, block_count):
            raise StoreError(StoreResultCode.DENIED, "Store block mask is out of range")
        closed = self.session.closure_fixpoint(
            [(table, count) for table, count in tables], mask
        )
        if not self.session.mask_within(closed, first_block, block_count):
            raise StoreError(
                StoreResultCode.DENIED, "Store block closure escapes its representation"
            )
        payload, spans, touched = self._fetch_block_spans(part, closed)
        exact_verified = False
        if self._spans_tile_full(spans, total_length):
            if self._hash(payload) != content_id:
                raise StoreIntegrityError("Store selective full read failed exactness")
            exact_verified = True
        return MaterializedView(
            payload=payload,
            representation_index=0,
            fidelity=base_fidelity,
            satisfied=True,
            mask=closed,
            stored_bytes_touched=touched,
            materialized_bytes=len(payload),
            inspect_bytes=resolve_bytes + inspect_bytes,
            exact_verified=exact_verified,
        )

    def materialize(
        self,
        domain_schema: bytes,
        domain_identity: bytes,
        *,
        intent: bytes | None = None,
        mask: int | None = None,
        tag: int | None = None,
    ) -> MaterializedView:
        """Select a representation under an intent and materialize it.

        Opaque representations (synopses, coded blobs) materialize whole;
        block-covered representations materialize the dependency closure
        of the requested mask, tag, or full range.
        """

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        if mask is not None and tag is not None:
            raise StoreError(StoreResultCode.DENIED, "Store materialize takes mask or tag, not both")
        if mask is not None and (not isinstance(mask, int) or mask < 0 or mask >= 1 << 64):
            raise StoreError(StoreResultCode.DENIED, "Store block mask is out of range")
        if tag is not None and (not isinstance(tag, int) or tag < 0 or tag >= 1 << 64):
            raise StoreError(StoreResultCode.DENIED, "Store block tag is out of range")
        entry, _generation, commit_generation, resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_rep_tables(self._read_envelope_part(entry, commit_generation))
        representations = part["representations"]
        rep_fields = part["rep_fields"]
        tables = part["tables"]
        assert isinstance(representations, list) and isinstance(rep_fields, list)
        assert isinstance(tables, list)
        content_id = part["content_id"]
        assert isinstance(content_id, bytes)
        total_length = part["total_length"]
        assert isinstance(total_length, int)
        inspect_bytes = part["inspect_bytes"]
        assert isinstance(inspect_bytes, int)
        if intent is None:
            intent_bytes = self.session.default_intent()
        else:
            self.session.validate_intent(bytes(intent))
            intent_bytes = self.session.normalize_intent(bytes(intent))
        winner, satisfied, _cost = self.session.select_representation(
            representations, intent_bytes
        )
        winner_fields = rep_fields[winner]
        fidelity = winner_fields["fidelity"]
        block_count = winner_fields["block_count"]
        first_block = winner_fields["first_block"]
        codec = winner_fields["codec"]
        root = winner_fields["root"]
        exact = winner_fields["exact"]
        assert isinstance(fidelity, int) and isinstance(block_count, int)
        assert isinstance(first_block, int) and isinstance(codec, int)
        assert isinstance(root, bytes) and isinstance(exact, int)
        if block_count == 0:
            if mask is not None or tag is not None:
                raise StoreError(
                    StoreResultCode.DENIED,
                    "Store opaque representation has no block coverage",
                )
            blob = (self.path / "reps" / f"{root.hex()}.payload").read_bytes()
            if self._verify_digest(blob) != root:
                raise StoreIntegrityError("Store representation blob integrity failure")
            if codec == RLE_CODEC_CODE:
                payload = self._decode_rle_blob(blob)
            elif codec == IDENTITY_CODEC_CODE:
                payload = blob
            else:
                raise StoreIntegrityError(f"Store cannot decode codec {codec}")
            exact_verified = False
            if exact:
                if self._hash(payload) != content_id:
                    raise StoreIntegrityError("Store exact representation failed exactness")
                exact_verified = True
            return MaterializedView(
                payload=payload,
                representation_index=winner,
                fidelity=fidelity,
                satisfied=satisfied,
                mask=0,
                stored_bytes_touched=len(blob),
                materialized_bytes=len(payload),
                inspect_bytes=resolve_bytes + inspect_bytes,
                exact_verified=exact_verified,
            )
        if tag is not None:
            wanted = 0
            for table, count in tables:
                wanted |= self.session.blocks_with_tag(table, count, tag)
        elif mask is not None:
            wanted = mask
        else:
            wanted = self.session.range_mask(first_block, block_count)
        if not self.session.mask_within(wanted, first_block, block_count):
            raise StoreError(StoreResultCode.DENIED, "Store block mask is out of range")
        closed = self.session.closure_fixpoint(
            [(table, count) for table, count in tables], wanted
        )
        if not self.session.mask_within(closed, first_block, block_count):
            raise StoreError(
                StoreResultCode.DENIED, "Store block closure escapes its representation"
            )
        payload, spans, touched = self._fetch_block_spans(part, closed)
        exact_verified = False
        if self._spans_tile_full(spans, total_length) and winner == 0:
            if self._hash(payload) != content_id:
                raise StoreIntegrityError("Store selective full read failed exactness")
            exact_verified = True
        return MaterializedView(
            payload=payload,
            representation_index=winner,
            fidelity=fidelity,
            satisfied=satisfied,
            mask=closed,
            stored_bytes_touched=touched,
            materialized_bytes=len(payload),
            inspect_bytes=resolve_bytes + inspect_bytes,
            exact_verified=exact_verified,
        )

    def materialize_plan(
        self, domain_schema: bytes, domain_identity: bytes, plan: bytes
    ) -> MaterializedView:
        """Execute a validated external materialization plan, or refuse it."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        plan = bytes(plan)
        if len(plan) != 128:
            raise StoreError(StoreResultCode.DENIED, "Store plan width is invalid")
        entry, _generation, commit_generation, resolve_bytes = self._resolve_entry(
            domain_schema, domain_identity
        )
        part = self._read_rep_tables(self._read_envelope_part(entry, commit_generation))
        envelope = part["envelope"]
        assert isinstance(envelope, bytes)
        representations = part["representations"]
        rep_fields = part["rep_fields"]
        tables = part["tables"]
        assert isinstance(representations, list) and isinstance(rep_fields, list)
        assert isinstance(tables, list)
        content_id = part["content_id"]
        assert isinstance(content_id, bytes)
        total_length = part["total_length"]
        assert isinstance(total_length, int)
        inspect_bytes = part["inspect_bytes"]
        assert isinstance(inspect_bytes, int)
        fields = self.session.plan_fields(plan)
        root = fields["root"]
        assert isinstance(root, bytes)
        winner: int | None = None
        for index, record_fields in enumerate(rep_fields):
            if record_fields["root"] == root:
                winner = index
                break
        if winner is None:
            raise StoreError(StoreResultCode.DENIED, "Store plan names no stored representation")
        code = self.session.plan_validate(plan, envelope, representations[winner])
        if code != 0:
            raise StoreError(StoreResultCode.DENIED, f"Store plan refused with code {code}")
        winner_fields = rep_fields[winner]
        fidelity = winner_fields["fidelity"]
        block_count = winner_fields["block_count"]
        first_block = winner_fields["first_block"]
        codec = winner_fields["codec"]
        exact = winner_fields["exact"]
        assert isinstance(fidelity, int) and isinstance(block_count, int)
        assert isinstance(first_block, int) and isinstance(codec, int)
        assert isinstance(exact, int)
        wanted = fields["mask"]
        assert isinstance(wanted, int)
        if block_count == 0:
            # MNCS plan admission already forced a zero mask for opaque
            # representations (code 8 otherwise); the whole blob follows.
            blob = (self.path / "reps" / f"{root.hex()}.payload").read_bytes()
            if self._verify_digest(blob) != root:
                raise StoreIntegrityError("Store representation blob integrity failure")
            if codec == RLE_CODEC_CODE:
                payload = self._decode_rle_blob(blob)
            elif codec == IDENTITY_CODEC_CODE:
                payload = blob
            else:
                raise StoreIntegrityError(f"Store cannot decode codec {codec}")
            exact_verified = False
            if exact:
                if self._hash(payload) != content_id:
                    raise StoreIntegrityError("Store exact representation failed exactness")
                exact_verified = True
            return MaterializedView(
                payload=payload,
                representation_index=winner,
                fidelity=fidelity,
                satisfied=True,
                mask=0,
                stored_bytes_touched=len(blob),
                materialized_bytes=len(payload),
                inspect_bytes=resolve_bytes + inspect_bytes,
                exact_verified=exact_verified,
            )
        if len(tables) > 1:
            # V1 stores at most one table per object; a spanning mask
            # needs composed multi-table validation (future work).
            raise StoreError(
                StoreResultCode.DENIED, "Store multi-table plans are not implemented"
            )
        for table, count in tables:
            table_code = self.session.plan_blocks_validate(plan, table, count)
            if table_code != 0:
                raise StoreError(
                    StoreResultCode.DENIED, f"Store plan block check refused with code {table_code}"
                )
        if not self.session.mask_within(wanted, first_block, block_count):
            raise StoreError(StoreResultCode.DENIED, "Store block mask is out of range")
        payload, spans, touched = self._fetch_block_spans(part, wanted)
        exact_verified = False
        if self._spans_tile_full(spans, total_length) and winner == 0:
            if self._hash(payload) != content_id:
                raise StoreIntegrityError("Store selective full read failed exactness")
            exact_verified = True
        return MaterializedView(
            payload=payload,
            representation_index=winner,
            fidelity=fidelity,
            satisfied=True,
            mask=wanted,
            stored_bytes_touched=touched,
            materialized_bytes=len(payload),
            inspect_bytes=resolve_bytes + inspect_bytes,
            exact_verified=exact_verified,
        )

    def get_bound_object(self, domain_schema: bytes, domain_identity: bytes) -> StoredObject:
        binding_id = self._binding_id(bytes(domain_schema), bytes(domain_identity))
        self._verified_projection()
        item = self._objects_by_binding.get(binding_id)
        if item is None:
            raise StoreError(StoreResultCode.DENIED, "Store object binding is not current")
        return item

    def _domain_binding_index(
        self, generation: int
    ) -> tuple[tuple[_Entry, bytes, bytes], ...]:
        if self._domain_index_generation == generation:
            return self._domain_index

        entries = sorted(
            self._read_generation(generation, verify_objects=False),
            key=lambda item: item.ordinal,
        )
        indexed: list[tuple[_Entry, bytes, bytes]] = []
        for entry in entries:
            schema, identity, logical, content, root, binding_generation = self._read_binding(
                entry.binding_id
            )
            if (
                logical != entry.logical_id
                or content != entry.content_id
                or root != entry.representation_root
            ):
                raise StoreIntegrityError("Store binding does not match generation entry")
            if binding_generation > generation:
                raise StoreIntegrityError("Store binding points into a future generation")
            indexed.append((entry, schema, identity))

        self._domain_index_generation = generation
        self._domain_index = tuple(indexed)
        return self._domain_index

    def find_bound_objects(
        self,
        domain_schema: bytes,
        domain_identity_prefix: bytes = b"",
    ) -> list[StoredObject]:
        """Return fully verified objects matching one generic domain prefix.

        The committed generation and every binding record are checked while
        locating candidates. A generation-scoped metadata index is reused
        across related queries. Payload trees are read and verified only for
        matching objects. Call :meth:`verify` or :meth:`current_objects` when
        a complete Store integrity pass is required.
        """

        selected_generation = self.current_generation
        schema_prefix = bytes(domain_schema)
        identity_prefix = bytes(domain_identity_prefix)
        if self._projection_generation == selected_generation:
            return [
                item
                for item in self._projection
                if item.domain_schema == schema_prefix
                and item.domain_identity.startswith(identity_prefix)
            ]

        found: list[StoredObject] = []
        for entry, schema, identity in self._domain_binding_index(selected_generation):
            if schema != schema_prefix or not identity.startswith(identity_prefix):
                continue
            found.append(
                self._read_entry(entry, selected_generation, verify_payload=True)
            )
        return found

    def object_for_binding(self, binding_id: bytes) -> StoredObject | None:
        """Return one current-generation object by its derived binding map."""

        self._verified_projection()
        return self._objects_by_binding.get(bytes(binding_id))

    def logical_id_for_domain(
        self, domain_schema: bytes, domain_identity: bytes
    ) -> bytes | None:
        """Resolve a current domain binding through the verified projection."""

        self._verified_projection()
        return self._logical_ids_by_domain.get((bytes(domain_schema), bytes(domain_identity)))

    def logical_ids_for_domain_identity(self, domain_identity: bytes) -> tuple[bytes, ...]:
        """Return bounded current logical matches when schema is intentionally omitted."""

        self._verified_projection()
        return self._logical_ids_by_identity.get(bytes(domain_identity), ())

    def current_objects(self) -> list[StoredObject]:
        return self._verified_projection()

    def objects_at(self, generation: int) -> list[StoredObject]:
        """Return the verified immutable object projection at one generation.

        Historical reads use the same integrity checks as current projections.
        Only committed generations at or below the observed head are readable;
        callers cannot use this API to inspect an unpublished future generation.
        """

        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise StoreError(StoreResultCode.DENIED, "generation must be a non-negative integer")
        if generation > self.current_generation:
            raise StoreError(StoreResultCode.DENIED, "generation is ahead of the committed Store head")
        return self._verified_projection(generation)

    def resident_status(self) -> dict[str, object]:
        """Expose retained generation cardinalities without copying authority."""

        return {
            "generation": self._projection_generation,
            "verified_object_projection_entries": len(self._projection),
            "binding_lookup_entries": len(self._objects_by_binding),
            "domain_identity_lookup_entries": len(self._logical_ids_by_domain),
            "identity_lookup_entries": len(self._logical_ids_by_identity),
            "commit_feed_generation": self._commit_feed_generation,
            "commit_feed_bytes": len(self._commit_feed) if self._commit_feed is not None else 0,
            "projection_capacity": None,
            "projection_policy": "complete verified current-generation authority projection",
            "limitation": "resident projection and lookup maps scale with the durable Store generation",
        }

    def object_metrics(self, domain_schema: bytes, domain_identity: bytes) -> dict[str, int | str]:
        """Return bounded geometry and overhead for one current object."""

        generation = self.current_generation
        binding_id = self._binding_id(bytes(domain_schema), bytes(domain_identity))
        entries = self._read_generation(generation, verify_objects=False)
        try:
            entry = next(item for item in entries if item.binding_id == binding_id)
        except StopIteration as exc:
            raise StoreError(StoreResultCode.DENIED, "Store object binding is not current") from exc
        manifest, total_length, chunk_count, depth, _content, tree_root, descriptor, _adaptive = self._read_manifest(
            entry
        )
        node_bytes = 0
        chunk_bytes = 0
        nodes: set[bytes] = set()
        chunks: set[bytes] = set()

        def visit(digest: bytes, level: int, start: int) -> None:
            nonlocal node_bytes, chunk_bytes
            if digest in nodes:
                return
            nodes.add(digest)
            raw = (self.path / "nodes" / f"{digest.hex()}.node").read_bytes()
            node_bytes += len(raw)
            children = self._read_node(
                digest,
                level=level,
                start=start,
                expected_count=min(FANOUT, (chunk_count - start + FANOUT**level - 1) // FANOUT**level),
            )
            if level == 0:
                for child in children:
                    if child not in chunks:
                        chunks.add(child)
                        chunk_bytes += (self.path / "chunks" / f"{child.hex()}.chunk").stat().st_size
                return
            span = FANOUT**level
            for index, child in enumerate(children):
                visit(child, level - 1, start + index * span)

        visit(tree_root, depth, 0)
        binding_bytes = self._binding_path(binding_id).stat().st_size
        structural_bytes = node_bytes + len(manifest) + len(descriptor) + binding_bytes + GENERATION_ENTRY.size
        return {
            "total_length": total_length,
            "chunk_count": chunk_count,
            "tree_depth": depth,
            "tree_node_count": len(nodes),
            "content_bytes": total_length,
            "stored_chunk_bytes": chunk_bytes,
            "structural_bytes": structural_bytes,
            "generation": generation,
        }

    def verify(self) -> dict[str, object]:
        generation = self.current_generation
        objects = self._verified_projection(generation)
        _entries, relations, provenance = self._read_generation_parts(generation)
        feed = self.commit_feed(generation)
        return {
            "ok": True,
            "generation": generation,
            "objects": len(objects),
            "relations": len(relations),
            "provenance_records": len(provenance),
            "commit_feed": feed.hex(),
            "recovery": self.recovery_result.value,
            "chunk_size": CHUNK_SIZE,
            "fanout": FANOUT,
            "artifact": self.provenance,
        }

    def recover(self) -> StoreResultCode:
        """Re-run durable recovery and return its typed outcome."""

        if self._closed:
            raise StoreError(StoreResultCode.DENIED, "Store is closed")
        self._invalidate_projection()
        self.recovery_result = self._recover()
        self._verified_projection(self.current_generation)
        return self.recovery_result

    def close(self) -> None:
        if self._closed:
            return
        if self._owns_session:
            self.session.close()
        self._closed = True

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> "EmbeddedStore":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()
