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
from typing import Callable, Iterator

from .errors import CommitResult, StoreError, StoreIntegrityError, StoreResultCode
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

# 20-byte node header + 31 * 32-byte child identities = 1012 bytes.
NODE_HEADER = struct.Struct(">2sBBIIQ")
MANIFEST = struct.Struct(">2sBBQIQI32s32s32s32s12s")
GENERATION_HEADER = struct.Struct(">2sBBQIII")
GENERATION_ENTRY = struct.Struct(">12s32s32s32sQQ")
BINDING_HEADER = struct.Struct(">2sBBII")
JOURNAL = struct.Struct(">2sBBQQ32s")
RELATION_SIZE = 172
PROVENANCE_SIZE = 112


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
        failpoint: Failpoint | None = None,
        verify_on_open: bool = True,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self.session = session or StoreSession()
        self._owns_session = session is None
        self._failpoint = failpoint
        self._closed = False
        # A verified current-generation projection is a rebuildable read cache.
        # The generation head remains the authority; a publication invalidates
        # this cache and a changed head causes the next read to rebuild it.
        self._projection_generation: int | None = None
        self._projection: tuple[StoredObject, ...] = ()
        self._commit_feed_generation: int | None = None
        self._commit_feed: bytes | None = None
        for name in (
            "chunks",
            "nodes",
            "objects",
            "descriptors",
            "bindings",
            "generations",
            "staging",
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
    ) -> bytes:
        return MANIFEST.pack(
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

    def _build_content(
        self,
        *,
        payload: bytes,
        descriptor: bytes,
        binding_id: bytes,
        logical_id: bytes,
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
        self._commit_feed_generation = None
        self._commit_feed = None

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

    def _read_manifest(self, entry: _Entry) -> tuple[bytes, int, int, int, bytes, bytes, bytes]:
        raw = (self.path / "objects" / f"{entry.logical_id.hex()}.manifest").read_bytes()
        if len(raw) != MANIFEST.size:
            raise StoreIntegrityError("Store object manifest has an invalid size")
        values = MANIFEST.unpack(raw)
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
        if logical_id != entry.logical_id or binding_id != entry.binding_id:
            raise StoreIntegrityError("Store object manifest binding is inconsistent")
        if content_id != entry.content_id or self._hash(raw) != entry.representation_root:
            raise StoreIntegrityError("Store object manifest root identity mismatch")
        if chunk_size != CHUNK_SIZE or chunk_count < 1:
            raise StoreIntegrityError("Store object manifest geometry is invalid")
        descriptor = (self.path / "descriptors" / f"{descriptor_id.hex()}.desc").read_bytes()
        if self._hash(descriptor) != descriptor_id:
            raise StoreIntegrityError("Store descriptor integrity failure")
        return raw, total_length, chunk_count, depth, content_id, tree_root, descriptor

    def _read_node(self, digest: bytes, *, level: int, start: int, expected_count: int) -> list[bytes]:
        raw = (self.path / "nodes" / f"{digest.hex()}.node").read_bytes()
        if self._hash(raw) != digest or len(raw) < NODE_HEADER.size:
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
                    if self._hash(raw) != child:
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
        raw, total_length, chunk_count, depth, content_id, tree_root, descriptor = self._read_manifest(entry)
        if total_length != entry.total_length:
            raise StoreIntegrityError("Store generation length does not match its manifest")
        payload = self._read_tree(
            tree_root=tree_root,
            depth=depth,
            chunk_count=chunk_count,
            total_length=total_length,
        )
        if verify_payload and (len(payload) != total_length or self._hash(payload) != content_id):
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
            content_id, root, _descriptor_id, _chunks, _depth = self._build_content(
                payload=payload,
                descriptor=descriptor,
                binding_id=binding_id,
                logical_id=logical_id,
            )
            new_entry = _Entry(logical_id, binding_id, root, content_id, len(payload), new_generation)
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
            self._projection = (
                *verified_projection,
                StoredObject(
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
                ),
            )
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

    def get_bound_object(self, domain_schema: bytes, domain_identity: bytes) -> StoredObject:
        binding_id = self._binding_id(bytes(domain_schema), bytes(domain_identity))
        try:
            return next(item for item in self._verified_projection() if item.binding_id == binding_id)
        except StopIteration as exc:
            raise StoreError(StoreResultCode.DENIED, "Store object binding is not current") from exc

    def current_objects(self) -> list[StoredObject]:
        return self._verified_projection()

    def object_metrics(self, domain_schema: bytes, domain_identity: bytes) -> dict[str, int | str]:
        """Return bounded geometry and overhead for one current object."""

        generation = self.current_generation
        binding_id = self._binding_id(bytes(domain_schema), bytes(domain_identity))
        entries = self._read_generation(generation, verify_objects=False)
        try:
            entry = next(item for item in entries if item.binding_id == binding_id)
        except StopIteration as exc:
            raise StoreError(StoreResultCode.DENIED, "Store object binding is not current") from exc
        manifest, total_length, chunk_count, depth, _content, tree_root, descriptor = self._read_manifest(entry)
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

    def __enter__(self) -> "EmbeddedStore":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()
