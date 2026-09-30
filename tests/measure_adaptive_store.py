"""Measure adaptive Store representations against whole-object reads.

This is intentionally a bounded measurement tool rather than a pytest
case; it needs a real retained MNCS session and multi-hundred-KiB
payloads. Every number reported is observed in this run: stored bytes,
inspected bytes, materialized bytes, exactness verdicts, and wall time
where reliably measurable.

The workload is one logical object of N semantic blocks where the task
needs a small subset: the comparison that matters is decode
amplification (bytes materialized / bytes actually needed), not raw
compression ratio.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from pathlib import Path

from mncs_store import BlockInput, EmbeddedStore, RepresentationInput, StoreSession


def workload(block_kb: int, blocks: int) -> tuple[bytes, list[bytes]]:
    parts = []
    for i in range(blocks):
        size = block_kb * 1024
        if i % 2 == 0:
            # Compressible: long runs with sparse markers.
            part = bytearray(bytes([i % 251]) * size)
            for j in range(0, size, 1024):
                part[j] = (i + j) % 251
            parts.append(bytes(part))
        else:
            parts.append(bytes((i * 37 + j * 11) % 251 for j in range(size)))
    return b"".join(parts), parts


def measure(root: Path, session: StoreSession, block_kb: int, blocks: int) -> dict[str, object]:
    payload, parts = workload(block_kb, blocks)
    state = root / f"blocks-{blocks}x{block_kb}k"
    row: dict[str, object] = {
        "blocks": blocks,
        "block_kb": block_kb,
        "payload_bytes": len(payload),
    }
    with EmbeddedStore(state, session=session) as store:
        block_inputs = [
            BlockInput(index=i, tag=100 + (i % 3), start=i * block_kb * 1024,
                       length=block_kb * 1024)
            for i in range(blocks)
        ]
        synopsis = f"adaptive-measurement:{blocks}x{block_kb}KiB tag-mod-3".encode()
        put_started = time.perf_counter()
        committed = store.put_bound_object(
            domain_schema=b"mncs-store.adaptive-measurement/1",
            domain_identity=f"{blocks}x{block_kb}".encode(),
            descriptor=f"measurement/{blocks}x{block_kb}\n".encode(),
            payload=payload,
            expected_generation=store.current_generation,
            synopsis=synopsis,
            representations=[RepresentationInput(fidelity=5, codec="rle", payload=payload)],
            blocks=block_inputs,
        )
        row["put_seconds"] = time.perf_counter() - put_started
        row["commit"] = committed.code.value
        schema, identity = b"mncs-store.adaptive-measurement/1", f"{blocks}x{block_kb}".encode()

        # Traditional whole-object materialization.
        started = time.perf_counter()
        whole = store.get_bound_object(schema, identity)
        row["whole_seconds"] = time.perf_counter() - started
        assert whole.payload == payload
        metrics = store.object_metrics(schema, identity)
        assert isinstance(metrics["stored_chunk_bytes"], int)
        assert isinstance(metrics["structural_bytes"], int)
        whole_stored = metrics["stored_chunk_bytes"] + metrics["structural_bytes"]
        row["whole_stored_touched"] = whole_stored
        row["whole_materialized"] = len(whole.payload)

        # Semantic inspection without payload expansion.
        started = time.perf_counter()
        env = store.get_envelope(schema, identity)
        row["inspect_seconds"] = time.perf_counter() - started
        row["inspect_bytes"] = env.stored_bytes_touched
        row["envelope_rep_count"] = env.fields["rep_count"]
        row["envelope_block_count"] = env.fields["block_count"]
        reps = store.get_representations(schema, identity)
        row["representations"] = [
            {
                "fidelity": r.fields["fidelity"],
                "codec": r.fields["codec"],
                "stored": r.fields["stored"],
                "plain": r.fields["plain"],
                "decode_class": r.fields["decode_class"],
                "exact": r.fields["exact"],
            }
            for r in reps
        ]

        # Selective materialization: the task needs block 2 only.
        needed = len(parts[2])
        started = time.perf_counter()
        selective = store.read_blocks(schema, identity, 1 << 2)
        row["selective_seconds"] = time.perf_counter() - started
        assert selective.payload == parts[2]
        row["selective_stored_touched"] = selective.stored_bytes_touched
        row["selective_materialized"] = selective.materialized_bytes
        row["selective_inspect"] = selective.inspect_bytes
        row["selective_exact"] = selective.exact_verified
        row["decode_amplification_traditional"] = len(whole.payload) / needed
        row["decode_amplification_selective"] = selective.materialized_bytes / needed

        # Tag-selected subset: blocks with tag 101.
        tagged = store.materialize(
            schema, identity, intent=session.encode_intent(2), tag=101)
        want = b"".join(part for i, part in enumerate(parts) if 100 + (i % 3) == 101)
        assert tagged.payload == want
        row["tag_materialized"] = tagged.materialized_bytes
        row["tag_stored_touched"] = tagged.stored_bytes_touched

        # Coded exact representation under a transfer-heavy intent.
        started = time.perf_counter()
        coded = store.materialize(
            schema, identity, intent=session.encode_intent(5, transfer=1000))
        row["coded_seconds"] = time.perf_counter() - started
        assert coded.payload == payload
        row["coded_representation"] = coded.representation_index
        row["coded_stored_touched"] = coded.stored_bytes_touched
        row["coded_materialized"] = coded.materialized_bytes
        row["coded_exact_verified"] = coded.exact_verified
        row["storage_ratio_rle"] = coded.stored_bytes_touched / len(payload)

        # Synopsis without payload.
        syn = store.read_synopsis(schema, identity)
        assert syn == synopsis
        row["synopsis_bytes"] = len(syn)

        # Machine-navigability cost: index bytes (envelope, tables)
        # are overhead; representation blobs are alternative payloads
        # with their own storage ratios, reported separately above.
        index_bytes = 0
        for directory in ("envelopes", "representations", "blocks"):
            for path in (state / directory).iterdir():
                index_bytes += path.stat().st_size
        blob_bytes = sum(
            path.stat().st_size for path in (state / "reps").iterdir()
        )
        row["index_bytes"] = index_bytes
        row["rep_blob_bytes"] = blob_bytes
        row["semantic_overhead_ratio"] = index_bytes / len(payload)
    return row


def canonical_demo(session: StoreSession) -> dict[str, object]:
    """Show canonicalization removing representational entropy."""
    # Structured rows: four groups of identical rows, so row order
    # decides whether equal bytes merge into long runs (canonical) or
    # fragment into short ones (shuffled) under run-length coding.
    rows = [bytes([i // 4]) * 16 for i in range(16)]
    ordered = b"".join(rows)
    shuffled = b"".join(rows[i] for i in (5, 1, 9, 0, 13, 3, 11, 7, 15, 2, 8, 4, 14, 6, 10, 12))
    assert session.canonical_equal(ordered, shuffled, 16, 16)
    canonical = session.canonical_sort(ordered, 16, 16)[:256]
    assert session.rows_sorted(canonical, 16, 16) == 0
    canonical_id = hashlib.sha256(canonical).hexdigest()
    shuffled_canonical = session.canonical_sort(shuffled, 16, 16)[:256]
    assert shuffled_canonical == canonical
    # Byte compression after canonicalization vs before it.
    rle_plain = session.rle_encode_windows(shuffled)
    rle_canon = session.rle_encode_windows(canonical)
    return {
        "table_bytes": len(ordered),
        "canonical_id": canonical_id,
        "rle_shuffled_bytes": sum(len(w) for w in rle_plain),
        "rle_canonical_bytes": sum(len(w) for w in rle_canon),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blocks", type=int, default=8)
    parser.add_argument("--block-kb", type=int, default=64)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="mncs-store-adaptive-") as directory:
        root = Path(directory)
        with StoreSession() as session:
            row = measure(root, session, args.block_kb, args.blocks)
            demo = canonical_demo(session)
            print(
                json.dumps(
                    {
                        "schema": "mncs-store.adaptive-measurement/1",
                        "chunk_size": 64 * 1024,
                        "sha256_oracle": "independent hashlib comparison; Store identity uses mncs.std.sha256.v1",
                        "artifact": session.artifact_sha256,
                        "backend": session.backend,
                        "semantic_seconds": session.semantic_seconds,
                        "store_calls": session.call_count,
                        "row": row,
                        "canonical_demo": demo,
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    main()
