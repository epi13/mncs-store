"""Measure the supported embedded content-tree path.

This is intentionally a bounded measurement tool rather than a pytest case;
the 1 MiB and 4 MiB rows are scheduled when the admitted Store artifact and
backend budget make the run practical.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from pathlib import Path

from mncs_store import EmbeddedStore, StoreError, StoreSession


def payload(size: int) -> bytes:
    return bytes((index * 17 + 3) % 256 for index in range(size))


def measure(size: int, root: Path, session: StoreSession) -> dict[str, object]:
    data = payload(size)
    state = root / f"size-{size}"
    started = time.perf_counter()
    try:
        with EmbeddedStore(state, session=session) as store:
            before_calls = session.call_count
            committed = store.put_bound_object(
                domain_schema=b"mncs-store.measurement/1",
                domain_identity=str(size).encode(),
                descriptor=f"measurement/{size}\n".encode(),
                payload=data,
                expected_generation=store.current_generation,
            )
            write_seconds = time.perf_counter() - started
            metrics = store.object_metrics(b"mncs-store.measurement/1", str(size).encode())
            content_id = store.current_objects()[0].content_id
            assert content_id == hashlib.sha256(data).digest()
            write_calls = session.call_count - before_calls
    except StoreError as error:
        return {
            "size": size,
            "status": "blocked",
            "code": error.code.value,
            "error": error.message,
            "elapsed_seconds": time.perf_counter() - started,
            "store_calls": session.call_count,
        }
    verify_started = time.perf_counter()
    with EmbeddedStore(state, session=session) as reopened:
        reopened.verify()
        verify_seconds = time.perf_counter() - verify_started
        reopen_started = time.perf_counter()
        reopened.current_objects()
        reopen_seconds = time.perf_counter() - reopen_started
    return {
        "size": size,
        "status": "committed",
        "generation": committed.generation,
        "chunk_count": metrics["chunk_count"],
        "tree_depth": metrics["tree_depth"],
        "tree_node_count": metrics["tree_node_count"],
        "stored_chunk_bytes": metrics["stored_chunk_bytes"],
        "structural_bytes": metrics["structural_bytes"],
        "store_calls": write_calls,
        "write_seconds": write_seconds,
        "verify_seconds": verify_seconds,
        "reopen_seconds": reopen_seconds,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sizes",
        nargs="*",
        type=int,
        default=[0, 1024, 32 * 1024, 128 * 1024, 1024 * 1024, 4 * 1024 * 1024],
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="mncs-store-measure-") as directory:
        root = Path(directory)
        with StoreSession() as session:
            rows = [measure(size, root, session) for size in args.sizes]
            print(
                json.dumps(
                    {
                        "schema": "mncs-store.embedded-scale-measurement/1",
                        "chunk_size": 64 * 1024,
                        "fanout": 31,
                        "sha256_oracle": "independent hashlib comparison; Store identity uses mncs.std.sha256.v1",
                        "artifact": session.artifact_sha256,
                        "backend": session.backend,
                        "rows": rows,
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    main()
