"""Real-process Store proofs for the campaign acceptance surface."""

from __future__ import annotations

import argparse
import multiprocessing
import os
import tempfile
from pathlib import Path


def commit_worker(path_text: str, identity: str, output) -> None:
    from mncs_store import EmbeddedStore

    with EmbeddedStore(Path(path_text)) as store:
        result = store.put_bound_object(
            domain_schema=b"real-proof/1",
            domain_identity=identity.encode(),
            descriptor=b"real-proof-descriptor/1",
            payload=(identity.encode() * 40),
            expected_generation=0,
        )
        output.put(result.code.value)


def crash_worker(path_text: str, failpoint: str) -> None:
    from mncs_store import EmbeddedStore

    def inject(name: str) -> None:
        if name == failpoint:
            os._exit(137)

    with EmbeddedStore(Path(path_text), failpoint=inject) as store:
        store.put_bound_object(
            domain_schema=b"real-proof/1",
            domain_identity=b"crash",
            descriptor=b"real-proof-descriptor/1",
            payload=b"crash-proof" * 40,
            expected_generation=0,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-failpoint", default="after_generation_publication")
    args = parser.parse_args()
    context = multiprocessing.get_context("spawn")
    with tempfile.TemporaryDirectory(prefix="mncs-store-real-proof-") as directory:
        root = Path(directory)
        from mncs_store import EmbeddedStore

        with EmbeddedStore(root / "race"):
            pass
        queue = context.Queue()
        workers = [
            context.Process(target=commit_worker, args=(str(root / "race"), f"writer-{index}", queue))
            for index in range(2)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(180)
            if worker.exitcode != 0:
                raise SystemExit(f"race worker exited {worker.exitcode}")
        race_results = sorted(queue.get(timeout=5) for _ in workers)
        if race_results != ["COMMITTED", "STALE_GENERATION"]:
            raise SystemExit(f"unexpected race results: {race_results}")

        with EmbeddedStore(root / "crash"):
            pass
        crashed = context.Process(
            target=crash_worker,
            args=(str(root / "crash"), args.crash_failpoint),
        )
        crashed.start()
        crashed.join(180)
        if crashed.exitcode != 137:
            raise SystemExit(f"crash worker exited {crashed.exitcode}, expected 137")
        with EmbeddedStore(root / "crash") as recovered:
            recovery = recovered.recovery_result.value
            recovered.verify()
            print({"race": race_results, "crash_failpoint": args.crash_failpoint, "recovery": recovery})


if __name__ == "__main__":
    main()
