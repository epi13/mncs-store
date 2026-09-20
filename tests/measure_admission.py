"""Measure Store semantic admission without running the full lifecycle suite.

The result separates process/compile admission from retained-session open and
records the compiler's optional phase timings.  It is intentionally a script,
not a benchmark assertion: compiler cost is evidence for architecture work,
not a correctness threshold.
"""

from __future__ import annotations

import argparse
import json
import os
import time

from retained_session import MNCS_BIN, RetainedSession


BASE_SOURCES = [
    "src/store/identity.mncs",
    "src/store/descriptor.mncs",
    "src/store/chunk.mncs",
    "src/store/manifest.mncs",
    "src/store/generation.mncs",
    "src/store/recovery.mncs",
    "src/store/publication.mncs",
    "src/store/relationship.mncs",
    "src/store/relationship/v2.mncs",
    "src/store/provenance.mncs",
    "src/store/commit_feed.mncs",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-semantic-state", action="store_true")
    parser.add_argument("--application", action="store_true")
    parser.add_argument("--label", default="stable")
    args = parser.parse_args()
    sources = ["src/store/application.mncs"] if args.application else list(BASE_SOURCES)
    if args.include_semantic_state and not args.application:
        sources.append("src/store/semantic_state.mncs")

    started = time.perf_counter()
    sessions = []
    try:
        for source in sources:
            sessions.append(RetainedSession(source))
        rows = [
            {
                "source": session.source,
                "compile_seconds": session.compile_seconds,
                "session_open_seconds": session.session_open_seconds,
                "artifact_bytes": len(session._artifact),
                "compiler_timings_ms": session.compiler_timings,
            }
            for session in sessions
        ]
        print(
            json.dumps(
                {
                    "schema": "mncs-store.admission-measurement/1",
                    "label": args.label,
                    "mncs_bin": str(MNCS_BIN),
                    "source_count": len(sessions),
                    "session_count": len(sessions),
                    "total_wall_seconds": time.perf_counter() - started,
                    "compile_seconds": sum(row["compile_seconds"] for row in rows),
                    "session_open_seconds": sum(row["session_open_seconds"] for row in rows),
                    "artifact_bytes": sum(row["artifact_bytes"] for row in rows),
                    "sources": rows,
                },
                indent=2,
            )
        )
    finally:
        for session in sessions:
            session.close()


if __name__ == "__main__":
    main()
