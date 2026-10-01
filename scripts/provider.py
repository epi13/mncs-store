#!/usr/bin/env python3
"""Bounded provider transport. All Store decisions execute existing MNCS APIs."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import uuid
from dataclasses import asdict, is_dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from mncs_store import (
    BlockInput,
    EmbeddedStore,
    RepresentationInput,
    StoreError,
    StoreSession,
)

OPERATIONS = (
    "describe",
    "status",
    "admit",
    "inspect-envelope",
    "list-representations",
    "select",
    "add-representation",
    "materialize",
    "materialize-plan",
    "read-blocks",
    "read-synopsis",
)
MAX_REQUEST = 65536
MAX_PAYLOAD = 32 * 1024 * 1024


class RequestError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def absolute(value):
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise RequestError(
            "PATH_NOT_ABSOLUTE", "transport paths must be explicit absolute paths"
        )
    return Path(value).resolve()


def bounded_file(path, limit):
    try:
        with path.open("rb") as stream:
            value = stream.read(limit + 1)
    except OSError as error:
        raise RequestError("TRANSPORT_FILE_UNAVAILABLE", str(error)) from error
    if len(value) > limit:
        raise RequestError("TRANSPORT_LIMIT", f"input exceeds {limit} bytes")
    return value


def bytes_input(value):
    if not isinstance(value, dict) or len(value) != 1:
        raise RequestError(
            "MALFORMED_BYTES", "bytes require exactly one of utf8, hex, base64, file"
        )
    key, data = next(iter(value.items()))
    if not isinstance(data, str):
        raise RequestError("MALFORMED_BYTES", "byte transport value must be a string")
    if key == "utf8":
        raw = data.encode()
    elif key == "hex":
        raw = bytes.fromhex(data)
    elif key == "base64":
        raw = base64.b64decode(data, validate=True)
    elif key == "file":
        return bounded_file(absolute(data), MAX_PAYLOAD)
    else:
        raise RequestError("MALFORMED_BYTES", "unknown byte transport encoding")
    if len(raw) > MAX_PAYLOAD:
        raise RequestError(
            "TRANSPORT_LIMIT", "payload exceeds provider transport bound"
        )
    return raw


def integer(value):
    if type(value) is not int or not 0 <= value < 2**64:
        raise RequestError("MALFORMED_INTEGER", "expected a u64 integer")
    return value


def encode_intent(session, value):
    if value is None:
        return session.default_intent()
    if not isinstance(value, dict):
        raise RequestError("MALFORMED_INTENT", "intent must be an object")
    names = {
        "fidelity",
        "latency",
        "compute",
        "memory",
        "transfer",
        "frequency",
        "lifetime",
        "locality",
    }
    if set(value) - names:
        raise RequestError("MALFORMED_INTENT", "unknown intent fields")
    fields = {key: integer(item) for key, item in value.items()}
    fields.setdefault("fidelity", 5)
    if fields["fidelity"] > 5:
        raise RequestError("MALFORMED_INTENT", "required fidelity must be 0..5")
    return session.encode_intent(**fields)


def selected_runtime():
    # Environment supplies these selected bindings. Refuse ambient artifacts,
    # precompiled Store artifact overrides, PYTHONPATH, and compiler aliases.
    required = ("MNCS_LANGUAGE_ROOT", "MNCS_BIN", "MNCS_EMBED_LIB", "MNCS_STORE_ROOT")
    selected = {key: absolute(os.environ.get(key)) for key in required}
    if selected["MNCS_STORE_ROOT"] != ROOT:
        raise RequestError(
            "SELECTED_PROVIDER_MISMATCH", "Store binding differs from invoked checkout"
        )
    for key in ("MNCS_BIN", "MNCS_EMBED_LIB"):
        if (
            not selected[key].is_relative_to(selected["MNCS_LANGUAGE_ROOT"])
            or not selected[key].is_file()
        ):
            raise RequestError(
                "SELECTED_TOOLCHAIN_UNAVAILABLE",
                f"{key} is not a file under selected Language",
            )
    if selected["MNCS_EMBED_LIB"].parent != selected["MNCS_BIN"].parent:
        raise RequestError(
            "SELECTED_TOOLCHAIN_MISMATCH",
            "compiler and embed must be selected from the same build directory",
        )
    os.environ.pop("MNCS_STORE_OUTPUT_DIR", None)
    os.environ.pop("MNCS_STORE_ARTIFACT", None)
    os.environ["MNCS_CLI"] = str(selected["MNCS_BIN"])
    return {key: str(value) for key, value in selected.items()}


def project(value):
    if is_dataclass(value):
        result = project(asdict(value))
        if hasattr(value, "constraints_satisfied"):
            result["constraints_satisfied"] = value.constraints_satisfied
        return result
    if isinstance(value, bytes):
        return {"hex": value.hex()}
    if isinstance(value, dict):
        return {key: project(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [project(item) for item in value]
    return value


def payload_result(payload):
    result = {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    if len(payload) <= 16384:
        result["base64"] = base64.b64encode(payload).decode()
    else:
        directory = absolute(
            os.environ.get("MNCS_STORE_OUTPUT_DIR")
            or os.environ.get("MNCS_ENV_SESSION_ARTIFACT_DIR")
        )
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = directory / f"store-{uuid.uuid4().hex}.payload"
        with path.open("xb") as stream:
            stream.write(payload)
        result["file"] = str(path)
    return result


def operate(operation, request, session):
    root = absolute(request["store"])
    with EmbeddedStore(
        root,
        session=session,
        read_only=operation not in ("admit", "add-representation"),
    ) as store:
        schema = bytes_input(request["domain_schema"])
        identity = bytes_input(request["domain_identity"])
        if operation == "admit":
            reps = [
                RepresentationInput(
                    integer(rep["fidelity"]), rep["codec"], bytes_input(rep["payload"])
                )
                for rep in request.get("representations", [])
            ]
            blocks = [
                BlockInput(**{key: integer(value) for key, value in block.items()})
                for block in request.get("blocks", [])
            ]
            result = store.put_bound_object(
                domain_schema=schema,
                domain_identity=identity,
                descriptor=bytes_input(request["descriptor"]),
                payload=bytes_input(request["payload"]),
                expected_generation=integer(request["expected_generation"]),
                synopsis=bytes_input(request["synopsis"])
                if "synopsis" in request
                else None,
                representations=reps,
                blocks=blocks,
            )
            return project(result)
        if operation == "add-representation":
            rep = request["representation"]
            return project(
                store.add_representation(
                    schema,
                    identity,
                    RepresentationInput(
                        integer(rep["fidelity"]),
                        rep["codec"],
                        bytes_input(rep["payload"]),
                    ),
                    expected_generation=integer(request["expected_generation"]),
                )
            )
        if operation == "inspect-envelope":
            return project(store.get_envelope(schema, identity))
        if operation == "list-representations":
            return project(store.get_representations(schema, identity))
        if operation == "read-synopsis":
            return payload_result(store.read_synopsis(schema, identity))
        intent = encode_intent(session, request.get("intent"))
        if operation == "select":
            return project(store.select_representation(schema, identity, intent))
        if operation == "read-blocks":
            value = store.read_blocks(schema, identity, integer(request["mask"]))
        elif operation == "materialize":
            value = store.materialize(
                schema,
                identity,
                intent=intent,
                **{
                    key: integer(request[key])
                    for key in ("mask", "tag")
                    if key in request
                },
            )
        elif operation == "materialize-plan":
            plan = request["plan"]
            if not isinstance(plan, dict):
                raise RequestError("MALFORMED_PLAN", "plan must be an object")
            raw = session.encode_plan(
                fidelity=integer(plan["fidelity"]),
                root=bytes_input(plan["root"]),
                mask=integer(plan["mask"]),
                cap=integer(plan.get("cap", 0)),
                authority=bytes_input(plan.get("authority", {"hex": "00" * 32})),
            )
            value = store.materialize_plan(schema, identity, raw)
        else:
            raise RequestError("UNSUPPORTED_OPERATION", operation)
        # Payload stays out of metadata JSON; large bytes use governed session artifacts.
        return {
            **{
                key: project(item)
                for key, item in asdict(value).items()
                if key != "payload"
            },
            "constraints_satisfied": value.constraints_satisfied,
            "payload": payload_result(value.payload),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=OPERATIONS)
    parser.add_argument("--request")
    parser.add_argument(
        "--artifact-dir",
        help="absolute directory for large result bytes outside Environment",
    )
    args = parser.parse_args()
    response = {
        "schema_version": "mncs.store.provider-result/1",
        "operation": args.operation,
    }
    try:
        selected = selected_runtime()
        if args.artifact_dir:
            os.environ["MNCS_STORE_OUTPUT_DIR"] = str(absolute(args.artifact_dir))
        response["selected"] = selected
        if args.operation == "describe":
            response.update(
                status="ok",
                result={
                    "contract": "adaptive-representations/1",
                    "operations": list(OPERATIONS),
                    "request": "absolute --request JSON file; store absolute path; domain_schema/domain_identity byte values",
                    "byte_transport": ["utf8", "hex", "base64", "file"],
                    "codec_inputs": ["identity", "rle"],
                    "fidelity_range": [0, 5],
                    "intent_fields": [
                        "fidelity",
                        "latency",
                        "compute",
                        "memory",
                        "transfer",
                        "frequency",
                        "lifetime",
                        "locality",
                    ],
                    "satisfied": "required fidelity only",
                    "latency_permitted": "separate MNCS verdict",
                    "constraints_satisfied": "fidelity and latency",
                    "plan_authority": "proposal provenance, never permission",
                    "max_request_bytes": MAX_REQUEST,
                    "max_payload_bytes": MAX_PAYLOAD,
                    "request_common": {
                        "store": "absolute Store directory",
                        "domain_schema": "bytes",
                        "domain_identity": "bytes",
                    },
                    "request_fields": {
                        "admit": [
                            "descriptor:bytes",
                            "payload:bytes",
                            "expected_generation:u64",
                            "synopsis?:bytes",
                            "representations?:[{fidelity,codec,payload}]",
                            "blocks?:[{index,tag,start,length}]",
                        ],
                        "add-representation": [
                            "expected_generation:u64",
                            "representation:{fidelity,codec,payload}",
                        ],
                        "inspect-envelope": [],
                        "list-representations": [],
                        "read-synopsis": [],
                        "select": ["intent?:object"],
                        "read-blocks": ["mask:u64"],
                        "materialize": ["intent?:object", "mask?:u64 OR tag?:u64"],
                        "materialize-plan": [
                            "plan:{fidelity,root:bytes32,mask,cap?,authority?:bytes32}"
                        ],
                    },
                },
            )
        else:
            with StoreSession() as session:
                if args.operation == "status":
                    intent = session.default_intent()
                    session.validate_intent(intent)
                    response.update(
                        status="ok",
                        state="ready",
                        result={
                            "artifact": session.artifact_sha256,
                            "backend": session.backend,
                            "verified": "selected retained Store intent capability",
                        },
                    )
                else:
                    if args.request is None:
                        raise RequestError(
                            "REQUEST_REQUIRED", "operation requires --request"
                        )
                    request = json.loads(
                        bounded_file(absolute(args.request), MAX_REQUEST)
                    )
                    if not isinstance(request, dict):
                        raise RequestError(
                            "MALFORMED_REQUEST", "request must be an object"
                        )
                    response.update(
                        status="ok", result=operate(args.operation, request, session)
                    )
                response["metrics"] = {
                    "retained_calls": session.call_count,
                    "retained_batches": session.batch_count,
                    "semantic_seconds": session.semantic_seconds,
                    "request_transport_bytes": session.request_transport_bytes,
                    "response_transport_bytes": session.response_transport_bytes,
                }
        code = 0
    except (
        StoreError,
        RequestError,
        ValueError,
        TypeError,
        KeyError,
        OSError,
    ) as error:
        response.update(
            status="error",
            diagnostic={
                "code": str(
                    getattr(
                        error,
                        "code",
                        "STORE_IO_UNAVAILABLE"
                        if isinstance(error, OSError)
                        else "MALFORMED_REQUEST",
                    )
                ),
                "message": str(error),
                "detail_code": getattr(error, "detail_code", None),
            },
        )
        code = 2
    print(json.dumps(response, sort_keys=True))
    return code


if __name__ == "__main__":
    sys.exit(main())
