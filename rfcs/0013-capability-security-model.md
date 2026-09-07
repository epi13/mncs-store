# RFC 0013 — Capability and security model

**Status:** Accepted for architecture

## Problem

A shared machine-native store will contain model state, agent evidence, compiler artifacts, user data, and potentially executable representations. Namespace-only access checks are insufficient for safe composition.

## Decision

Store operations are authorized by explicit capabilities/authorities supplied by the MNCS security/runtime environment. Capabilities may distinguish operations such as discover metadata, read payload, follow relationships, read provenance, create, update, pin, replicate, export, administer placement, or attest/verify.

The storage engine must not treat possession of an Object ID, Content ID, filesystem path, placement token, or index result as sufficient authority.

## Least authority

Queries and index-provider interactions receive only the visibility needed for the caller's capability. Derived indices containing protected metadata must preserve equivalent access boundaries or use partitioned/protected representations.

## Integrity versus authorization

A valid content hash proves content identity, not permission to read or execute it. A valid capability grants an operation, not proof that content is uncorrupted. Both checks remain necessary.

## Executable/model state

Loading executable artifacts or model-controlled structures from Store does not imply they are trusted for execution. Consumers must apply their own verification/attestation policy, which Store can preserve as provenance/evidence.

## Deferred

Concrete capability token formats, revocation mechanisms, tenant namespaces, encryption-at-rest/key management, and remote authentication are delegated to future security RFCs and MNCS-wide security primitives.
