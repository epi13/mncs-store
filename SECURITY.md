# Security

`mncs-store` is expected to become a trust boundary for persistent MNCS state. Storage bugs can become code-execution, confidentiality, integrity, provenance, or availability failures in consumers.

## Security-sensitive areas

Treat the following as security-relevant:

- accepting content whose hash/integrity evidence does not match;
- exposing a partially committed generation as durable or visible;
- capability or namespace bypass;
- unsafe zero-copy/mmap lifetimes, bounds, alignment, or mutability;
- forged or silently discarded provenance;
- malicious representation descriptors that cause overflow or out-of-bounds access;
- decompression/decoding bombs at import boundaries;
- replica rollback or generation equivocation;
- unbounded graph/reference traversal from hostile data;
- path, device, or placement metadata used as an authority without validation.

## Design stance

Canonical content should be independently verifiable, and recovery must fail closed when integrity cannot be established. Derived indices and caches must never be treated as authoritative copies of canonical objects. External formats and remote peers are untrusted inputs.

When implementation begins, fuzzing/property testing should target parsers, representation descriptors, manifests, recovery logs, reference traversal, and cross-version migration.

For a private vulnerability report, use GitHub's repository security reporting facilities when enabled rather than opening a public issue containing exploit details.
