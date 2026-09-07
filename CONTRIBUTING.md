# Contributing to mncs-store

`mncs-store` is defining a foundational persistence boundary for MNCS. Small semantic ambiguities can spread across the entire project family, so changes should be explicit and evidence-driven.

## Before changing semantics

Read:

- `README.md`
- `docs/architecture.md`
- `docs/invariants.md`
- the relevant RFCs under `rfcs/`

If a change modifies a storage invariant or cross-repository contract, update or add an RFC in the same change.

## Preferred change shape

1. State the problem and affected invariant.
2. Add or update the RFC/design text.
3. Add executable tests when implementation exists.
4. Implement in MNCS language.
5. Record any language pressure discovered under `pressure/`.
6. Document compatibility, recovery, and security consequences.

## Compatibility

Do not preserve a poor canonical representation merely for compatibility with JSON, SQL, protobuf, filesystem layouts, or another external system. Build explicit adapters at the boundary. Conversely, do not invent proprietary encodings without specifying deterministic representation and migration/version behavior.

## Commits and pull requests

Keep commits coherent and avoid mixing unrelated ecosystem changes into storage work. Pull requests should identify which RFCs and invariants they affect and should distinguish architecture implemented now from future roadmap work.

## Security reports

See `SECURITY.md` for security-sensitive findings. Storage corruption, capability bypass, forged provenance, unsafe zero-copy views, and generation visibility violations should be treated as security-relevant until shown otherwise.
