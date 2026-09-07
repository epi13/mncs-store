# Source layout

The architecture bootstrap intentionally contains no pretend storage engine.

When implementation begins, `src/` should contain MNCS-language modules whose boundaries follow the RFCs rather than a conventional SQL/database layering copied wholesale from another system.

Expected early modules/concepts include object identity, representation descriptors, content/chunk addressing, manifests, local placement, generation commit/recovery, typed views, references/provenance, and provider interfaces.

The first milestone is the single-node typed persist → close → reopen → verify → typed-read proof described in `ROADMAP.md`.
