# Source layout

MNCS-language modules define Store's semantic bytes and decisions. The
supported embedded boundary in `../python/mncs_store/` supplies retained
session transport and local filesystem durability; it does not choose Store
outcomes. The old test drivers remain differential/reference tools, not a
consumer API. The current content path uses bounded chunks and a manifest/tree
root rather than increasing one MNCS value until it contains a whole object.

The following frozen Phase-1 modules are reference-only and are not imported
by `store.application.v1` or the supported embedded boundary:

| Module | RFC | Owns |
|---|---|---|
| `store/descriptor.mncs` (`store.descriptor.v1`) | 0003 | frozen descriptor encoding/validation used by the differential oracle |
| `store/chunk.mncs` (`store.chunk.v1`) | 0004 | frozen fixed-width chunk codecs used by the differential oracle |
| `store/manifest.mncs` (`store.manifest.v1`) | 0004/0005 | frozen manifest vocabulary used by the differential oracle |
| `store/read_verify.mncs` (`store.read_verify.v1`) | 0015 | frozen in-language chunk-file verification used by the differential oracle |

The current semantic modules are:

| Module | RFC | Owns |
|---|---|---|
| `store/identity.mncs` (`store.identity.v1`) | 0002 | current nominal logical/content/chunk/root identity wrappers used by generation and typed Store paths |
| `store/content.mncs` (`store.content.v1`) | current Store path | bounded streaming `DigestState` adapter over `mncs.std.sha256.v1`, including granted filesystem windows |
| `store/generation.mncs` (`store.generation.v1`) | 0005 | Generation headers; CAS decisions + conflict tokens; snapshot tokens + binding; commit state machine; reclamation membership scans |
| `store/recovery.mncs` (`store.recovery.v1`) | 0015 | current native old/new recovery selection and historical triage vectors |
| `store/relationship.mncs` (`store.relationship`) | current relation path | one generic 172-byte relation representation: type identity, endpoints, generation, provenance, ordinal, optional typed metadata |

Profile: current Store semantic modules use Source 0.18 where granted
filesystem effects are required; frozen compatibility modules retain their
declared source profiles. The content module imports only the existing
`mncs.std.sha256.v1` streaming implementation; Store does not define another
hash system.

Value classes: fixed compatibility widths remain tested, while the supported
object path uses 64 KiB content chunks, 31-way structural nodes, a bounded
manifest, and a generation-bound representation root. Individual semantic
operations stay bounded even when the object is larger than one MNCS value.
