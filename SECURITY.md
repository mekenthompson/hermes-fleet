# Security policy

Do not report suspected credentials or private deployment data in a public issue.

Use GitHub private vulnerability reporting when enabled for the repository. Report security issues through GitHub advisory reporting, not a public issue.

## Public product boundary

This repository must not contain credentials, private secret-source references, authentication state, sessions, memories, logs, real profile identities, private topology, or host administration material.

The source policy script is a fail-closed repository-shape check, not a substitute for a dedicated secret scanner. It rejects forbidden state paths, private topology, private-key markers, symlinks, and unexpected binaries. Dedicated tree and history secret scanning remains mandatory before publication.

Every publication candidate requires tree, history, build-artifact, image-layer, dependency, vulnerability, provenance, SBOM, and license review. These are future release gates in Phase 2; current CI does not claim to implement them. Passing source tests alone is not publication approval.
