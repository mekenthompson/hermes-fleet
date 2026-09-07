# Fleet image release

The public Fleet image is a minimal child of the **exact Agent digest** recorded in `release/agent-image-manifest.json`. Mutable tags are rejected before Docker runs.

## Transaction

Pull requests and manual validation runs build one `linux/amd64` candidate from the exact Fleet source revision. Preflight verifies the pinned Agent handoff, inherited Agent provenance marker and installed Hermes version, Fleet revision and image identity, inherited runtime user/entrypoint/command, non-root `hermes` workload execution, 1Password CLI 2.39.0 from its exact official image digest, Claude Code 2.1.261, the Claude ACP adapter 0.75.1, the attributed subscription-only Claude provider plugin, absence of a Docker socket, full SPDX evidence, bounded package-level SPDX, official SPDX 2.3 schema validity through a fully hashed validator lock, and the critical-vulnerability gate.

A `publish=true` dispatch from `main` exports that candidate. The protected `fleet-image-publish` job first requires a successful `CI` push run for the **exact selected main SHA** (not a green run from another commit), then downloads, loads, and re-verifies the same candidate before pushing it **without rebuilding**. Publication attaches SLSA build provenance and the bounded package-level SPDX document to the resulting digest. The full SPDX and scanner report remain pre-publication evidence.

The final authenticated artifact is a House handoff manifest containing only public Agent and Fleet source/image identities. It is not deployment configuration and contains no topology, profile identity, host path, policy, or secret reference.

## Boundaries

The public Fleet source repository is `https://github.com/mekenthompson/hermes-fleet`. The existing image package remains `ghcr.io/mekenthompson/hermes-fleet-public`; repository renaming must not invalidate published immutable image references.

This workflow performs **no production deployment**. It does not modify profile state, Docker Compose inputs, networks, secrets, or running containers. The public image inherits the Agent image's user, entrypoint, and command. The Agent image records `User=root` and drops to the non-root `hermes` account through its entrypoint. Fleet also copies the `op` executable from the immutable official image `docker.io/1password/op@sha256:d7d12b409ec699c9fa139d3bdfc80671f744380d39db8c539d9dc6e7e553d3c1`, verifies version `2.39.0` during build and release, and records that source in the image provenance marker. Claude Code is installed from the committed lockfile at exact version `2.1.261`; `npm ci` disables lifecycle scripts, then the Dockerfile explicitly invokes only Anthropic's locked `install.cjs` to select the native optional package. The same lock pins `@agentclientprotocol/claude-agent-acp` to `0.75.1` with integrity `sha512-Un6I4BRkhpCFS3I7kr5C/lkAm8Nc3VuGZU2YQ3xIpJAIxV94iWO0Q2CH2QABxMERpONRu4Le6XC9V+5PImQZ2A==`. Hermes launches that adapter through the supported external-process provider seam using the image-owned `hermes-claude-acp-subscription` wrapper; the `claude` executable alone is not the ACP adapter. The provider plugin is adapted from `mvdbastos/hermes-acp-agents` revision `0526610a3945cc376ac517b63ca358a5b838a2fc`, records the exact upstream file hashes, retains the upstream MIT copyright and permission notice in `UPSTREAM_LICENSE`, and removes API-key, proxy, cloud-provider, and model-override environment variables before launch so the external Claude subscription login remains authoritative. Auto-update is disabled so runtime bytes cannot drift from the scanned image. The image contains no credentials, Claude OAuth state, account data, vault references, or 1Password configuration; those remain external runtime inputs.

## Automated publication gate

Only `main` may enter `fleet-image-publish`. There is no manual reviewer prerequisite: the exact-SHA CI proof, preflight runtime checks, full SPDX, compact SPDX, Trivy result, VEX revalidation, frozen candidate verification, and attestations are mandatory and fail closed. Publication still does not authorize rollout.

### Scoped VEX handling

Trivy always runs and its full JSON report is retained. Critical findings fail the release unless `scripts/verify-trivy-vex.py` matches the finding to an unexpired, reviewed record in `release/vex-exceptions.json`. A record is bound to the exact vulnerability, package and versions, scanner target and type, immutable component image, executable SHA-256, approval, expiry, and the advisory's affected symbols. The gate extracts the executable from the built Fleet candidate and fails if its hash changes or an affected symbol is present. It runs both before saving the candidate and again before publication. Unknown critical findings, malformed policies, changed artifacts, expired records, or additional critical findings fail closed.

The current KEN-275 record covers only `GO-2026-6303` / `CVE-2026-56854` in the digest-pinned 1Password CLI 2.39.0 executable. It is classified `not_affected` because the advisory's SSH server symbols are absent and Fleet uses `op read` as a client. The record expires on 1 October 2026 and must be removed when 1Password ships a fixed executable.

## Rollback

Rollback is digest selection, not an image rebuild. A private deployment may retain a previously verified Fleet digest and select it after its own cold/warm compatibility and rollback tests. Publishing a digest does not authorize rollout.
