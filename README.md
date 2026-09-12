# Hermes Fleet

An opinionated, public Docker isolation pattern for running multiple [Hermes Agent](https://github.com/NousResearch/hermes-agent) profiles as separate containers.

This repository publishes an opinionated, provenance-bound Fleet child image through a protected manual release workflow. Source publication and image publication do not imply production readiness or authorize deployment.

## Design

- Build a complete Agent image from an exact pushed public fork commit.
- Build the Fleet image as a digest-pinned child of that Agent image.
- Run one container, writable state volume, workspace volume, and Docker network per profile.
- Keep Docker socket, host networking, host bridges, privileged mode, credentials, identities, sessions, memories, and deployment topology out of the public image.
- Keep optional integrations as standalone plugins, disabled by default.
- Bundle a generic Perplexity Search API provider while requiring its API key from the deployment secret boundary.
- Bundle a generic Webkite CLI provider, GitHub CLI, extra coding CLIs, and the Honcho Python extra without deployment identities or workspace IDs.
- Rebind the runtime user to UID/GID 1000 so profile containers can start with no-new-privileges.
- Bundle generic Linear Agent executable code while requiring deployment policy as a separate read-only data mount.

## Supported local commands

The supported build wrapper validates the Agent base reference before Docker sees it:

```bash
python3 scripts/build-fleet-image.py \
  --agent-image ghcr.io/example/hermes-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --tag hermes-fleet:local
```

The supported Compose wrapper validates `HERMES_FLEET_IMAGE` before invoking Docker Compose:

```bash
export HERMES_FLEET_IMAGE=ghcr.io/example/hermes-fleet@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
python3 scripts/compose.py config
```

Direct `docker build` and `docker compose` calls bypass the immutable-reference guard and are not supported.

## Published image releases

The protected Fleet image workflow builds from the exact Agent handoff in `release/agent-image-manifest.json`. Pull requests run a non-publishing build, runtime/provenance verification, full and bounded SPDX generation, and Trivy critical-vulnerability gate. A manual `publish=true` dispatch from `main` promotes that exact scanned candidate without rebuilding.

Release source repository:

```text
https://github.com/mekenthompson/hermes-fleet
```

Release image repository:

```text
ghcr.io/mekenthompson/hermes-fleet-public
```

The GitHub repository uses the canonical product name. The existing GHCR package retains `hermes-fleet-public` so previously published immutable references remain valid.

Consume release outputs only by immutable digest. The authenticated House handoff artifact records the Fleet source/digest and its exact Agent parent. Image publication does not deploy profiles or authorize production rollout. See [`docs/image-release.md`](docs/image-release.md).

## Local validation

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/verify-public-tree.py
python3 scripts/verify-agent-image-ref.py   ghcr.io/example/hermes-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

## Example deployment

Set `HERMES_FLEET_IMAGE` to an immutable digest reference, then use `python3 scripts/compose.py`. The example profiles are synthetic. Replace them in a deployment repository, not in this public product tree.

The Phase 2 Compose file demonstrates volume and network separation only. Runtime hardening, egress controls, cold/warm state compatibility, UID/GID behavior, shutdown, persistence, and rollback remain Phase 3 acceptance gates. The example is not a production baseline until those daemon-backed tests pass.

The managed files under `examples/` demonstrate a small administrator overlay. They are not a security boundary and contain no secrets. Mutable Hermes configuration remains in each profile's `/opt/data/config.yaml`.

See `contracts/` for the machine-readable architecture boundaries.

The optional Linear Agent worker is documented in [`docs/linear-agent.md`](docs/linear-agent.md). Its executable code is part of the attested image; deployment policy remains external.
