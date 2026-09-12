# Hermes Fleet

Opinionated Docker isolation for running multiple [Hermes Agent](https://github.com/NousResearch/hermes-agent) profiles as separate containers.

Hermes Fleet is the public distribution: a provenance-bound child image, independent sidecar images, and the contracts that keep deployment identity out of the runtime. Pin every image by digest. Source or image publication is not a production rollout.

## Why

Hermes Agent is one process. Fleet is many profiles, each with its own container, writable state, workspace, and Docker network. The public product proves that pattern without shipping anyone's credentials, deployment identities, private binaries, or live topology.

## What's in the image

The published child, `ghcr.io/mekenthompson/hermes-fleet-public`, is a digest-pinned child of an exact Hermes Agent image. It currently bundles:

- UID/GID 1000 for the `hermes` account so profile containers can start with no-new-privileges
- GitHub CLI 2.98.0 from the official release tarball
- 1Password CLI 2.39.0 from its digest-pinned official image (no vault config)
- The Agent `honcho` extra, with no workspace identifiers
- Codex, Grok, and OpenCode from the committed lockfile
- Optional plugins, **disabled by default**: Perplexity search, Linear Agent (policy mounted at deploy time), Claude ACP

Consume it only as:

```text
ghcr.io/mekenthompson/hermes-fleet-public@sha256:<digest>
```

Mutable tags are not a supported interface.

## What's not in this repository

Keep these in a private overlay repository, not here:

- Credentials, OAuth state, vault references, and secret-source URIs
- Profile identities, live compose, and host topology
- Private binaries and unpublished local CLIs
- Deployment policy such as Linear agent maps

`scripts/verify-public-tree.py` fail-closes on the obvious cases. It is a shape check, not a secret scanner.

## Architecture

```text
Hermes Agent image  (exact digest)
        │
        ▼
Hermes Fleet child  (this repo)
        │
        ├── profile A container + state volume + workspace + network
        ├── profile B container + state volume + workspace + network
        └── independent sidecar images (browser broker, proxies, Camofox, Kokoro)
```

Sidecar source lives under `sidecars/`. Each sidecar publishes its own GHCR image through a dedicated workflow. The Fleet child does not bake those sidecars in.

## Quick start

Build and compose only through the wrappers. Direct `docker build` / `docker compose` skip the immutable-reference guard.

```bash
python3 scripts/build-fleet-image.py \
  --agent-image ghcr.io/example/hermes-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --tag hermes-fleet:local

export HERMES_FLEET_IMAGE=ghcr.io/example/hermes-fleet@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
python3 scripts/compose.py config
```

The example profiles in `compose.example.yaml` are synthetic. For a real
deployment, copy [`examples/overlay-repo/`](examples/overlay-repo) into a
separate overlay repository. That is where fleet-wide Hermes defaults,
per-agent overrides, compose, and unpublished binaries live. See
[`docs/overlay-repository.md`](docs/overlay-repository.md).

## Development

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/verify-public-tree.py
```

Pull requests build a non-publishing `linux/amd64` candidate, verify runtime/provenance, and run the critical-vulnerability gate. Pushes to `main` that touch image inputs publish the exact scanned candidate. See [`docs/image-release.md`](docs/image-release.md).

Coding agents should start at [`AGENTS.md`](AGENTS.md). Product scope is [`reference/`](reference/).

## Docs

| Doc | What it covers |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | How to work in this repo |
| [`reference/`](reference/) | Vision, principles, invariants, product spec, jobs |
| [`docs/overlay-repository.md`](docs/overlay-repository.md) | Overlay repo layout, config layers, sample files |
| [`docs/image-release.md`](docs/image-release.md) | Image bake, scan, and publication |
| [`docs/linear-agent.md`](docs/linear-agent.md) | Linear worker plugin (policy stays external) |
| [`docs/perplexity.md`](docs/perplexity.md) | Optional search provider |
| [`plugins/kokoro-voice/`](plugins/kokoro-voice/) | Optional local Kokoro sidecar TTS (disabled by default) |
| [`SECURITY.md`](SECURITY.md) | Vulnerability reporting and public boundary |
| [`contracts/`](contracts/) | Machine-readable architecture boundaries |

## License

[MIT](LICENSE)
