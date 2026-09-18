# Hermes Fleet

Opinionated isolated Hermes. One agent per container and gateway. No shared credentials.

The public distribution is a provenance-bound child image, independent sidecar images, and the contracts that keep deployment identity out of the runtime. Pin every image by digest. Source or image publication is not a production rollout. A standing team of specialists you text is an outcome, not the tagline.

## Why this exists

Hermes Agent is one process. That is the wrong blast radius for a team.

Fleet is many named specialists, each with its own container, gateway, writable state, workspace, Docker network, memory, and credentials. You text the same person back. A change to one leaves the others alone. Failed upgrades recover without taking the rest down.

The public product proves that pattern without shipping anyone's credentials, deployment identities, private binaries, or live topology.

## What winning looks like

These are the product outcomes. If a change advances none of them, it does not belong here.

- **Standing team.** Named specialists, each independently addressable, with unshared memory and credentials. Not one generalist wearing hats.
- **On a leash.** One agent cannot see another's files, network, or vault. An intended change lands on the target and every other in-scope agent is still independently working.
- **There when you reach.** Telegram, Slack, and Hermes Desktop have the same bar. A restart does not drop identity, tools, or sessions. Persistence includes working tools, not just surviving files.
- **Subscription-honest.** Claude stays on the granted plan login, not a silent API meter. Other billed plans follow the same rule.

North Star is **trusted unsupervised turns**: work the principal did not sit through, that finished without a rescue, stayed inside granted tools, and made no off-plan model call. Isolation that strips the integrations that make Hermes useful is a loss.

## Opinions we will not trade

**One profile, one container, one gateway.** A shared runtime with profile names is not a fleet. Each specialist is a Docker service with its own volume and network. No Docker socket, no host network, no privileged mode. The image user is UID/GID 1000 so containers can start with no-new-privileges.

**No shared credentials.** The child image includes the 1Password CLI with no vault config. The overlay injects Connect host, token, and allowed origins per container. Vault item bindings stay in that overlay. Two agents never share a Connect token or an OAuth login. Recreating a profile or borrowing another agent's secrets to get past a blocker is out of product.

**Plugins stay off until the overlay turns them on.** The image may bundle optional plugins. Default-enabled is empty. Enabling a plugin without its policy or origin fails closed. That is how we keep a generic image from becoming someone's deployment.

**Digest or it did not happen.** Consume `ghcr.io/mekenthompson/hermes-fleet-public@sha256:<digest>`. Mutable tags are not a supported interface. Built, published, and deployed are different claims. A green unit run is not publication. Publication is not a production rollout.

**Identity stays in the overlay.** This repository has synthetic examples. Household names, bot tokens, vault URIs, OAuth state, and live compose do not belong here. `scripts/verify-public-tree.py` fail-closes on the obvious cases. It is a shape check, not a secret scanner.

## How a deployment actually uses it

Copy [`examples/overlay-repo/`](examples/overlay-repo) into a private overlay repository. That overlay owns:

- fleet-wide Hermes defaults and per-agent overrides (`fleet-config/`)
- compose that pins this child image by digest
- unpublished binaries and install policy
- secret injection (1Password Connect, not files in git)
- plugin policy maps (Linear workers, browser origin, voice sidecar URL)

Each compose service is one specialist. The operator changes one service. The principal texts Telegram, Slack, or Desktop and gets that specialist back, on the model plan they already pay for.

The example profiles in `compose.example.yaml` are synthetic. See [`docs/overlay-repository.md`](docs/overlay-repository.md).

## Plugins, and why they exist

Optional, **disabled by default**. They exist so isolation does not mean a dumb agent.

### Linear Agent

Multi-agent work tracking without a shared inbox. Each profile is a Linear worker with its own identity. The public image ships the worker; it does not ship routes, workspace names, or OAuth bindings. The overlay mounts root-owned policy maps. Enable without policy and the process fails closed before OAuth.

The principal assigns work in Linear. Specialists pick up the cards they own. They do not read each other's queues. Stop containment stays durable across a bounce. Policy and plugin settings must agree on profile, workspace, vault item binding, and rollout scope. Details: [`docs/linear-agent.md`](docs/linear-agent.md).

### Browser handoff

The agent can run an isolated browser and hand the live session to the human when a login or a visual check needs a person. Cookies and origin stay on that profile's broker and sidecar. The public origin comes from `HERMES_BROWSER_HANDOFF_PUBLIC_HOST` at deploy time, not from this git tree. Sidecar images (broker, Camofox) publish separately; the Fleet child does not bake them in.

### Claude ACP

Stay on the granted Claude plan login via a profile-local ACP client. The overlay wires the account. The image does not silently fall back to an API key.

### Kokoro voice

Local TTS through a sidecar. Voice on Telegram without a cloud TTS meter. Sidecar URL and token come from the overlay.

### Perplexity search

A `web_search` backend only. Not a general web browser, not an extract path, not on by default.

### Read-only source snapshots

Allowlisted list/read/search over source snapshots so a specialist can inspect code without a write path into the tree.

### Tooling policy hook

Optional overlay binary that gates extra installs. Disabled until the overlay wires it.

## Architecture

```text
Hermes Agent image  (exact digest)
        |
        v
Hermes Fleet child  (this repo)
        |
        +-- profile A container + state volume + workspace + network
        +-- profile B container + state volume + workspace + network
        +-- independent sidecar images (browser broker, proxies, Camofox, Kokoro)
```

Sidecar source lives under `sidecars/`. Each sidecar publishes its own GHCR image through a dedicated workflow.

## What's in the image

The published child, `ghcr.io/mekenthompson/hermes-fleet-public`, is a digest-pinned child of an exact Hermes Agent image. It currently bundles:

- UID/GID 1000 for the `hermes` account so profile containers can start with no-new-privileges
- GitHub CLI 2.98.0 from the official release tarball
- 1Password CLI 2.39.0 from its digest-pinned official image (no vault config)
- The Agent `honcho` extra, with no workspace identifiers
- Codex, Grok, OpenCode, and Claude Code from the committed lockfile
- The optional plugins listed above
- Optional `tooling-policy-hook` binary for overlay install policy

Consume it only as:

```text
ghcr.io/mekenthompson/hermes-fleet-public@sha256:<digest>
```

## What's not in this repository

Keep these in a private overlay repository, not here:

- Credentials, OAuth state, vault references, and secret-source URIs
- Profile identities, live compose, and host topology
- Private binaries and unpublished local CLIs
- Deployment policy such as Linear agent maps

## Quick start

Build and compose only through the wrappers. Direct `docker build` / `docker compose` skip the immutable-reference guard.

```bash
python3 scripts/build-fleet-image.py \
  --agent-image ghcr.io/example/hermes-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --tag hermes-fleet:local

export HERMES_FLEET_IMAGE=ghcr.io/example/hermes-fleet@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
python3 scripts/compose.py config
```

## Development

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/verify-public-tree.py
```

Pull requests build a non-publishing `linux/amd64` candidate, verify runtime/provenance, and run the critical-vulnerability gate. Pushes to `main` that touch image inputs publish the exact scanned candidate. See [`docs/image-release.md`](docs/image-release.md).

Coding agents should start at [`AGENTS.md`](AGENTS.md). Product scope is [`reference/`](reference/).

## Docs

- [`AGENTS.md`](AGENTS.md) — how to work in this repo
- [`reference/`](reference/) — vision, principles, invariants, product spec, jobs
- [`docs/overlay-repository.md`](docs/overlay-repository.md) — overlay repo layout, config layers, sample files
- [`docs/image-release.md`](docs/image-release.md) — image bake, scan, and publication
- [`docs/linear-agent.md`](docs/linear-agent.md) — Linear worker plugin (policy stays external)
- [`docs/perplexity.md`](docs/perplexity.md) — optional search provider
- [`plugins/kokoro-voice/`](plugins/kokoro-voice/) — optional local Kokoro sidecar TTS
- [`plugins/browser-handoff/`](plugins/browser-handoff/) — optional browser handoff
- [`plugins/readonly-source/`](plugins/readonly-source/) — optional allowlisted read-only snapshot tools
- [`scripts/tooling-policy-hook`](scripts/tooling-policy-hook) — optional overlay install-policy hook
- [`SECURITY.md`](SECURITY.md) — vulnerability reporting and public boundary
- [`contracts/`](contracts/) — machine-readable architecture boundaries

## License

[MIT](LICENSE)
