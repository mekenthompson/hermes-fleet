# AGENTS.md

Instructions for humans and coding agents working in `mekenthompson/hermes-fleet`.

This is the **public** Hermes Fleet distribution. It owns generic implementation, the opinionated Fleet child image, independent sidecar source, and public GHCR publication. Deployment overlays, identities, and private binaries live elsewhere.

## Hard rules

- Pin images by immutable digest. Never teach people to run mutable tags.
- Do not add credentials, vault URIs, OAuth state, sessions, memories, logs, or live topology.
- Do not add household names, private hostnames, or real profile identities.
- Do not vendor private binaries or unpublished local CLIs. Private binaries belong in the private overlay repository, with a hashed manifest and a COPY+sha256sum install.
- Do not enable optional plugins by default.
- Do not treat a green unittest run as publication approval. Image publication still requires the Fleet image workflow.
- Do not bounce, start, or pin live household services from this repository.

## Layout

```text
Dockerfile                 Fleet child only
compose.example.yaml       Synthetic example, not a production baseline
contracts/                 Public product boundaries
docs/                      Human docs for public behavior
plugins/                   Optional, default-disabled plugins
release/                   Pinned Agent parent handoff
scripts/                   Build, compose, and verification wrappers
sidecars/                  Independent sidecar source and image workflows
tests/                     Public product and sidecar tests
```

There is no `sidecars/` copy in the private overlay, and no private compose in this tree.

## Tests you must run

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/verify-public-tree.py
```

If you change the Dockerfile or anything it copies, also expect `.github/workflows/fleet-image.yml` to bake. Keep `scripts/fleet-image-change-scope.py` in sync with every `COPY` source.

## Pull requests

- Small, one topic. Rebase onto current `main`.
- Independent review before merge. Reviewers are read-only.
- Public repo merges with rebase. Do not force-push `main`.
- Do not add reviewers, do not merge on failed CI, do not publish from a PR.

## Image changes

The child must keep:

- Digest-pinned Agent parent
- `USER 1000:1000` after rebinding `hermes`
- Inherited Agent entrypoint and command
- No Docker socket, no host network, no privileged mode

If you add a tool, pin it (version + sha256 or lockfile) and prove it in `fleet-image.yml` on both preflight and publish.

## Docs

Update `README.md` when the public product shape changes. Keep this file honest about the public/private split. `SECURITY.md` is the vulnerability path.
