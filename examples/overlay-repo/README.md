# Example overlay repository

Placeholder overlay for a Hermes Fleet deployment. Copy this directory into a
new git repository. Replace image digests, agent names, and config values.
Do not copy secrets into git.

## Layout

```text
compose.yaml                 Digest-pinned Fleet child. No secrets.
fleet-config/schema.yaml     Allowlisted desired-state keys
fleet-config/defaults.yaml   Shared defaults for every agent
fleet-config/agents/         One YAML file per agent
managed/                     Optional /etc/hermes/config.yaml mounts
```

## Config split

- `fleet-config/defaults.yaml` — every agent. No `profile:` key.
- `fleet-config/agents/*.yaml` — one file per agent. `profile:` matches the
  filename stem.
- `managed/*.yaml` — optional `/etc/hermes/config.yaml` mounts. Restart-gated.
- live `/opt/data/config.yaml` — not in this repo
- secrets — runtime injection only. Never git.

`fleet-config/` is overlay desired-state for a renderer. Compose does not mount
it. Set `HERMES_FLEET_IMAGE` to an immutable `@sha256:` reference before
`docker compose config`.
