# Linear Agent plugin

The Fleet image bundles a generic, disabled-by-default Linear Agent Session worker. It does not contain routes, profile names, workspace names, OAuth bindings, requester allowlists, or webhook secrets.

A deployment that enables the plugin must provide its worker and publisher policies as read-only data mounts at:

```text
/opt/hermes/plugins/linear-agent/linear-agents.json
/opt/hermes/plugins/linear-agent/linear-publishers.json
```

Both policy files are deliberately absent from the image. Enabling the plugin without the worker policy fails closed before OAuth or inbox processing. The runtime accepts only a bounded regular file opened with no symlink following. It must be owned by root or the runtime UID, must not be group/world writable, and when runtime-owned must not be owner-writable. A root-owned `0644` bind mount is therefore readable but not writable by the UID 1000 worker.

The policy entry and profile-local plugin settings must agree exactly on:

- profile and logical agent;
- Linear workspace;
- managed OAuth vault and item binding identifiers;
- profile-local OAuth and Connect paths;
- rollout scope;
- optional requester UUID allowlist.

The protected profile Connect environment must contain `OP_CONNECT_HOST`, `OP_CONNECT_TOKEN`, and `OP_CONNECT_ALLOWED_HOSTS`. The allowlist is a comma-separated set of exact HTTP(S) origins; the configured host must be one of them. This keeps deployment endpoints outside public executable code while retaining fail-closed host approval.

`waiting_state_name` defaults to the generic `Waiting on Principal`; deployments whose Linear workflow uses another label must set it explicitly. The generic provisioning, reconciliation, tracking, project-update, and live-canary implementations ship in the image, but they do not contain deployment bindings and do nothing unless the operator supplies policy and invokes them.

Executable worker code must not be bind-mounted. Code is supplied by the attested Fleet image; deployment repositories supply only policy maps, configuration, secrets, and invocation. See `examples/linear-agent-policy.json` for a synthetic policy shape.
