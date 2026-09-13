# Linear Agent plugin

The Fleet image bundles a generic, disabled-by-default Linear Agent Session worker. It does not contain routes, profile names, workspace names, OAuth bindings, requester allowlists, or webhook secrets.

A deployment that enables the plugin must supply its worker and publisher policies at these default locations:

```text
/opt/hermes-fleet/policy/linear-agents.json
/opt/hermes-fleet/policy/linear-publishers.json
```

Set `HERMES_LINEAR_AGENT_POLICY_PATH` or `HERMES_LINEAR_PUBLISHER_POLICY_PATH` before starting the process to override the corresponding path. Policies may be supplied by a private deployment image or read-only data mounts; they are never included in the public Fleet image. They must contain policy and secret references, not secret values.

Enabling the plugin without the worker policy fails closed before OAuth or inbox processing. The reader accepts at most 1 MiB from a regular file, refuses symlinks, and rejects special files without waiting for a writer. The worker itself must not run as root. Policy files and every ancestor directory from `/` to the file must be root-owned and have no group or other write bits; a runtime-owned `0444` map is rejected because that runtime can chmod, replace, and restore it. Use root-owned directories (for example mode `0755`) and a root-owned readable policy file. Both `0444` and `0644` files are accepted: root is the trusted deployment writer, while the non-root runtime must not be able to write the file or replace it through an ancestor. A read-only mount does not relax the ownership or group/other-write checks.

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
