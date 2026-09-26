# Linear plugin

Linear is the human record of work. This profile's Kanban board runs the work. The plugin translates
between them and holds no execution state of its own.

- **Linear** owns status, the delegate (which agent owns an issue, visible to every agent), comments,
  agent-session activity and project updates.
- **Kanban** (core, per container) owns execution: durable tasks, restart-safe workers, orphan
  reclaim, the retry breaker, and `blocked(needs_input)` as a durable stop.
- **The plugin** turns Linear events into Kanban calls, and Kanban events into Linear writes.

It is off by default. It shares nothing across containers: the Linear delegate is the only
cross-agent record.

## Enable it

Add one block to the profile's config, in the deployment overlay. The values below are synthetic.

```yaml
plugins:
  entries:
    linear:
      allow_gateway_injection: true   # lets follow-ups and stop requests reach chat sessions
      settings:
        enabled: true
        credentials:
          mode: connect                     # or: token_file (with path:)
          vault_id: example-vault-id
          item_id: example-item-id
          connect_env_file: /opt/data/.op.env              # OP_CONNECT_HOST / OP_CONNECT_TOKEN, mode 600
          cache_file: /opt/data/secrets/linear-oauth.json  # access-token cache only
        states:                             # defaults; a null value means "leave the status alone"
          in_progress: In Progress
          done: Done
          blocked: Blocked
        team_states:                        # per team key (or team id); overrides `states`
          OPS: {blocked: null, done: In Review}
        completion_contracts:               # Linear project id -> GitHub repo whose PR proves Done
          example-project-id: example-org/example-repo
        ingress_database: /opt/data/workspace/linear/ingress.db
        quiet_minutes: 30                   # chat project-update quiet period
        recheck_minutes: 5                  # ownership re-read interval for active work
```

Optional: `state_database` (default `<profile home>/linear/state.db`), `board` (Kanban board slug),
`api_url`, `tick_seconds` (default 2).

Credentials: with `connect`, 1Password Connect is the source of truth for the OAuth client and the
current refresh token. The local cache holds only the access token, plus a rotated refresh token
until Connect accepts it. Delete the cache and it rebuilds. Each profile uses its own Linear app
identity; the plugin reads `viewer.id` to learn who "self" is.

Webhooks: the host ingress verifies the HMAC signature and the one-minute `webhookTimestamp`
window (`api.verify_webhook` is the same check). It dedupes by the `Linear-Delivery` header, not
`webhookId`, which is constant. It then writes each delivery to this profile's inbox table
`deliveries(logical_agent, delivery_id, profile, payload, payload_sha256, received_at, status,
attempts)`. The plugin reads pending rows for its profile and marks them `imported`.

## What happens

| Event | Kanban | Linear |
|---|---|---|
| Delegated to this agent | `create_task`, key `linear:<issue>:<session>` | Thought within seconds, then delegate = self and In Progress |
| `linear start ABC-1` in chat | none; chat stays the executor | Refused with a link if another agent is delegate and it is started. Otherwise delegate = self and In Progress |
| Delegation while chat owns it, including our own start echo | none (loop guard) | One "already in progress from Hermes chat" response |
| Follow-up prompt | Task comment, unblock if blocked; chat: injected into the owning session | Thought "passed to ..." |
| Worker completes with evidence | task done | Done state, one response with the links, one project update |
| Worker completes without evidence | task done | Blocked state, one error: unfinished |
| Worker blocks (`needs_input`) | task blocked | Blocked state, one elicitation |
| Retry breaker trips | task blocked (`gave_up`) | Blocked state, one error |
| Stop (a prompt with `signal: stop`) | `block_task(needs_input)`, which survives restarts; chat: stop injected | Blocked state, "Stopped by ..."; the delegate is kept |
| Re-delegate or prompt after a stop or block | same task unblocked | In Progress |
| Re-delegate after Done | new task (new session, new key) | as delegation |
| Delegate moved to someone else (seen on re-read) | task blocked and archived; chat: told to stop | "Reassigned to ..." comment |
| Human moves the issue to Done or Canceled | task blocked and archived | nothing; the human wins |

**Done needs evidence.** For a PR, set `completion_contracts` so the task gets core's
`completion_contract`: core checks the exact PR head on GitHub before the task can complete. For
other work, the result must contain a link: the merged commit, a deploy check, or the findings.

**Restart.** Core respawns Kanban workers. The task body starts with a reconcile step, so a retry
checks what already happened before continuing. When the breaker trips, the issue goes to Blocked.
Chat work is asked to reconcile. If its session cannot be reached, it becomes Blocked with
"interrupted by restart".

**Ownership.** Before every status write, and every `recheck_minutes` for active work, the plugin
re-reads the issue. An Issue webhook that changes the delegate triggers an immediate re-read.
Session events older than the newest one handled for that issue are ignored, so a delayed
delegation cannot take work back. Ordering uses the session and activity `createdAt`. The issue's
`updatedAt` is not used, because Linear creates the session before it updates the issue.

**Project updates.** For chat, one per project per session, sent `quiet_minutes` after the last
turn. For Kanban, one per finished task.

## Delivery guarantees

Every Linear write goes through a local outbox, oldest first per issue.

- **Creates** (comment, activity, project update) carry a UUID v4 client `id`, stored when the
  write is queued. Linear rejects a repeat with `INPUT_ERROR` "conflict on insert of ..." naming that
  id, and `api.is_duplicate_create_error` treats that as sent. A retry after a lost response
  therefore never duplicates a comment.
- **Status and delegate writes** are set-to-value, preceded by the ownership re-read. A queued
  claim that meets a newer human edit (a close, or another delegate) is dropped rather than
  reopening the issue. A late Stop for an older session does not stop newer work.
- **Backoff** starts at 1 minute and doubles to a 1 hour cap. After 24 hours the write is marked
  failed. This is loud: an error log, a message in the owning chat (or a comment on the Kanban
  task), and one more try after the next successful write. A superseded status write never
  replays.
- **Rate limits**: when Linear answers `RATELIMITED`, all calls pause until the
  `X-RateLimit-*-Reset` time (epoch milliseconds).
- **Credential or permission failures**: a token failure (Connect or refresh) is retried like any
  outage; inbox deliveries are retried for a day before they are parked. HTTP 403 fails loudly
  at once.
- **Missing states**: a configured state name that does not exist on the team fails loudly. Set a
  team's state to `null` to skip that status change. The comment or activity still posts.

State: `work(issue_id, origin, owner_ref, task_id, project_id, last_updated_at)` holds a row only
while work is active; there are no tombstones. `outbox(id, kind, payload, attempts, next_at,
state)` has three states: pending, sent, failed.

## Known limits

- **Stopping chat work is a request, not a hard stop.** The stop is injected into the session.
  Core's `request_stop` only covers plugin-dispatched executions.
- **Chat commands need Linear reachable** to resolve an identifier. When it is not, the tool says
  so and asks the agent to retry.
- **Stop acts on queued or running tasks.** A task in `review` or `todo` cannot be blocked by core;
  the plugin says so in Linear instead of claiming it stopped.
- **A human edit racing an agent status write** is accepted. The re-read narrows the window.

## Tests

`tests/test_linear_plugin.py` needs no Agent source.

`tests/test_linear_kanban_scenarios.py` runs every rule against a fake Linear over HTTP and a real
Kanban board from the pinned Agent:

```bash
HERMES_AGENT_SRC=/path/to/hermes-agent /path/to/agent-venv/bin/python \
  -m unittest discover -s tests -p 'test_linear_kanban_scenarios.py'
```
