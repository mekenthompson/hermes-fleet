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
  enabled: [linear]  # append to the existing list; preserve other enabled plugins
  entries:
    linear:
      allow_gateway_injection: true   # lets follow-ups and stop requests reach chat sessions
      settings:
        enabled: true
        identity:
          viewer_id: example-app-user-id
          organization_id: example-workspace-id
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

For an approved fresh-work pilot only, set `activation_cutoff_ms` to a positive integer Unix epoch
timestamp in milliseconds. Events with a source timestamp before it (or without a usable source
timestamp) are imported without being applied; the boundary is inclusive. The cutoff is persisted
in the state database, cannot be changed, and cannot first be set over existing work, outbox, or
chat-stop state. Use a fresh isolated state database and inbox for activation. Leave it unset to
preserve legacy behavior; this boundary does not recover or clear legacy state.

Credentials: with `connect`, 1Password Connect is the source of truth for the OAuth client and the
current refresh token. The local cache holds only the access token, plus a rotated refresh token
until Connect accepts it. Delete the cache and it rebuilds. Each profile uses its own Linear app
identity. Deployment requires the immutable `identity.viewer_id` and
`identity.organization_id` binding above. Startup validates the binding before credential access
or state admission. API calls and chat authorization revalidate the actor/workspace and refuse
mismatches rather than falling back to a delegate. Optional `identity.teams` and
`identity.projects` lists restrict issue lookups and issue writes; they are not a complete
specialist authorization boundary for Agent Session activity or arbitrary GraphQL calls.
Keep specialist activation disabled until its separate scope contract is accepted.

Existing work has the same authorization requirement: delegation-driven task resumption,
work-bearing follow-ups, and chat recovery re-read the bound actor/workspace, applicable
issue scope and current delegate before scheduling or injection. A failed read, foreign
or absent delegate, or identity/scope mismatch leaves the existing work and owner unchanged.
A queued chat claim is not confirmation of delegation. Stop remains a separate fail-safe
control path and does not depend on permission to resume.

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
| Stop (a prompt with `signal: stop`) | `block_task(needs_input)`, which survives restarts; bound chat: generation-fenced core chat Stop | Kanban: Blocked and "Stopped by ...". Chat: Blocked with a Stop receipt activity; the delegate is kept |
| Re-delegate or prompt after a stop or block | same task unblocked | In Progress |
| Re-delegate after Done | new task (new session, new key) | as delegation |
| Delegate moved to someone else (seen on re-read) | task blocked and archived; chat: told to stop | "Reassigned to ..." comment |
| Human moves the issue to Done or Canceled | task blocked and archived | nothing; the human wins |

**Done needs evidence.** A GitHub PR link is checked against the exact head and required checks
before either a Kanban result or chat command marks Linear Done, including projects without a
configured contract. Set `completion_contracts` to make core enforce that check before Kanban
completion too. A non-GitHub pull-request link is not accepted as completion evidence. Other
work needs a destination link such as a merged commit, deploy check, or findings.

Queued PR closeouts retain the accepted head and completion contract. Delivery rechecks
acceptance on that same head before changing status. If acceptance changes after an uncertain
send, the original write and dependent evidence remain held for remote reconciliation.
Legacy queued PR closeouts without a recorded head also stay held; current checks cannot
establish which head the earlier completion accepted or whether an earlier write landed.
An older task or chat closeout cannot change a successor's status; an unsent predecessor
can still receive a truthful local-result receipt without a Done claim or project line.

Chat closeout receipts acknowledge durable local queuing, not remote acceptance.
Named mutations require literal `success: true`; refusals retain failed recovery rows and
alert the owning destination. New execution requires a fresh issue ownership read as well
as verified actor identity. An API outage delays new delegation rather than admitting stale work.

**Restart.** Core respawns Kanban workers. The task body starts with a reconcile step, so a retry
checks what already happened before continuing. When the breaker trips, the issue goes to Blocked.
The bridge records each Kanban event and its queued Linear writes in one local transaction;
after a crash it replays events beyond that local cursor even if core's notification claim advanced.
Rows upgraded from the older work schema reconcile historical transitions against the current
task state, so an old breaker event cannot block a task that has since resumed.
Chat work is asked to reconcile. If its session cannot be reached, it becomes Blocked with
"interrupted by restart".

**Ownership.** Before every status write, and every `recheck_minutes` for active work, the plugin
re-reads the issue. An Issue webhook that changes the delegate triggers an immediate re-read.
Session events older than the newest one handled for that issue are ignored, so a delayed
delegation cannot take work back. Ordering uses the session and activity `createdAt`. The issue's
closure timestamps fence claims against an authoritative closed state; `updatedAt` is a
conservative fallback when the closure timestamp is absent. It does not order sessions,
because Linear creates the session before it updates the issue. A fresh explicit
delegation can reopen closed work. Every accepted chat follow-up advances the source watermark.
A refused chat injection remains in ingress for retry across restart, with the existing one-day
parking limit. An interrupted injection has an unknown outcome and is not repeated; its receipt
asks for reconciliation, and a newer explicit instruction can steer the same owner.

**Project updates.** For chat, normally one per project per session, sent `quiet_minutes` after the
last observed turn. If a terminal status is still retrying, owned nonterminal lines can publish
first; the terminal line stays queued and can publish in a second update after the status succeeds.
Later turns in the same session cannot edit an update already sent. For
Kanban, one per finished task. At send time each issue's line is checked against its current
delegate and state; a taken-over issue is omitted without dropping other owned lines. The
batch is frozen from the latest committed rows before its first send, so a concurrent terminal
capture cannot disappear behind a stale outbox snapshot. A pending or retryable failed claim
holds its issue's line until ownership is decided. An unpublished batch with no owned lines is
forgotten, leaving the session able to publish later valid work. The quiet deadline stops moving
when the first send begins.

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
  task), and one more try after the next successful write. A failed chat alert remains due until
  injection succeeds. A superseded status write never replays.
- **Rate limits**: when Linear answers `RATELIMITED`, all calls pause until the
  `X-RateLimit-*-Reset` time (epoch milliseconds).
- **Credential or permission failures**: a token failure (Connect or refresh) is retried like any
  outage; inbox deliveries are retried for a day before they are parked. HTTP 403 fails loudly
  at once.
- **Missing states**: a configured state name that does not exist on the team fails loudly. Set a
  team's state to `null` to skip that status change. The comment or activity still posts.

State: `work` holds the active issue, including nullable `run_generation`; upgrades never infer a
generation for earlier chat work. `ownership_id` fences each local work incarnation;
`pending_resume` retains an unfinished Kanban resume or chat injection intent across crashes.
`chat_stop` retains the original profile, issue, Linear session,
activity, session key and generation across crashes. Its Stop receipt and worker observation may
be accepted, pending, completed, unknown, stale, not running or unsupported. The receipt means
core accepted an interrupt request; worker completion never proves external effects stopped.
The outbox keeps the same UUID v4 for each acknowledgement across retries, so Linear sees one
activity even if the response is lost. `outbox` has pending, sent and failed states.

## Known limits

- **Chat Stop is scoped to a bound ordinary chat run.** The plugin saves the host-provided session
  key and run generation at `linear start`. A Linear Stop records that exact target durably before
  calling core on the gateway loop. A restarted or late request can only retry that generation;
  it cannot stop a successor. Legacy and unbound chat rows report unsupported and stay Blocked
  until a newer Linear prompt; chat start cannot prove a newer turn without a saved generation.
  An ambiguous response stays visible as unknown or stale, never as proof of cancellation.
  `linear done` stays fenced until a newer prompt or explicit chat start resumes it. Stop does not
  undo tools, child processes or external effects that already ran; reconcile before resuming.
- **Chat commands need Linear reachable** to resolve an identifier. When it is not, the tool says
  so and asks the agent to retry.
- **Stop acts on queued or running tasks.** A task in `review` or `todo` cannot be blocked by core;
  the plugin says so in Linear instead of claiming it stopped.
- **A human edit racing an agent status write** is accepted. The re-read narrows the window.
- **Missing closure timestamps leave ordering ambiguous.** The conservative `updatedAt` fallback
  can refuse a fresh re-delegation if another edit follows its source event. Reconcile or send
  a newer instruction; the bridge does not infer permission to reopen closed work.
- **An uncertain project-update send keeps its original UUID and body.** The body freezes before
  the create call, including a possible crash just before the call. If Linear accepted the create
  but its response was lost, changing a retry could not change the update already published.
  A later terminal result remains on the issue status and evidence comment, but may not appear in
  that session's project update. If ownership changes before retry, the write holds for remote
  reconciliation instead of publishing a changed body under the same UUID.

## Tests

`tests/test_linear_plugin.py` needs no Agent source.

`tests/test_linear_kanban_scenarios.py` runs every rule against a fake Linear over HTTP and a real
Kanban board from the pinned Agent:

```bash
python3 -m venv .venv-linear
.venv-linear/bin/python -m pip install --require-hashes --only-binary=:all: \
  -r release/linear-scenario-requirements.txt
.venv-linear/bin/python scripts/run-linear-scenarios.py \
  --agent-source /path/to/manifest-pinned/hermes-agent
```

Run from this repository with Python 3.14.7 and a separate process from general unit discovery.
The runner checks the source Git HEAD against `release/agent-image-manifest.json`, then fails for
missing source, import errors, zero tests, failures, or any skipped scenario. CI checks out that
exact revision and runs this lane on every pull request and main push. The scenarios use fake
Linear traffic and local Kanban state; they do not enable the runtime plugin. Unit tests and this
scenario lane prove source behavior, while image publication and deployment require their own gates.
