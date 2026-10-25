# Linear outcome tracking with native Hermes execution

Ordinary chat, questions, short commands and bounded investigations require no Linear issue.
Use native durable tasks when remaining work must survive a process, wait asynchronously, or
continue through implementation, review and release. Link meaningful human outcomes to Linear.

## Architecture

| Concern | Authority |
|---|---|
| Authenticated user, allowed scope and permissions | Native Hermes |
| Short interactive work | Current native conversation |
| Durable execution, recovery, Stop and stale workers | Native task, worker claim and execution generation |
| Human outcome, evidence and blockers | Linked Linear issue |
| Progress destination | Authorized native subscriber |
| Remote reporting and uncertain delivery | Plugin durable outbox |

Previously the plugin claimed chats, parsed continuation phrases, injected restart instructions,
and maintained chat-to-worker claim/transfer/resume protocols. New work uses one existing native
task and one issue/task reporting binding. The plugin does not create a chat execution owner,
start chat turns on restart, or decide whether ordinary tools may run. Legacy records remain
visible and fenced; they are not silently adopted, deleted or replayed.

A channel change never moves a worker. A conversation reference is a delivery address, and an
issue identifier, delegate field or comment is not a tool-execution grant. Verified Linear
assignments and cancellation signals become native execution requests. Duplicate deliveries use
native request keys and the existing outbox. No additional scheduler or recovery daemon exists.

## Track existing work

Create and control durable work with native Hermes task actions. Then link it:

```json
{"action":"track","issue":"EXAMPLE-123","task_id":"existing-native-task"}
{"action":"status","issue":"EXAMPLE-123"}
```

`track` validates the authenticated host and profile, issue/workspace/specialist scope, and native
task identity. Linking never creates, starts, unblocks, transfers or replaces a task. It queues
reporting after native work already exists; Linear delivery does not gate that worker. Retrying
a link from another authorized conversation returns the same binding and leaves its claim and
execution generation unchanged. Linking does not subscribe another channel or broadcast progress.

The native task owns completion, blocking, Stop and review. Its events project to an Agent Session
when one is bound, otherwise to issue comments. Legacy `kanban_chat` and `kanban` records share
that execution path; their reporting address is the only distinction. A coordinator may supervise
distinct children but must not also execute their claimed work.

Natural-language follow-ups are interpreted in the authenticated conversation and delivered once
to the existing native task. Use stable native creation and comment request keys after lost
responses. Status questions do not resume stopped work. Completed work returns status; additional
requested work is a new increment. Reconcile recoverable native state and external effects before
resuming; a stale heartbeat alone does not prove death. Another agent's executor requires the
supported native handoff. Merely opening another conversation interrupts nothing.

The retired `start`, `delegate`, `transfer` and `resume_transfer` protocols refuse new execution.
Compatibility `done`, `blocked` and `release` exist only for retained legacy chat records. No exact
sentence or user-authored JSON grants continuation. Unknown legacy continuation deliveries remain
visible in `hermes linear status` and are not replayed on restart.

## Enable it

Add one block to the profile's config, in the deployment overlay. The values below are synthetic.

```yaml
plugins:
  enabled: [linear]  # append to the existing list; preserve other enabled plugins
  entries:
    linear:
      allow_gateway_injection: false
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
        recheck_minutes: 5                  # ownership re-read interval for active work; capped at 10
```

Optional: `state_database` (default `<profile home>/linear/state.db`), `board` (Kanban board slug),
`api_url`, `tick_seconds` (default 2). `ingress_profile` optionally selects the exact profile identifier
written into this private inbox by its trusted ingress route. It defaults to the runtime profile;
set it explicitly when a standalone `default` executor has a separately named inbox route. Only
the inbox selector changes, including Stop priority and resume checks. Native authorization, executor
identity, board, app/workspace credentials and scope do not change. The override must contain only
ASCII letters, digits, underscores or hyphens; malformed values refuse startup before credentials
or state admission. This does not enable the plugin or replay imported deliveries. Changing an
existing inbox's binding can expose previously pending deliveries: reconcile them and obtain
scoped activation approval before deploying the binding.

API requests share a serialized client within the profile. HTTP 400 GraphQL `RATELIMITED`
and HTTP 429 pause requests until the exhausted request, complexity, or endpoint budget's
epoch-millisecond reset; `Retry-After` seconds or HTTP dates can extend that pause.
Missing or unusable timing falls back to 60 seconds. Successful responses that exhaust a
budget are returned normally, then subsequent requests pause. Endpoint exhaustion
conservatively pauses the whole profile client. The longest cooldown survives restarts in
`<state_database>.rate-limit.json`, containing only a timestamp. Startup waits for it before
identity validation and work admission. Quota state stays local to the profile; each profile
uses its own app identity. Ownership and authorization reads remain fresh. Queued writes
retain their client IDs and wait for quota recovery.

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

Already-authorized native work continues safely while Linear is unavailable. New Linear-origin
assignments require fresh app/workspace and requester authorization before native admission.
No reporting outage or queued claim grants execution, and no chat is restarted to repair reporting.

Webhooks: the host ingress verifies the HMAC signature and the one-minute `webhookTimestamp`
window (`api.verify_webhook` is the same check). It dedupes by the `Linear-Delivery` header, not
`webhookId`, which is constant. It then writes each delivery to this profile's inbox table
`deliveries(logical_agent, delivery_id, profile, payload, payload_sha256, received_at, status,
attempts)`. The plugin reads pending rows for its profile and marks them `imported`.

## Linear-origin events and native lifecycle

Verified assignments request one native task using the event's stable idempotency key. Existing
bindings route requests to that task; no second worker is created to repair status. Follow-up
comments are saved with native idempotent comment keys and do not automatically unblock stopped
work. Explicit verified assignments may request native resumption, whose retirement and claim
checks still apply. Unverifiable events admit no work.

Human reassignment or closure is reread before affected execution requests and status writes.
Native cancellation records Stop and reconciles worker/process teardown; acknowledgement does
not prove external effects ended. Old workers retain no authority to write or close a replacement
run. A missing or expired background completion grant produces a diagnostic before effectful
background work; no unscoped approval fallback is permitted.

Core owns restart recovery and process liveness. The plugin recovers reporting only. Notifications
must never start a chat turn to manufacture execution authority. Native task subscribers receive
progress on their authorized destinations. A notification recipient change is not executor change.

## Durable reporting and uncertain effects

Every remote update uses the profile-local durable outbox. Creates retain stable UUIDv4 IDs across
retries; issue writes reread current ownership and human edits. Read-before-write/readback is
best-effort rather than remote compare-and-swap. Advisory thoughts/actions get one try and do not
hold other writes. Native event cursors and outbox capture deduplicate completion and milestone
notifications across restarts.

A lost status or description response is reconciled against external state before retrying.
Neither a remote Done nor a newer reopen proves an uncertain predecessor's outcome. Earlier
receipts, task history, checkpoints, workspaces and PRs remain available. A PR already merged or
a deployment already completed is read back and continued from, never replayed from stale prose.
Ordinary chat and unrelated native work continue while reporting waits.

Failed writes, revoked permissions and missing historical requester provenance remain visible.
No revoked state is cleared, command guard disabled, or newer session used as old authority.
Queued reporting is not remote delivery; source tests, merge, publication, deployment and live
acceptance are separate claims.

## Migration and operation

Inventory existing chat-owned, task-owned, manual and stopped work before migration. Reconcile
actual workers, authorizations and unknown effects through supported native controls. Explicitly
link only remaining work after overlapping execution has retired; preserve its authorization and
effect history. Unresolved records stay visible and fenced. Never edit ownership databases,
automatically adopt manual tasks, delete checkpoints or create duplicate executors.

The reporting binding records `linked_at` as its source provenance. It is separate from the
native resumption fence: a same-timestamp initial Stop wins, and an out-of-order distinct comment
cannot hide Stop or another instruction. Existing records receive only the additive default of
zero, which fences event-driven execution requests until the retained binding and effects are
reconciled and an authenticated native turn explicitly revalidates the same task. Migration does
not infer a new binding, native grant or execution generation.

```bash
hermes linear status --json
hermes linear resolve <row> applied|not_applied --evidence "verified external state"
hermes linear replay <delivery>
```

These operator actions reconcile reporting receipts and parked inbox deliveries. They do not grant
execution or resume stopped tasks. A queued legacy release is not relinquishment: verify remote
readback and local removal before a handoff. The historical transfer metadata is read only to
preserve fences; no transfer/resume protocol writes new ownership records.

## Tests and release

Before credential access or state admission, the gateway service checks the native task authority
and idempotent-comment APIs. An incompatible runtime receives a clear diagnostic and the service
stays inactive; existing records are preserved. Import compatibility permits reviewed bundle
staging during a coordinated image upgrade and never grants execution on the older runtime.

Run `python3 -m unittest discover -s tests -p 'test_*.py'` and
`python3 scripts/verify-public-tree.py`. The native boundary suite uses
`scripts/run-linear-scenarios.py --agent-source <manifest-pinned-clean-source>` and refuses skips.
It exercises registered producer, real private transport/host journal, native task store and HTTP
outbox, alongside native admission/delegation/completion and Stop tests. Helper-only tests do not
establish deployed acceptance. Independent review, required exact-head CI, published immutable
release and deployed Desktop/messaging checks are required before claiming completion.

## Specialist authorization

An optional `specialist_scope` requires all three nonempty, unique exact-ID lists:

```yaml
specialist_scope:
  allowed_team_ids: [team-id]
  allowed_project_ids: [project-id]
  allowed_requester_ids: [user-id]
```

To cover every team and project in the bound organization, including new teams,
new projects, and issues without a project, use this explicit form instead:

```yaml
specialist_scope:
  all_teams: true
  all_projects: true
  allowed_requester_ids: [user-id]
```

Both flags must be exactly `true`. Omitting a list, mixing forms, or leaving the
requester list empty is refused. Organization-wide scope keeps app/workspace,
issue creator, session requester, and prompt actor authorization checks by default.

For work delegated by an allowed requester regardless of the issue author, add
`requester_basis: agent_session` to either form. The bridge resolves the exact
Agent Session from Linear, requires its human creator to be allowlisted and its
app user to match this client, then binds subsequent issue effects to the session
in the existing durable work row. Queued effects retain their exact issue/session
bindings in the outbox payload, so closeout remains authorized after that work
row is deleted and cannot borrow a successor's authority. Every transport attempt,
including a credential-refresh retry, rechecks authority after credential resolution.
Reply events are revalidated against their exact activity and session before delivery to native work. Incompatible requester bindings remain separate project
update batches, and transport admission independently checks superseded ownership.
Legacy effects or reply intents lacking those bindings
fail closed and require reconciliation instead of borrowing current work authority.
Every effect resolves that session again;
issue authorship alone never grants authority in this mode. Mentions use the same
session-created path. Replies require an authoritative activity from an allowed
human on that exact session. Foreign prompts are refused without poisoning an
otherwise authorized issue. Missing human creator, wrong app, or unavailable
session authority fails closed; automatic app-created sessions are not evidence
of human delegation. An unbound chat claim cannot bootstrap this policy.

Omitting `requester_basis` (or using `issue_creator`) preserves the original
creator-based policy. This opt-in does not clear existing scope fences or replay
historical deliveries. Deploy the reviewed source and policy separately, then
reconcile retained denials through an explicitly approved recovery procedure.

This is separate from the immutable app/workspace identity. The client resolves
issues, Agent Sessions and prompt activities through Linear before admitting
work, and rechecks the owning issue before every effect. A foreign requester,
team, project or mismatched session is refused without admitting executable work or a Linear effect.
Permanent scope denials stay fenced even if the issue later appears allowed. The state work
row, Kanban event history, and queued writes remain for reconciliation. Fenced Kanban tasks
are archived to stop workers; core may clean their managed workspace. New Kanban tasks stay
blocked until their issue and work row are durably admitted. Acknowledgements are queued
before the bridge reports a successful start.
Transient resolution failures retain delivery work for retry. Arbitrary GraphQL
is disabled in specialist mode; only the bounded operations are available.
General-mode behavior is unchanged. Specialist chat actions obey the same issue
boundary; retained legacy chat terminal actions stay fenced to their historical owner.
