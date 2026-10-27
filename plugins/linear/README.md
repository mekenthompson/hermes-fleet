# Linear plugin

Linear is the human work record. Native Hermes Kanban owns tasks, execution, claims,
worker recovery, permissions and Stop. This plugin links records, accepts verified Linear
requests and reports native outcomes through a durable outbox. An issue, comment, assignee
or reporting destination never grants execution permission.

Reporting remains compatible with official Hermes `ff294849385e433dd40a993b4f07673abb95c6f5`.
Automatic assignment admission additionally requires native block/unblock task-event guards
and guarded-resume event receipts for response-loss reconciliation.
No fork-specific invocation context, lifecycle controller, approval reservation or chat recovery
API is required. The plugin is disabled by default.

## Tools

| Action | Result |
| --- | --- |
| `track` with `issue`, `task_id`, optional `board` | Link an existing task on the caller's native board or an explicitly selected existing board. No task creation, claim, restart, resume or transfer. |
| `track` with `replace: true`, UUIDv4 `id`, `expected_binding_revision` | Explicitly replace a reporting link after inspecting actual work. Archive the old projection and hold superseded reports; no execution state changes or assertion that the old worker stopped. |
| `status` with `issue` | Read the issue, linked or retained native state, the reporting `binding_revision`, and verified `local.profile`/creation board for chat-origin work. |
| `followup` with `issue`, `body`, UUIDv4 `id` | Add an operator instruction to active linked native work. Stopped and completed work requires a current native action. |
| `create_issue` with UUIDv4 `id`, `title`, `team` | Create an undelegated issue; optional description/project/parent. |
| `create_project` with UUIDv4 `id`, `name`, `team` | Create a project. |
| `link_issue` with `issue`, `related`, `relation` | Relate existing issues. |
| `update_description` with `issue`, UUIDv4 `id`, `description`, `expected_description` | Queue a description replacement after checking the exact previous text. |
| `add_comment` with `issue`, UUIDv4 `id`, `body` | Queue a Linear comment. |

Ordinary content actions need no task link or chat ownership claim. They retain current app,
workspace and specialist authorization. Queued content receipts confirm durable capture, not
remote delivery. Use readable Linear Markdown. Description preflight is best effort; Linear
provides no remote compare-and-swap. Inspect conflicting human edits before replacing them.

Use existing native Kanban tools, Desktop controls or CLI for task creation, explicit resume,
cancellation and routing. Questions, short commands and bounded investigations work without
Linear. To continue active linked work, resolve its task and send a native operator comment;
opening another conversation does not replace its executor. Passive progress uses native
notification subscriptions with `notify`, never `wake` as an execution workaround.

Tracking preserves the native profile, board and task. The task must belong to the verified
executor profile. Native board validation and worker pins apply across the private RPC;
selecting a board cannot expand a worker's scope. Missing or archived boards are refused
without creating or reopening them. Subsequent status, follow-up and reporting use the
recorded board. The configured plugin board remains the creation destination for verified
Linear assignments, so linking an ordinary or project task does not require moving it.

Before creating work, a validated assignment inspects existing native boards in this home.
A task's `linear:<issue id>` retry key or explicit `Linear issue <id or identifier>: <issue URL>`
body line identifies existing work to inspect. A unique task with that explicit issue link,
assigned to the verified local executor, is linked on its original board; subsequent follow-ups
steer that task. A retry key alone cannot authorize adoption of manual work. Linking
does not resume stopped work or restart completed work. Several matching tasks, a different
executor, or an unavailable existing board require scoped inspection, never a replacement
worker or access to another profile's private state. A conflicting configured project
completion contract requires acceptance reconciliation; reuse cannot downgrade it.
Incidental issue mentions are not links.
Automatic binding rechecks all explicit issue lines on the freshly read native task.
Repeated matching lines and canonical ID/identifier aliases share one scope; distinct
explicit issue markers hold automatic or manual linking, replacement and follow-up delivery.
Follow-up delivery also holds any newly stopped task;
marker-free manually tracked work retains its authorized reporting binding. Validation
reflects the last native observation before the existing write boundary; native concurrency
and control rules continue to apply. Current explicit scope conflicts also hold native
Stop controls, new event capture and queued publication. The binding, native history and
queued evidence stay available for inspection; authorized status reads show reconciliation.
Unavailable history or event metadata that cannot be projected holds that binding's
cursor while other work and ordinary issue content continue. Persisted JSON is checked
before native decoding; SQL NULL and valid JSON null remain supported legacy absence.
Consumed run evidence uses the same check. Existing active board files must contain
a JSON object with a boolean archive flag when present; supported missing metadata
and legacy defaults still work. Invalid storage holds native admission and reporting
for that board while authorized issue content and other valid bindings remain usable.
Consumed native identity, evidence, event and comment text must decode as UTF-8 before
native constructors run; unreadable bytes cannot become a missing issue link or a
replacement-glyph outcome. Valid UTF-8 BLOB text and native legacy defaults remain usable.
Native predicate keys and all candidate run ordering stamps are checked before filtering
or selecting the latest run. An unreadable earlier attempt cannot win ordering and
publish stale evidence; unused earlier summaries and metadata remain native history.

Invalid events are retained
for inspection, without being consumed or repaired. Original held project lines can report separately as their scope
becomes valid; later same-session updates do not republish completed lines.
Uncertain started project requests retain their frozen content and identity.
Archived tasks and board archives in this native home are inspected as retained history.
An archived board is never reopened or linked for execution; a completed outcome stays
history until a deliberate new increment. Intact unrelated archives do not block new work.
Unavailable or ambiguous history holds the affected replacement while ordinary reads and
unrelated native work remain available. A missing database for an existing board is not an
empty board; a fresh named-board setup may still have no synthesized default database.
Plugin restart preserves missing, empty, damaged or unreadable existing native data. Native
operations inspect through the host's tracked read-only SQLite connections and read
transactions. SQLite supplies consistent committed snapshots and preserves transaction
locks across processes; contention refuses inspection after a finite timeout. Reads never
initialize, migrate, repair or change committed database, WAL or event data. SQLite may
create or update its standard `-shm` WAL-index cache and empty `-wal` bookkeeping file.
Startup never initializes an existing file. Authorized issue reads remain available even
when native evidence is unavailable or outside the caller's board pin; that evidence and
native mutations stay fenced. Restoring data needs no new activation marker or service reset.
Agents creating issue-related native work should preserve the explicit issue line and use
the canonical retry key when available; the user does not supply these internal arguments.

When no existing work is found, validated Linear assignments create a blocked native task, link it, then admit it using the
native event guard. The user does not need a separate activation step. Activation retains
the original creation token across retries; a newer Stop invalidates it. Native scheduling,
capacity and dependencies decide when the admitted worker runs. A restart can finish an
interrupted admission without creating a second task or refreshing a stopped task's token.

Older native hosts without those guards keep new assignments blocked and explain the missing
capability. Previously parked or legacy work is never automatically adopted on upgrade.
Explicit chat work/takeover uses native controls after inspection; linking reporting alone
never starts, stops or transfers a worker. Use `status`'s verified `native.local.profile` when
creating local native work rather than guessing from an agent's display name.

## Trusted host context

The registered handler captures the native session bound by upstream producers. It uses
`gateway.session_context.bound_session_key`, the current context's session variables and
`hermes_constants.get_hermes_home`; `profile_matches_home` checks the profile/home pair.
Environment-only session names and model-supplied identity fields are not credentials.
API turns use the actual upstream API-server session producer; messaging turns retain native
platform, user, route and session fields. An API session may have no messaging user ID; the
plugin preserves that fact and relies on the API host's existing admission.

Desktop/API processes use the same gateway bridge through a profile-private Unix socket.
Every action carries the complete captured context, including native delegated-child
restrictions. A delegated child cannot relay a native follow-up through the service. Both sides validate home/profile and
same-UID peer credentials; the directory is mode 700 and socket mode 600. Same-UID native
processes are the local trust boundary, not isolated hostile tenants. No transfer-only
allowlist or raw human phrase authorizes a request. Missing context, a different profile,
or an unavailable gateway produces an explicit refusal. After an uncertain response, inspect
delivery and reuse the original request ID; do not start another executor.

The service polls the existing ingress, persisted native events and Linear outbox in one
plugin-owned reporting thread. The upstream gateway host marker selects startup only; it is
not an authorization credential. Official `on_unload` stops that loop; Desktop/API discovery
registers the client without starting it. The private socket admits one reporting host. Registered
native lifecycle hooks only wake the service; callbacks perform no remote I/O or database
mutation. The persisted event cursor and queued effects provide restart catch-up. There is
no chat-stop loop, process controller or restart prose injection.

## Configuration

Add this to the profile's deployment overlay; the IDs are examples:

```yaml
plugins:
  enabled: [linear]
  entries:
    linear:
      settings:
        enabled: true
        identity:
          viewer_id: example-app-user-id
          organization_id: example-workspace-id
        credentials:
          mode: connect
          vault_id: example-vault-id
          item_id: example-item-id
          connect_env_file: /opt/data/.op.env
          cache_file: /opt/data/secrets/linear-oauth.json
        states:
          in_progress: In Progress
          done: Done
          blocked: Blocked
        ingress_database: /opt/data/workspace/linear/ingress.db
        recheck_minutes: 5
```

`token_file` credentials with `path` are also supported. OAuth and token caches remain private
profile state. The immutable viewer/workspace binding is revalidated on requests. Optional
`identity.teams` and `identity.projects` restrict issue access and writes. `state_database`
defaults to `<profile home>/linear/state.db`; optional `board` selects the destination for
Linear-origin task creation. `api_url`, `tick_seconds` (default 2), `team_states` and
`completion_contracts` retain their existing meanings.
`ingress_profile` selects the trusted inbox route only; it does not change caller authorization.
`activation_cutoff_ms` imports older signals without executing them and cannot retroactively
activate an existing state database. API quota backoff remains durable and profile-local.
A Linear outage delays reporting; unrelated native work and ordinary conversation continue.

Optional specialist scope requires exact nonempty `allowed_team_ids`, `allowed_project_ids`
and `allowed_requester_ids` lists. The organization-wide form uses explicit `all_teams: true`,
`all_projects: true` and nonempty `allowed_requester_ids`. `requester_basis: agent_session`
requires an authoritatively resolved session from an allowed requester, belonging to this app
and issue. Historical effects keep their original requester authority; a new session never
supplies missing authority to old writes. Creation and issue linking remain disabled in
specialist mode. General-mode Agent Sessions are also resolved through Linear and checked
against the configured app and issue scope; payload identity alone is insufficient.

## Migration and delivery limits

Existing chat/manual/old Kanban bindings, stopped work and uncertain deliveries remain visible
for reconciliation. The service does not automatically adopt, delete, close or replay them.
Keep their checkpoints, workspace/PR references and effect history. Reconcile them before
linking new native work. A reporting link is metadata, never a competing ownership lock.

Native create-task retry lookup is not an atomic exactly-once guarantee. A durable local
intent coalesces duplicate ingress; ambiguous in-flight creation stays pending for inspection.
Native comments have no idempotency key: the plugin retains ambiguous delivery and inspects
actual comments before retrying. Marker text is provenance, never authorization. Guarded
native controls reject stale observations. The integration does not interpret arbitrary
tracker prose as permission to resume stopped work. Linear Stop uses guarded native controls
when source ordering is verifiable, including initial queued work; conflicting controls remain
visible for native reconciliation. A blocked task or Stop acknowledgement is
not proof that an external process or effect ended; inspect native process/effect state.

Before status mutations/retries the bridge re-reads Linear so human reassignment/closure wins.
This revalidation does not provide remote compare-and-swap; conflicting or unverifiable
mutation responses remain subject to reconciliation.
The local outbox preserves UUIDs and uncertain external effects. Use the profile CLI:

```bash
hermes linear status
hermes linear resolve <row> applied|not_applied --evidence "verified remote outcome"
hermes linear replay <delivery>
```

Resolution records inspected evidence; it does not mutate Linear. Do not infer cancellation or
successful completion from a workflow label, an old comment or a lost response.

## Validation

`tests/test_linear_native_frontdoor_scenarios.py` exercises official producer → registered tool → Unix
socket → consumer boundaries via the manifest-verified `HERMES_AGENT_SRC`; the same suite is separately run against
the exact official snapshot above.
These are source/backend tests. Desktop, Telegram and Slack client round-trips and deployed
fleet behavior require separate live acceptance; source tests do not claim that evidence.

### Native Agent Session conversations

The optional `linear` gateway platform sends verified Agent Session assignments and
prompts through ordinary native authorization, conversation admission, model tools
and replies. It uses the existing profile inbox and scoped outbox. Current requests
can inspect stopped, scheduled or completed work and decide the authorized next
action; a status request creates no task. The model uses native Kanban controls and
the work-tracking skill for durable work and handoff.

Enable it through native platform configuration and the real permitted Linear user
IDs, with an explicit per-profile toolset:

```yaml
platforms:
  linear:
    enabled: true
    allowed_users: ["<authorized-linear-user-id>"]
platform_toolsets:
  linear: [linear, kanban]
```

This route requires Core support for durable provider input and typed structured
Stop. Capability checks leave it unavailable on older Core; ordinary plugin reads
and native reporting remain separate. No injection flag, shared credentials or
trusted-inbound authorization bypass is used. Creator-null automatic assignments
retain their native dispatcher provenance; the recipient app is never invented as
a human sender. Their coordination task carries an unknown requester and uses the
existing native assignment, contract and permission guards. A later authenticated
Activity may authorize its own conversation without changing the original creator.
Specialist reporting and queued content retain that exact Session/Activity pair;
each effect verifies the current provider actor, app, issue and organization again.

Provider retries preserve original actor, Activity, Agent Session, text and source
time. Inbox acknowledgment follows committed native acceptance, including exact
retry reconciliation. Structured Stop reaches native conversation control and the
linked task; an accepted control receipt does not prove physical cleanup. Replies
remain queued until the existing scoped sender confirms publication. Prior uncertain
effects stay held while unrelated authorized discussion continues.
Creator-null assignment acknowledgment requires a verified durable admission or
known original supersession from the existing native dispatcher. Held scope and
uncertain creation remain pending; retry never repeats an unknown create.
Stop acknowledgment also requires a verified task-control outcome, a durable unbound
Stop, or positive supersession evidence. Held or unknown controls retain the provider
delivery and original task/board/event/run coordinates; retries can recognize their
native receipt but cannot acquire a new task generation. A retained authorization
fence still holds task and content effects. Independently authorized status reads
and conversation replies remain available, with fresh source authorization on each
reply attempt; they do not clear the fence or replay its effects.

Source and component checks do not establish deployment or client acceptance. The
JTBD outcome cases still require actual Linear, Desktop, Slack and Telegram evidence.
The guarded finite-model task test does not establish a gateway-spawned actorless
worker or live client acceptance; that case remains a release acceptance check.
