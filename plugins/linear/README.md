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
| `origin_reference` with no other fields | Obtain the current ordinary native admission reference for independent owner inspection. Grants no permission. |
| `followup` with `issue`, `body`, UUIDv4 `id` | Add an operator instruction to active linked native work. Stopped and completed work requires a current native action. |
| `delegate_self` with `issue`, UUIDv4 `id`, `expected_binding_revision`, `expected_delegate_id`, `expected_updated_at` | Route future Linear input to this app after coordinating existing native work. Requires an unfinished native task already linked to this executor. Preserves the human assignee and all native execution state. |
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

For an explicit chat takeover, first inspect the current owner and native work, coordinate
its Stop and verify cleanup, then link the existing task or a deliberately created remainder
on the receiving agent. Read `status` and pass its exact binding revision, delegate ID
(including null) and issue timestamp to `delegate_self`. Confirm Linear delivery before
using current native controls to resume the remainder. Assignment echoes see the existing
binding and cannot start a second task. Delegated children return routing requests to their
parent. Agent routing metadata neither proves the old worker stopped nor grants execution.
The plugin uses its own app identity and writes only `delegateId`; it never borrows another
agent's credentials or changes the human assignee. The timestamp check is optimistic,
because Linear offers no remote compare-and-swap for this update.

Routing requests use the existing outbox and retain their UUID and preimage across restart.
Reuse the exact request after a lost response: fresh desired-state readback can settle it
without another mutation. A different remote delegate after an uncertain attempt holds the
request for the existing `hermes linear status` / `resolve` workflow. It never resends blindly
or clears an earlier uncertain effect. A newer request cannot bypass that hold.

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

## Native origin evidence

The registered `linear` tool accepts `{"action":"origin_reference"}` with no other fields.
It captures the current host-bound caller, resolves its native compression lineage, and
checks its ordinary active admission with the existing gateway owner. It returns a reference
and `permission_granted: false`; it does not contact another agent or transfer execution.
The reference contains the native-generated admission ID and profile/owner/epoch/generation/
scope binding only. Arbitrary session and request IDs stay inside the owner, where the
read-only admission getter resolves them. Work references use the same minimal coordinates.
This lets native A2A privacy redaction remain enabled even for explicit API IDs containing
addresses or credential-shaped text; neither requester principals nor original input leave
the owner in a cross-agent reference.
Delegated children, bots, automation, ambiguous steering, unknown execution and stale owners
cannot supply this reference. Unsupported native hosts fail closed.

Provider SDKs can invoke the registered synchronous tool from their own asyncio loop.
Origin and work evidence use Core's existing bounded coroutine bridge on that path,
preserving the native profile and turn context while the gateway owner answers receipt
checks on its separate loop. The owner CLI keeps its ordinary synchronous entrypoint.
This changes neither the reference contract nor authorization, Stop or current-owner checks.

An already-authorized operator can independently inspect that reference **inside its owning
profile**, using that profile's own mounted plugin and UID:

```sh
hermes linear inspect-origin --reference-json '<reference object>'
```

This read-only command returns only the exact current request's original text, native
principal, filtered requester limits and relevant profile/workspace policy. It accepts no
other profile home, database path or transcript selector. It never attaches a viewer, loads
runtime tool inventory, initializes credentials, starts a gateway, or runs a model.
API credential principals remain API credential principals; they are not attributed to a
named human. Inspection repeats the exact requester, text, workspace/policy, scope,
Stop evidence and applicable provider verification after owner readback; a changed
projection is refused. Recheck current native state before any subsequent control operation.
Consumed admission payloads, native messaging source/event objects and turn/settings
envelopes pass the existing native mapping validator before field access. Optional native
author decoding and absent/null optional settings retain their existing semantics; malformed
arrays and scalars fail as unavailable evidence rather than escaping the owner command.

Ordinary native Linear input uses a separate, bounded evidence branch. It checks
the committed human actor, organization, issue, Agent Session and original
provider message ID with gateway command/control flags disabled. Bot, automated,
actorless and typed Stop inputs cannot become human-origin references. If an
already-running scoped `BoundLinearAPI` is available, the edge freshly verifies
the original Activity author (or the assignment Session creator), current app,
issue, organization and specialist limits through its existing read methods.
A later allowed human Activity can verify independently of a missing assignment
creator. The shared `requester_session(..., activity_id=...)` context binds only
exact provider IDs; each read or effect retry freshly checks that Activity, and
never rewrites the original creator or grants permission. Without a current
Activity binding, an unknown session requester stays unknown.
It never constructs an API client, initializes a credential source or starts a service.

The result states `provider_verification.state: verified` or `needed`. A later
owner CLI process normally has no existing provider API and reports `needed`;
an earlier gateway check does not establish later provider freshness. Missing
original provider IDs also report `needed` rather than inventing a selector.
Such evidence cannot by itself support cross-owner replacement. Ordinary reads
and existing native Telegram, Slack and local origin inspection remain usable.

References and inspection results grant no permission, prove no worker stopped, and establish
no new cross-profile route. A peer may communicate a reference as untrusted input; an operator
must independently inspect its original instruction and apply existing requester, profile,
specialist and workspace permissions. Native Stop and quiescence verification remain separate.

For an already authorized Kanban task that continues after its original chat turn ends, the
registered tool separately accepts `{"action":"work_reference"}` with no other fields. Its
worker-only capture requires native worker registration, the actually bound session ID and
the dispatcher-owned process; it does not require or fabricate a chat platform or requester.
The current task/run/claim coordinates come only from the owner's frozen Kanban policy.
Native Kanban workers see only `linear_work_reference({"action":"work_reference"})` in the
Linear tool inventory. The ordinary `linear` action remains compatible for existing callers.
Inventory follows the native launch policy at tool selection; environment hints grant nothing.
Workers execute and report through native `kanban_complete` summary and evidence. Specialist
agent-session assessments additionally require `metadata.issue_comment` matching their assessment
contract; ordinary completion reporting does not publish an arbitrary value from that field.
The existing gateway outbox publishes the native completion report. A coordinator with a
native chat session checks remote issue and project publication. Queued reporting does
not establish publication or prevent the worker from completing verified local work.
Ordinary chat coordinators retain the full Linear tool inventory and native session checks.
This selection relies on the native managed-worker bootstrap: plugin registration precedes
policy binding, and the first model tool inventory is read inside the immutable launch
`policy_scope`. The availability predicates bypass the registry's TTL cache. Core's outer
quiet-mode inventory memo does not track policy binding: a caller that reads that inventory
before binding policy can retain the ordinary schema afterward. Arbitrary policy changes
within one interpreter are not supported by this plugin-only change.
An authorized operator can inspect this typed reference inside the same owning profile:

```sh
hermes linear inspect-work --reference-json '<reference object>'
```

The projection verifies the exact active native admission, epoch/generation, current running
task and run, unexpired claim, assigned profile, owner/worker bindings and frozen workspace,
branch, tenant and skills. It uses native tracked read-only SQLite transactions. It returns
only this task's bounded instruction and scope; raw claim tokens, DB paths, unrelated cards,
context and transcript history are excluded. A successor run needs its own fresh reference.
Stopped, settled, superseded, expired or changed work invalidates an earlier reference.

Work evidence reports `requester.kind: native_dispatcher`, `original_requester: unknown` and
uses the existing JSON-object decoder for frozen policy/request data and consumed claim,
admission and worker-binding event metadata. Native absent/null event metadata retains its
empty-object meaning; required ownership fields must still verify independently. Malformed
mapping shapes are refused on both reads without repairing retained state. It returns
`permission_granted: false`. Native cards do not bind the original human/API principal or an
approved issue scope. Generated task text is work content; it is never human consent. Any
controller must apply its independently authorized profile, issue, specialist and workspace
limits before effects. These commands do not create a controller route or execute a takeover.

Both owner inspections return `stop_evidence`. Where the existing native
`process_stop_pending` getter is supported, a pending checkpoint refuses inspection; a clear
checkpoint is labelled `clear`. Older compatible cores report `unknown`, never false. In
particular, a direct Stop can remain in flight while an older core's receipt is still started.
Neither a reference, a receipt nor a clear checkpoint proves physical quiescence. A controller
requiring known Stop evidence must reject `unknown` before effects and separately verify
checkpoint, native Stop, settlement and process quiescence using its authorized control path.

For the stopped task's final cleanup observation, an already authorized operator can run
the owner-only command inside the old work's fixed approved deployment:

```sh
hermes --profile default linear inspect-cleanup <issue-id> --expected-stop-event-id <stop-event-id>
```

It derives the board/task from the current saved native reporting link. If the link is absent
or retained legacy data, it inspects existing native history using the same issue-matching
rules as ordinary discovery, without creating or adopting a reporting link. Optional
`--issue-json '{"id":"...","identifier":"...","url":"..."}'` supplies independently read
issue metadata for explicit marker matching; it is discovery data and grants no authority.
Ambiguous, foreign, archived, unreadable or scope-fenced results remain unknown. Existing
saved links never skip native discovery across retained boards. Another matching executor,
including archived work, holds proof. The selected board's strict SDK history, current task
issue-scope check, predicate-key preflight and native event decoding share one tracked read
transaction. An explicit conflicting issue marker stays unknown even when a saved link or
idempotency key selects that task. Marker-free
authorized manual links and repeated matching canonical ID/identifier aliases remain valid.
Native-normalized owned keys must agree with SQLite's actual task/run predicates before filtering; a readable
BLOB alias is unknown rather than an omitted attempt. The checks preserve raw SQLite
equality/order and unrelated readable foreign records, without repairing any keys.
No filtered-out opaque key or conflicting event/run ownership is positive foreign evidence.

The optional host namespace-disposal transport is the existing protected ingress directory's
`namespace-dispositions/<sha256(epoch UTF-8)>.json` sibling directory. Every ancestor must be
root-owned, nonwritable by group/others and nonsymlink. The receipt directory is `root:1000`
mode `0750`; each single-link regular receipt is `root:1000` mode `0440`, opened without
following symlinks and limited to 16 KiB. Neither the group-writable ingress SQLite bytes, plugin metadata, model
arguments nor an arbitrary file can provide this proof. No new mount or credential is needed.

Version 1 has exactly these fields (all identifiers below are synthetic):

```json
{"version":1,"kind":"pid_namespace_disposed","deployment_profile":"specialist","native_profile":"default","home":"/opt/data","home_dev":123,"home_ino":456,"epoch":"00000000-0000-0000-0000-000000000001:12345","container_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","host_pid1":23456,"pid1_start_ticks":12345,"pid_namespace_inode":34567,"host_pid_namespace_inode":45678,"pid_mode":"private","old_instance_disposed":true,"pid1_gone":true}
```

The host producer captures the selected old service/container, its live PID1 birth and private
PID namespace, and the declared profile's actual bind-volume identity **before** stopping it.
The host and old container must agree on `/opt/data` device/inode. It publishes atomically only
after proving that exact old host PID1 birth gone under isolated PID mode. The old container
may be absent, present stopped, or restarted with a distinct new PID1/private namespace and
consistent profile/home binding. Contradictory Docker/kernel state, a name change, failed
inspection or unreadable identity is insufficient. Gateway-only restart keeps PID1 and needs no receipt.
Linux terminates a PID namespace's remaining processes when its init exits and prevents new
processes entering that dead namespace; see [pid_namespaces(7)](https://man7.org/linux/man-pages/man7/pid_namespaces.7.html).
It never overwrites a conflicting receipt. The epoch's final integer equals `pid1_start_ticks`;
the two namespace inodes must differ. Integer fields exclude booleans; `home_dev` is nonnegative,
other integer identities are positive. `old_instance_disposed` and `pid1_gone` are literal true.

The consumer binds the protected inbox/deployment profile, current native profile and canonical
home, actual current home device/inode, and exact retained worker epoch. It still performs all
current Stop, admission, scope, descendant and effect checks. A namespace disposition proves
only physical fate; it cannot mark an external effect successful or authorize more work.
Legacy observations made before this producer existed are not automatically imported or backdated.

`quiescence_proven: true` requires the exact current blocked `needs_input` Stop, no current
claim, matching frozen owner/run/admission/executor records for every retained task attempt,
terminal native admissions with recognized settled outcomes and worker/child ledger,
confirmed clear native Stop checkpoints, and recorded interpreter and
submitter identities proven gone or recycled. A native Stop on an unclaimed ready or Review
card can also record a control-only blocked run. The inspector lists it in `control_only_runs`
only when its exact zero-duration row has no step, claim, lease, process, heartbeat, runtime, metadata or error fields
and has exactly one matching native block event, with no execution events. Such a row
does not replace an executed attempt's admission or worker evidence. Any other row stays
subject to the full execution checks, including missing or ambiguous owner evidence.
Admission and Stop checks cover the native
compression lineage of every attempt, every retained execution in that scope and all of
their descendant session lineages. The read repeats that discovery until no new session or
execution remains; terminal extra compute roots participate too. Before native lineage resolution, only
the relevant local receipt (or its chat-linked receipt) and explicit reset-child mapping
metadata are decoded in the same pinned snapshot. Native helpers retain topology and
semantic validation. Those consumed inputs join the repeated digest; unrelated model
configuration and foreign receipts remain outside this check. A settled parent does not
discharge queued or unknown child work. Those session identities, admissions and every
retained worker identity/state participate in the repeated evidence digest, including
settled arrivals during the owner read. Every consumed owned worker field must satisfy
its native schema and writer contract, including kind, counters and referenced session.
Historical terminal epochs/generations and terminal sequence zero remain valid; malformed
retained records remain unknown. Selected admission records use the native identity and
JSON validators before primary payload field access, recognized intents and native counter
contracts, preserving resultless controls and nullable legacy terminal metadata. Frozen
Kanban context and saved binding metadata pass the existing JSON-object decoder before
field access; arrays, null and scalars stay unknown without state repair. The native worker/admission physical-lineage
join also checks for started or unknown claims whose logical target differs. Unrelated
foreign payloads remain outside these checks. Runtime ownership selectors must retain
native TEXT types before ownership filters; BLOB or other ambiguous selectors report
`native_runtime_ownership_key_unreadable` for native-owner reconciliation. Execution/session
IDs in the runtime worker ledger and admission targets use the native runtime identity
validator, including its empty-value and length rules. Broader SessionDB/metadata text fields
and optional empty/NULL values retain their own writer contracts. Values are never stripped,
normalized or decoded into identity or authority. Only fixed selector columns are checked; valid foreign
TEXT scope and unrelated payloads remain outside the check. The native read snapshot is
closed before owner RPC and read again afterward. A terminal label with a missing, unknown or
unrecognized outcome remains held; native cancelled and settled steering histories do not
clear any other run, worker, scope or Stop obligation. Worker births use the actual current native
instantiation epoch and native birth validation/comparison. Positive reuse of a live PID
also requires this exact attempt's fully matched original native `owner_admitted` PID and
`worker_birth` receipt. A legacy fingerprint cannot distinguish historical Linux boot ticks
from a clock-adjusted epoch fallback. Without a trustworthy original monotonic witness,
a living legacy PID stays unknown even if its current fingerprint differs; a genuinely
gone PID remains positive evidence. Current native births and identity are read repeatedly;
unreadable or changing evidence cannot prove reuse. Unknown run states remain unsettled
even when an end timestamp exists. Missing, malformed, unreadable
or unproved older-epoch witnesses stay unknown for native-owner reconciliation. Scoped process checkpoints must also contain
no live or unconfirmed scope. A retained checkpoint symlink, including a dangling link,
or unreadable metadata stays unknown; only genuine absence is empty. Duplicate JSON
keys are rejected before checkpoint ownership classification.
Only positively foreign records are excluded;
missing, partial or conflicting route/owner attribution remains unknown. An attempt that
spawned before native admission, orphan worker events, or any unverified older attempt also
remain unknown; a later admitted attempt cannot hide them. Any attributed retained process checkpoint,
including a legacy host process or native job scope, stays unknown even if its leader is gone
or recycled or an affirmative-looking exit file exists. Only the native owner can retire
that checkpoint after scope cleanup, reader finalization and durable ownership/output settlement. Reconcile through that owner's existing native
Stop/recovery path; this read-only command never performs retirement. Reads use native tracked SQLite
transactions, never database copies. Receipts and the entire scoped observation are checked
again before returning. A newer Stop, unblock, late worker binding, owner change or changed
reporting link invalidates the old observation. Reinspect immediately before later effects.

Unknown evidence returns `quiescence_proven: false`, `state: unknown` and exit code 1. This
includes a missing old-core Stop getter, missing worker identity or executor settlement,
pending cleanup, queued/unknown admission or inaccessible history. A Stop ACK, receipt or
second successful `kanban block` alone is insufficient. Output excludes transcript contents,
claim tokens, process commands and unrelated records. This command is not an agent-tool action,
does not stop/recover processes or mutate state, and always returns `permission_granted: false`.
The pinned older-core tests label corrective getter adapters explicitly; they are not proof
of deployed corrective-core behavior or production takeover acceptance.

The isolated source proof is `python scripts/run-linear-origin-scenarios.py --agent-source
<clean manifest-pinned Agent checkout>`. Its guard is installed before native imports and
rejects credential/model activation, external processes/network, real-user config access and
writes outside its temporary root. This source proof does not establish deployed client or
handoff readiness.
