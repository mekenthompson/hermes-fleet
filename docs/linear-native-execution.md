# Linear reports native Hermes work

Ordinary conversation, short commands and status questions require no Linear issue. When
work needs durability, use one native task and optionally link its human outcome with
`linear track`. An authenticated conversation can discuss or follow that task from another
surface without moving its executor or returning to an old chat.

| Before | After |
| --- | --- |
| Chat claim selected the conversation allowed to work or edit an issue. | Native identity and task scope authorize work; issue edits do not require a task link. |
| Transfer and resume phrases carried execution meaning. | Native task actions carry structured state; natural language is interpreted in the authenticated conversation. |
| Restart prose injected another chat turn. | Persisted native history feeds the reporting outbox; notifications grant no execution. |
| Plugin retirement inspected and controlled worker processes. | Native worker claims, run IDs, recovery, Stop and terminal reaping remain authoritative. |
| Separate chat and task execution paths could disagree. | A binding records native profile, board and task for reporting, with no competing executor lock. |

```mermaid
flowchart LR
  Chat[Authorized conversation] --> Native[Native task controls]
  Native --> Worker[Native dispatcher and worker]
  Linear[Verified Linear request] --> Parked[Blocked native task]
  Parked --> Native
  Worker --> History[Persisted native events]
  History --> Outbox[Durable reporting outbox]
  Outbox --> Issue[Linear outcome]
```

## Upstream boundary and reduced automation

The plugin targets the public API at official Hermes
`ff294849385e433dd40a993b4f07673abb95c6f5`. The aligned fork release is
`5282b788113aef959925397d05252d883cd7c4be`; the plugin does not require the fork's
invocation-context or profile-service APIs, approval helpers or lifecycle extensions.

Validated Linear assignments create tasks **blocked**, with no automatic unblock. Native
controls perform activation from an authorized surface. Native creation has no atomic
create-and-cancel contract, and unblock has no expected-event guard. A state read followed
by automatic activation could undo Stop. The parked request deliberately preserves that
safeguard rather than introducing another control protocol.

Running cancellation is relayed only with verified ordering and a concrete native run ID.
Cancellation of a queued or otherwise unverified run remains a visible reconciliation request
for native controls; it is not an effective Stop. A blocked state or an acknowledged request
does not prove external processes or effects ended. Native process observation and terminal
reaping remain necessary. The plugin never kills a PID or clears revoked authority.

These are explicit capability limits: automatic execution from a Linear assignment and
complete queued cancellation are not implemented. Existing native task creation and active
linked follow-ups retain their native behavior.

## Delivery and migration

The existing metadata/outbox store records native delivery attempts before a call. Duplicate
ingress coalesces to one attempted create or comment. A lost response is reconciled against
actual native state; a missing or ambiguous result stays visible without blind replay.
The reporting cursor and its remote intents commit together. Native notification cursors are
not consumed to stand in for that commit.

Human issue edits, closure and reassignment are inspected again at remote mutation boundaries.
Status intents retain the source event time so delayed catch-up cannot overwrite a later
human decision. Reporting failures queue in the outbox without controlling native execution.

Legacy chat bindings, admissions, transfers, retirement records, pending Stop requests and
unknown remote effects stay inspectable. They are not automatically adopted, deleted or
replayed. Preserve their workspaces, checkpoints, PRs and original effect history. The operator
view names required reconciliation. Unlinked issue operations retain app/workspace/specialist
authorization independently of those old execution claims.

## Evidence boundaries

The native scenario suite exercises upstream task creation, claiming, lifecycle and comment
consumption; the registered tool and private socket are exercised with actual upstream session
producers, including a separate API process. Cases cover concurrent and duplicate requests,
lost replies, parked creation during Stop, stale run rejection, reporting restart, human edits,
outages and specialist assessments. The source is also exercised against the pre-release core
to establish bundle staging compatibility.

Retired tests asserted chat claims, transfer ceremonies, restart injection and plugin process
retirement. Their replacements assert preserved legacy data, no automatic adoption or replay,
native bindings, immutable delivery attempts and ordinary unlinked operations. Credential,
workspace, requester, actor, content readback, project batching and uncertain-effect tests
remain in the suite.

These tests do not prove authenticated Desktop, Telegram or Slack client round-trips or the
end of background external effects after Stop. Those require separate deployed acceptance.
Passing tests, merged source, published artifacts, deployment and live UX are distinct results.
