---
title: Durable work execution
serves: entrust-ongoing-work
status: accepted-direction-implementation-unverified
---

# RFC: durable work execution

## Decision record

### Context

An operator alignment interview on 2026-09-20 ratified the job and operating boundaries below. A subsequent MoA planning pass proposed a native-first delivery sequence. These are requirements and a plan, not evidence of shipped behavior. Interview evidence is single-source and directional. This record introduces the new Job Spec; it does not retarget an existing protected job, outcome, principle, or invariant.

### Decision

Linear is the primary human-facing work record and cross-gateway handoff interface for this delivery. Gateway-local Kanban owns durable execution; delegates provide bounded, ephemeral assistance. The Linear integration remains optional and default-disabled in the public distribution. Existing chat-only installations do not acquire an unconfigured tracker dependency; Linear-backed coordination is claimed only when configured.

Start with one profile per gateway. Multiple specialist profiles are a future-compatible option, not a prerequisite or part of this delivery. A repository is a persistent workspace; delivery projects are finite scoped outcomes. Several projects may use one repository without sharing writer workspaces.

The principal approved autonomous end-to-end execution of authorized objectives within standing permissions, including publication, merge, and deployment where granted. Approval of a plan alone does not start implementation or authorize disruptive tests. New scope, cost, privileges, and uncertain recovery require escalation.

### Consequences

A gateway needs one accountable execution owner per deliverable and durable links among tracker item, chat origin, local task, run identity, and Git evidence. The existing integration must implement these semantics or explicitly block unsupported cases. This RFC does not authorize a new scheduler, distributed queue, dashboard, standalone ledger, or cross-container shared board.

Private rollout identities, repository assignments, budgets, and private deployment tracker URLs belong in the deployment overlay or private tracker, never this public tree.

## Operating contract

### Ownership and interfaces

- One meaningful deliverable per tracker issue; sub-issues for independently reviewable outcomes or cross-gateway handoffs, not every tool call or delegate.
- Ongoing work with several milestones is a finite scoped project, not an oversized issue or an endless repository catch-all.
- A chat-created item records the current owner without creating another executor. A tracker-origin assignment may start work. Follow-ups steer the owner rather than relaunching it.
- The owning gateway manages local cards and dependencies. Cross-gateway handoffs use linked tracker outcomes, retaining original accountability for the overall result and receiving ownership of the handed-off deliverable.
- Current human edits and newer stop decisions must not be overwritten by stale snapshots or delayed events. Verify the integration's revision and ownership handling before claiming cross-surface synchronization.

### Parallelism and workspaces

- Independent authorized issues and projects run concurrently within an explicit per-gateway budget covering workers and delegates. Priority affects admission; genuine dependencies and resource conflicts require serialization.
- Separate worker and delegate caps are not proof of a shared bound. Inspect launch paths and enforce a conservative aggregate bound or report the missing mechanism. Numeric defaults remain deployment proposals until validated.
- Keep delegation flat initially. Delegates receive bounded context, scope, acceptance criteria, and evidence expectations. Their material results are persisted to the owning card/repository and surfaced through the owner.
- Parallel coding writers require successful workspace isolation and approved base ancestry. Automatic worktree failure must not silently permit shared writes; uncommitted parent changes are not assumed to appear in child worktrees.
- Integration operates on the actual reviewed commits. Review must be performed by a separate fresh-process reviewer, never the author. The reviewer may use the same profile's configured identity and granted provider route; distinct credentials or personas are not required. A separate session qualifies only if it satisfies the fresh-process boundary.

### Authorization, maintenance, and completion

- Each repository has an explicit maintenance remit. The initial narrow remit permits failing-CI and broken-test repair without weakening coverage or security checks. Dependency/security updates are proposals; applying upgrades, features, broad refactors, and schema changes needs an approved objective initially.
- Routine steps inside granted scope do not introduce stage-by-stage human approvals. Required checks, permissions, and independent review still apply.
- Establish destination-specific acceptance at the start. Merge-only work needs the merged result and required checks; deployment work needs deployment and a live check; investigation work needs findings and evidence delivered. An open PR is not deployment or completion.
- Preserve configured provider routes and credential boundaries. Do not introduce fallback spending or new personas as a side effect of enabling delegation.

### Recovery and cancellation

- Persist progress, decisions, canonical work identity, branch/commit/worktree references, evidence, and next action at meaningful checkpoints. Transcripts alone are not a handoff.
- After restart, reconcile ownership, existing processes, repository state, PR/CI state, and external actions before resuming authorized work. Do not assume every running record is dead or blindly replay consequential actions.
- Persist pause/cancel before stopping descendants and reconciling them. A restart must not reactivate intentionally stopped work. Report any unresolved descendant explicitly.
- Use bounded recovery attempts. An ambiguous action outcome, uncertain ownership, or repeated recovery failure becomes a visible blocker, not unlimited retry.
- Retain recoverable Git work and durable evidence before workspace cleanup. Backups and mounted databases do not prove usable recovery by themselves.

### Reporting

The work tracker receives meaningful milestones, blockers, review outcomes, and completion evidence. The origin chat receives acknowledgement, significant changes, decisions, final result, and bounded periodic updates during long work. Routine tools and individual delegate completions remain internal unless material.

Notification recovery must replay unsent durable milestones without rerunning completed work or mirroring every message to every interface. Interval and retention policies remain deployment decisions; no unsupported delivery guarantee is claimed.

## Delivery sequence

Acceptance of this RFC does not itself authorize implementation execution, disruptive testing, publication, deployment, or runtime rollout; those require separately granted execution scope.

1. **Reconcile, read-only:** identify existing work owners and projects; inspect boards, launch paths, profile resolution, mounts, workspaces, integration state, approvals, and evidence. Do not migrate or take over existing tasks by inference.
2. **Specify the minimal delta:** reuse native features; map each unmet guarantee to an existing component, test, and owner. Fix only demonstrated gaps. Keep independently buildable outcomes parallel after the shared reconciliation.
3. **Verify components:** separate unit behavior from cross-component behavior. Test duplicate/out-of-order events, ownership fencing, isolation failure, cancellation, ambiguous effects, evidence retention, and notification catch-up.
4. **Prove a bounded pilot:** one authorized deliverable, then concurrent scoped projects within the same repository. Live interruption and publication/deployment canaries require their own explicit execution scope and safe recovery path.
5. **Expand in bounded batches:** apply the common contract with domain-specific permissions and acceptance. Confirm neighbouring gateways did not change unexpectedly. Preserve task ownership throughout rollout.

## Capability and proof gaps

All outcome evidence below is **to validate**. Source inspection and historical audits may narrow implementation work but cannot mark these cases passed. The implementing change must add exact test commands/paths and real evidence before claiming the capability.

| Requirement | Required evidence |
| --- | --- |
| Single owner across chat and configured Linear | Duplicate assignment and follow-up do not spawn competing execution; current instructions win |
| Bounded parallel work | Independent tasks overlap, dependencies gate, and observed aggregate activity stays within the approved bound |
| Safe workspaces | Expected commit ancestry; isolated writers; failed isolation blocks unsafe work |
| Durable resume | Interrupted authorized work reconciles actual processes, Git and remote actions before continuation |
| Persistent stop | Active descendants stop or remain explicitly unresolved; restart does not undo pause/cancel |
| Ambiguous external effects | Crash between action and acknowledgement causes reconciliation or blocker, not blind replay |
| Delivery catch-up | Lost notifications recover without duplicate work or repeated milestone spam |
| Evidence survives cleanup | Commits, artifacts, findings and checkpoint references remain available |
| Honest completion | Fresh review, required CI and intended destination verification support the Done claim |
| Surface parity and isolation | Every claimed chat/tracker surface passes separately; unrelated gateways and credentials remain unaffected |

Proposed numeric ceilings, provider behavior, trace completeness, and restart success remain unmeasured until executed. Component-green is not outcome-green, and neither is production-readiness.

## Rollback and operability

Stop new admissions, persist the stop decision, reconcile active workers and external actions, then revert only compatible policy/code where authorized. Preserve current database volumes, links, Git evidence and artifacts. Do not overwrite recent progress with an old image or blindly restore an old database. A source revert does not reverse an external merge or deployment; those need their own authorized recovery.

Operators must be able to identify the exact failed boundary and next action from the owning work record. Unresolved gaps stay visible; they do not become silent best-effort guarantees.

## Product contract checks

- **Outcome:** advances `there-when-you-reach` through continuity and the linked job; control also supports `on-a-leash` without changing its definition.
- **Principles:** agents do the work, humans grant scope; fewer surfaces to supervise; native mechanisms before new machinery; evidence over success claims.
- **Invariants:** no shared credentials, no neighbouring-agent disturbance, no conflation of built/published/deployed, no off-plan model calls, no private deployment identity in this tree.
- **Not shipped:** this accepted direction does not assert an implemented capability or add default-enabled integration behavior.
