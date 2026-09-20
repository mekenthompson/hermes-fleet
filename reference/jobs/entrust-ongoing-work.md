---
title: Entrust ongoing work without losing control
job: entrust-ongoing-work
serves: there-when-you-reach
outcome: A principal can leave approved work with an agent, steer it from a supported interface, and receive a verified result after interruptions without reconstructing progress or authorizing the same work twice.
stakes: full
invariants:
  - no-shared-credentials
  - one-agent-change-is-local
  - built-published-deployed-are-different
  - no-off-plan-model-call
  - public-image-has-no-deployment-identity
---

# Job Spec: entrust ongoing work without losing control

## The job

**Struggling moment:** When ongoing work outlasts a conversation or the agent restarts, the principal is stuck reconstructing what happened, whether anything is still running, and which instructions still apply.

**Job story:** When I entrust an approved outcome to my agent, I want to observe and steer that same work wherever I reach it, so I can leave execution to the agent without losing control or repeating decisions.

## Today's alternatives

Today the principal follows separate chat threads, manually translates progress into a tracker, restarts interrupted work, or remains present to prevent mistakes. The bar is less supervision without losing the direct control those workarounds provide. A second mandatory interface to maintain would not beat that alternative.

## The bet

This job assumes principals will delegate ongoing work when its ownership, decisions, progress, and completion remain trustworthy across interruptions and interfaces. If they still prefer to supervise every step after those properties are proven, the proposed autonomy is not the progress they hired.

## Measures of success

Mechanism: durable ownership and reconciled progress let the principal leave execution without reconstructing context. Leading indicators: work resumes without human reconstruction; steering changes the existing work rather than creating a competing execution; delivered results match the agreed destination.

The job metric is the share of interrupted approved outcomes recoverable without reconstruction or duplicate effects. It rolls into **there-when-you-reach** and its first-class surface hold rate, supporting trusted unsupervised turns. Baseline and target are unmeasured; instrumentation belongs in the RFC.

## Good / bad

**Good looks like:**
- The principal can see what is owned, progressing, waiting, or complete without asking each worker.
- A decision made through one supported interface governs the same work reached through another.
- Independent work progresses together while genuine dependencies and permissions remain enforced.
- Interrupted work recovers safely; stopped work stays stopped; uncertainty becomes an actionable blocker.
- Completion includes evidence that the requested result reached its intended destination.

**Bad looks like:**
- Parallel workers duplicate ownership, overwrite each other's changes, or turn retries into repeated external actions.
- A second interface starts another executor instead of steering the existing one.
- The principal must move every internal task or approve routine steps already within the agreed remit.
- Agents expand maintenance into unapproved product work or spend outside granted model routes.
- **Silent failure:** the record says complete, but the result was not delivered, the cancellation was lost, or the work never resumed after an interruption.

## What the job requires

**Must be able to:**
- Preserve an accountable owner, authorized scope, decisions, evidence, and a recoverable next action for ongoing work.
- Reconcile actual execution and external effects before continuing after interruption.
- Apply steering and stop requests to the owned work and its descendants without exceeding permissions.
- Expose meaningful progress and exceptions while retaining detailed internal evidence.
- Verify results at the destination agreed when the work began.

**Won't:**
- Require the principal to manage internal worker bookkeeping.
- Turn a saved record or a passing component test into a claim of recovered or delivered work.
- Share credentials or collapse independently isolated agents into a common execution identity.
- Infer authorization for new scope, privileges, or spending from task persistence.

## Prove it

**Evidence status: not yet outcome-validated.** No runnable outcome scenario has been established for this job. The following are required acceptance cases, not existing harness names or passed tests. The [delivery RFC](../rfcs/durable-work-execution.md) owns implementation and evidence links; it must replace these gaps with real paths and results before capability completion.

- **Entrust work × Slack, Telegram, and Desktop, individually:** start approved work, steer the same owned item, interrupt its worker, and observe safe recovery and a verified result without reconstruction. Protects `no-shared-credentials`, `one-agent-change-is-local`, and `built-published-deployed-are-different`. All surfaces: to validate.
- **Entrust work × configured work tracker:** observe the same owner and material milestones, delay delivery, and verify catch-up without relaunch. When no tracker is configured, this optional delivery path is not claimed. Protects `built-published-deployed-are-different`. To validate.
- **Stop work × each claimed interface:** stop active descendants, restart the owning runtime, and prove stopped work remains stopped. Protects `one-agent-change-is-local`. To validate.
- **Silent-failure challenge × completion and restart:** interrupt after a consequential action but before acknowledgement; reconcile or block rather than repeat it or claim success. Protects `built-published-deployed-are-different` and `no-off-plan-model-call`. To validate.

Fuzz dimensions: duplicate and out-of-order instructions, notification outages, stale ownership, process death, permission denial, ambiguous remote outcomes, shared-resource conflicts, and missing recovery evidence. Assertions must include unaffected neighbouring agents and unchanged credential boundaries.

## Verdict

Done when the principal can leave approved ongoing work, steer or stop it through every claimed interface, and receive the verified destination result after interruption without reconstructing progress, duplicate consequential actions, or lost control. Each claimed surface needs its own outcome evidence.

## Abandon signal

We named the wrong job if trustworthy continuity and control are available but principals still reject leaving execution to the agent because their desired progress requires continuous participation.

## Production-readiness

Bound resource admission and retries; preserve evidence through cleanup; contain changes to the named agents; retain stop decisions across restarts; fail closed on uncertain ownership or side effects. Do not silently broaden billing routes. Recovery proof must include usable tools, not only surviving data.

## Related

- [Product spec](../product-spec.md)
- [Delivery RFC and ratification record](../rfcs/durable-work-execution.md)
