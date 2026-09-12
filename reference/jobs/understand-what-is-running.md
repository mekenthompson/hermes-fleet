---
title: Understand what is running
job: understand-what-is-running
serves: on-a-leash
stakes: full
invariants:
  - built-published-deployed-are-different
  - one-agent-change-is-local
  - public-image-has-no-deployment-identity
  - no-shared-credentials
  - no-off-plan-model-call
last_reviewed: 2026-09-12
---

# Job Spec: understand what is running and whether the intended change actually took effect

A durable Job Spec. How we surface live state lives in an RFC that `serves:` this job. That artefact churns. This job does not.

`serves:` retargeted from `change-recoverable` to `on-a-leash` by [../rfcs/successor-vision.md](../rfcs/successor-vision.md).

## The job

An operator has just changed one agent. They need to know what each agent is actually running, and whether the agent they meant to change now has that change. They are not asking for a dashboard. They are asking for an honest read of live state that a person who did not make the change can repeat.

Struggling moment: When a change is "done," the operator is stuck because source, published artefact, docs, and the running agent can each tell a different story, and the person who made the change is the one declaring success.

Job story: When I change one agent, I want to read what is live on the target and on the others, so I can know the intended change took effect and nobody else moved.

## Today's alternatives

Today they: inspect the live host themselves, read whatever explanation shipped with the change, trust the person who made it, or use a private layout they already know.

The bar: those work when the operator already knows where to look, and when they remember that published is not deployed. They fail when the explanation drifts from the live agent, when stored files survived but a tool no longer does its job, or when the author grades their own work. Switching cost is giving up a private read that already knows the household.

## The bet

This job assumes: operators cannot trust intended-change hold rate without an independent live read that distinguishes built, published, and deployed, and that names whether the target matches intent. If that's false, don't build: if stock tools already answer this honestly for a Fleet layout, this job is documentation, not a product surface.

## Evidence & confidence

- Operator interview 2026-09-12: first job chosen because live intended-change cannot be scored without it. Successor interview kept the job and moved it under on-a-leash.
- Public product already treats publication and rollout as different claims. That supports the distinction. It does not independently prove operators conflate the two.
- Private overlay work on drift exists. It was not reviewer-readable from the public tree, so it is not counted as verified evidence.

Confidence: 2 (directional) for the struggling moment. 2 for the shape of a public, topology-free proof. Not 3: one interview plus public docs, no independently readable observation of the claimed operator behaviour.

## Measures of success

Mechanism: after an intended change, a fresh-process reader can state what the target is actually running, whether that matches intent, and that in-scope neighbours still match their last intended state, including that tools still do their job.

Leading indicator: share of intended changes where that read is possible without asking the author.

Where the number lives: rolls into on-a-leash's intended-change hold rate (product spec), the leash signal under TUTR.

## Good / bad

### Good looks like

- The operator can answer, without guessing: what this agent is running, which capabilities are on, and whether the last intended change is present.
- Built, published, and deployed are reported as different claims. A skipped publish is not a failed deploy. A published artefact is not a running agent.
- Neighbours are part of the read. "A has the new state" includes "B did not move."
- Persistence proof includes working tools: the path that will actually run, not only that stored files still exist.
- A reviewer who did not make the change can repeat the read from the artefact.

### Bad looks like (never ship this)

- Status "healthy" or "latest" with no live identity and no intended-change comparison.
- Docs, workflow, and live state disagree, then the docs are edited to match whatever shipped, without a policy decision.
- Unit-green or a published artefact reported as the agent running the change.
- A new ledger, panel, or score instead of a repeatable read.
- Silent failure: the read is green, the agent is up, and the tool the operator cares about now fails because it is invoked the wrong way or a dependency is missing. The outcome never landed; the metric did.

## What the job requires

### Must be able to

- Distinguish built vs published vs deployed in every status path.
- Show the live state of a named agent and whether the intended change is present.
- Show that in-scope neighbours were not disturbed.
- Be repeatable by a fresh-process reviewer with no access to the author's narrative.
- Fail closed when live state cannot be read, rather than inferring it from source or CI.

### Won't

- Add a control-plane UI, ledger, or confidence score.
- Put household topology, profile identities, or secret names into the public product.
- Change publication policy merely so two documents agree.
- Treat unit tests as outcome UAT.
- Build a new public harness before checking what the private deployment already exercises.

## Prove it

Outcome-level acceptance, named by job × surface. Independent of unit tests.

**This Job Spec is not complete.** No runnable public scenarios exist. Do not invent them here. Do not treat the job as done until those scenarios exist and a fresh-process reviewer can run them.

Coverage still required, each with a named invariant:

- After a one-agent change, the target read shows the intended live state and neighbours do not. Invariant: `one-agent-change-is-local`.
- A published artefact, a skipped docs-only publish, and a running agent are three different answers. Invariant: `built-published-deployed-are-different`.
- A tool that survives as files but fails when invoked is a failed read. Invariant: persistence includes working tools.
- A second process, not the author, can complete the read from the artefact alone.

## Verdict

Done when: after an intended change, a fresh-process reviewer can state what is running, whether the target has the change, and that neighbours did not move, without trusting the author, proven by runnable scenarios that do not yet exist.

## Abandon signal

We named the wrong job if operators already trust live state from stock tools, never conflate published with deployed, and still cannot (or will not) score intended-change hold rate for some other reason.

## Production-readiness

Honesty: a path that cannot read live state fails closed. It does not guess from source.

Isolation: public proof uses synthetic identities only.

## Related

- Decision: [../rfcs/successor-vision.md](../rfcs/successor-vision.md)
- Implementation RFC: not yet. An RFC that `serves: understand-what-is-running` will carry the how, including any private-proof inventory.
