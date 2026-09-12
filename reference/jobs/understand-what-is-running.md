---
title: Understand what is running
job: understand-what-is-running
serves: change-recoverable
stakes: full
invariants:
  - built-published-deployed-are-different
  - one-agent-change-is-local
  - public-image-has-no-deployment-identity
  - no-shared-credentials
last_reviewed: 2026-09-12
---

# Job Spec: understand what is running and whether the intended change actually took effect

A durable Job Spec. How we surface live state lives in an RFC that `serves:` this job. That artefact churns. This job does not.

## The job

An operator has just changed one agent. They need to know what each agent is actually running, and whether the agent they meant to change now has that change. They are not asking for a dashboard. They are asking for an honest read of live state that a person who did not make the change can repeat.

Struggling moment: When a change is "done," the operator is stuck because source, published artefact, docs, and the running agent can each tell a different story, and the person who made the change is the one declaring success.

Job story: When I change one agent, I want to read what is live on the target and on the others, so I can know the intended change took effect and nobody else moved.

## Today's alternatives

Today they: inspect the host runtime, read the docs, trust the implementing agent's report, or use a private overlay that already knows the topology.

The bar: those work when the operator already knows which host and name to look at, and when they remember that published is not deployed. They fail when docs drift from the workflow, when stored files survived but a tool no longer does its job, or when the author grades their own work. Switching cost is giving up a private read that already knows the household.

## The bet

This job assumes: operators cannot trust the North Star without an independent live read that distinguishes built, published, and deployed, and that names whether the target matches intent. If that's false, don't build: if stock runtime tools already answer this honestly for a Fleet layout, this job is documentation, not a product surface.

## Evidence & confidence

- Operator interview 2026-09-12: first job chosen because the North Star is unverifiable without it.
- Public docs already separate publication from rollout (`docs/image-release.md`). That supports the claim distinction. It does not independently prove operators conflate the two.
- Related overlay issues exist (HF-107, HF-163). Their contents were not reviewer-readable from the public tree, so they are not counted as verified evidence.

Confidence: 2 (directional) for the struggling moment. 2 for the shape of a public, topology-free proof. Not 3: one interview plus public docs, no independently readable observation of the claimed operator behaviour.

## Measures of success

Mechanism: after an intended change, a fresh-process reader can state what the target is actually running, whether that matches intent, and that in-scope neighbours still match their last intended state, including that tools still do their job.

Leading indicator: share of intended changes where that read is possible without asking the author.

Where the number lives: rolls into change-recoverable's intended-change hold rate (product spec), which drives the North Star.

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

Outcome-level acceptance, named by job × surface. Independent of unit tests. **Runnable public scenarios for this job are not authored yet.** Do not invent them in this spec. Inventory private overlay proof before adding a harness. Treat HF-107 and HF-163 as pointers, not as verified coverage.

Coverage to author, each with a named invariant:

- intended-change-present (operator live read): after a one-agent change, the target read shows the intended live state and neighbours do not. Invariant: `one-agent-change-is-local` plus intended state on the target.
- built-published-deployed-distinct (docs + live read): a published artefact, a skipped docs-only publish, and a running agent are three different answers. Invariant: `built-published-deployed-are-different`.
- working-tool-path (cold and warm): a tool that survives as files but fails when invoked is a failed read. Invariant: persistence includes working tools (principle 5), not a green volume.
- fresh-process-reviewer: a second process, not the author, can complete the read from the artefact alone.

Fuzz corpus: vary change kind × publish skipped vs published × cold empty vs warm existing state × whether tools still execute. Invariants must hold across the corpus, not just the happy path.

## Verdict

Done when: after an intended change, a fresh-process reviewer can state what is running, whether the target has the change, and that neighbours did not move, without trusting the author, proven by the scenarios above once they exist.

## Abandon signal

We named the wrong job if operators already trust live state from stock tools, never conflate published with deployed, and still cannot (or will not) score the North Star for some other reason.

## Production-readiness

Honesty: a path that cannot read live state fails closed. It does not guess from source.

Isolation: the public proof uses synthetic profiles only. No household names.

Before a new harness: inventory private evidence and say what is already proven.

## Related

- Queued jobs: change-one-agent-without-disturbing-another, upgrade-and-recover, bring-existing-agent-under-management.
- Overlay pointers: HF-107, HF-163. Private; not independently verified here. Those are not this Job Spec.
- Implementation: not yet. An RFC that `serves: understand-what-is-running` will carry the how.
