---
title: Hermes Fleet product spec
last_reviewed: 2026-09-12
---

# Hermes Fleet product spec

The product-level layer between the anchors ([vision](vision.md) / [principles](principles.md) / [invariants](invariants.md)) and the Job Specs. Job Specs `serves:` one of the outcome slugs named here.

## The product, in one line

Hermes Fleet is the opinionated way to run Hermes isolated: one agent per container and gateway, no shared credentials, so an operator can change or recover one agent without losing a working, capable setup.

## North Star

**Metric:** intended-change hold rate

**Definition:** of intended fleet changes in a window, the share where (a) the target agent is observed to have the intended state and (b) every other in-scope agent is observed still independently working. Independently working includes tools that still do their job, not merely a process that is still up.

**Now → Target:** unmeasured → to validate once the dogfood deployment runs the public Fleet image. No numeric target was ratified.

**Why this one:** it is the vision made countable. Isolation, capability, recovery, and surviving setup all fail this number if they fail.

## Outcomes

Every job ladders up to exactly one of these. A change that advances none of them is out of scope.

### isolation-holds – Isolation holds

One agent cannot see another's files, network, credentials, or vault. The operator can change A and leave B alone.

Signal: cross-agent bleed or unsolicited neighbour restart on an intended one-agent change. Shape: count of such events per intended one-agent change. Now: unmeasured. Target: zero, to validate. Guardrail: do not "fix" bleed by collapsing agents onto a shared runtime.

### capable-hermes – Isolated Hermes stays capable

The isolated agent is still Hermes. Browser handoff, local voice, Linear, and search still work. Optional capabilities stay off until turned on. Adding a surface does not replace an existing one.

Signal: enabled-capability hold rate. Shape: after an isolation or intended-runtime change, share of enabled capabilities on the target that still complete their job. Now: unmeasured. Target: to validate. Guardrail: do not strip integrations to make isolation easier.

### change-recoverable – Change is recoverable

The operator can upgrade or roll back one agent, know whether the intended change took effect, and recover a failed upgrade without touching the others. Built, published, and deployed stay distinct claims.

Signal: intended-change hold rate on one-agent upgrades and rollbacks. This outcome owns the North Star's observability. Now: unmeasured. Target: to validate. Guardrail: do not treat a published artefact as a deployed agent.

### working-setup-survives – Working setup survives

An existing agent can come under Fleet management without losing identity, tools, or sessions. Persistence includes working tools and dependency ownership, not just stored files that still exist.

Signal: setup-survival rate. Shape: share of existing agents brought under management that still authenticate and run their pre-move tools. Now: unmeasured. Target: to validate. Guardrail: do not recreate a profile or borrow another agent's credentials to clear a blocker.

## The customer model

Last revised: 2026-09-12 – first ProductOS pass from operator interview. Confidence: **2** (directional). Dogfood operator evidence is strong; public-adopter evidence is [GUESS].

**Who hires the job.** When an operator already runs (or is about to run) more than one Hermes agent on one host, they want each agent isolated and still capable, so a change or failure stays local. They choose the runtime. They are judged on agents staying up and not leaking.

**Forces**

- Push: shared process, shared credentials, and a bounce that takes everyone down. Evidence: operator interview 2026-09-12.
- Pull: one bounded agent, recover one without touching the rest. Evidence: same interview; isolation is the public product default.
- Anxiety: isolation that breaks browser, voice, tracker, or login; an upgrade they cannot see or undo. Evidence: interview non-goals and first job.
- Habit: keep the private snowflake because it already works. [GUESS] for public adopters.

**The workaround.** Hand-assembled runtime, shared networks, copied env files, and "I know what's running because I just deployed it." Switching cost is the working private overlay.

**Behaviour.** The operator will read live state when they distrust docs. They have zero tolerance for a status that says deployed when the profile is still on the previous intended state.

**Language and the buying committee.** Dogfood words in use: "what's running", "intended change", "don't disturb the others." The dogfood operator champions, evaluates, and signs. Public-adopter language, champion, evaluator, signer, and veto are unknown. [GUESS]

**Their economics.** Fleet is not a paid product. Public-adopter budget, margin, and buying motion are unknown. The dogfood operator is judged on agents staying up and not leaking. A bad month looks like a change that takes more than one agent down, or a "success" that was only published.

**What must be true to win**

- Operators will run one isolated agent rather than one Hermes with many profiles. If they will not, isolation-holds is the wrong outcome.
- The public product can carry generic capability without household identity. If every useful integration is deployment-specific, capable-hermes never ships publicly.
- Live state can be read independently of the author. If not, the North Star cannot be scored.

**Known unknowns**

- Public-adopter demand and buying committee. Close: later adopter conversations, not this interview.
- Whether stock runtime tools already satisfy "understand what is running" well enough that Fleet should not add a surface. Close: the first Job Spec's bet; abandon if true.
- What private drift proof already covers. HF-107 and HF-163 exist; their contents were not reviewer-readable from the public tree. Close: private overlay inventory before any new harness.

## How it functions, at a high level

Each agent stays independently bounded. A change is supposed to land on one of them and leave the others alone. The operator can observe whether that happened and recover when it did not. Built, published, and deployed stay different claims. Deployment identity stays outside the public product. Runtime and packaging live in `docs/` and `contracts/`.

## The job index

### change-recoverable

- [understand-what-is-running.md](jobs/understand-what-is-running.md) – share of intended changes where a fresh-process reader can state the target's live state and whether it matches intent. Job metric: unmeasured. Rolls into change-recoverable's Signal. **Specified.**

Queued, not yet specified:

- upgrade-and-recover – failed one-agent upgrades restored without disturbing neighbours.

### isolation-holds

Queued, not yet specified:

- change-one-agent-without-disturbing-another – intended change to A leaves B's process, image, and tools untouched.

### working-setup-survives

Queued, not yet specified:

- bring-existing-agent-under-management – an already-working agent keeps identity, tools, and sessions after coming under Fleet.

### capable-hermes

No Job Spec yet. Browser handoff, local voice, Linear, and search are capabilities this outcome must keep working. They become jobs when we specify their operator progress, not before.

## Evidence & confidence

Evidence: 2026-09-12 operator interview (vision, four outcomes, North Star, invariants, principles, non-goals, first job); public contracts and README isolation model; known publication vs deployment split in `docs/image-release.md`.

Confidence: 2. The dogfood operator ratified the sentence. Public adopter demand is unmeasured.

## Related

- [vision.md](vision.md)
- [principles.md](principles.md)
- [invariants.md](invariants.md)
- Linear: [HF-186](https://linear.app/switchroom-ai/issue/HF-186/adopt-productos-in-public-hermes-fleet)
