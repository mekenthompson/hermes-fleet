---
title: Hermes Fleet product spec
last_reviewed: 2026-09-12
---

# Hermes Fleet product spec

The product-level layer between the anchors ([vision](vision.md) / [principles](principles.md) / [invariants](invariants.md)) and the Job Specs. Job Specs `serves:` one of the outcome slugs named here.

Decision: [rfcs/successor-vision.md](rfcs/successor-vision.md).

## The product, in one line

Opinionated isolated Hermes. One agent per container and gateway. No shared credentials. A standing team of specialists you text is an outcome, not the tagline.

## North Star

**Metric:** trusted unsupervised turns (TUTR)

**Definition:** of unsupervised principal turns on Telegram, Slack, and Desktop in a scoring window chosen when the metric is instrumented, the share that were Trusted. A turn is Trusted when it was Unsupervised, completed without a human rescue, stayed inside the granted tools, and made no off-plan model call. The window is not a number invented here.

**Now → Target:** unmeasured → to validate once dogfood runs the public Fleet image and principals text the team on those three surfaces. No numeric target was ratified.

**Why this one:** it is principal-turn quality. Isolation, leash, presence, and subscription honesty all fail this number if they fail.

**Leash signal (not the headline):** intended-change hold rate. Of intended fleet changes, the share where the target has the intended state and every other in-scope agent is still independently working, including tools that still do their job.

## Outcomes

Every job ladders up to exactly one of these. A change that advances none of them is out of scope.

### standing-team – Standing team

Named specialists you text, each with its own identity, memory, tools, and credentials. Not one generalist with profile names.

Signal: share of in-scope specialists that a principal can address as themselves, with unshared memory and credentials. Shape: count of independently addressable specialists that still remember and authenticate as themselves. Now: unmeasured. Target: to validate. Guardrail: do not fake a team by routing everyone through one runtime.

### on-a-leash – On a leash

Isolation plus recoverable change plus no self-escalation. One agent cannot see another's files, network, credentials, or vault. A change to A leaves B alone. Failed upgrades recover without touching the others. Built, published, and deployed stay distinct claims.

Signal: intended-change hold rate (the leash signal above). Now: unmeasured. Target: to validate. Guardrail: do not "fix" bleed by collapsing agents onto a shared runtime, and do not treat a published artefact as a deployed agent.

### there-when-you-reach – There when you reach for it

The isolated agent is still Hermes, still reachable, still itself after a move or restart. Telegram, Slack, and Desktop have the same bar. Adding a surface does not replace an existing one. Persistence includes working tools, not just surviving files.

Signal: first-class surface hold rate. Shape: after an isolation, restart, or intended-runtime change, share of claimed surfaces on which the specialist still completes the job, and share of pre-move tools that still run. Now: unmeasured. Target: to validate. Guardrail: do not treat one surface as proof of another.

### subscription-honest – Subscription-honest

Claude stays on the granted plan login (ACP/OAuth), not a silent API meter. Other billed subscriptions follow the same rule: the principal's granted plan is the ceiling unless they opt a specific account into overage.

Signal: off-plan model-call rate. Shape: share of turns that billed a path the principal did not grant. Now: unmeasured. Target: to validate. Guardrail: do not "make it work" by dropping to an API key.

## The customer model

Last revised: 2026-09-12 – successor interview. Confidence: **2** (directional). Dogfood principal+operator evidence is strong; public-adopter evidence is [GUESS].

**Who hires the job.** Two people, often the same human on different days.

- Principal: already paying for a model plan, wants a standing team they can text, that remembers them, that asks before consequences.
- Operator: stands the team up, isolates them, upgrades them, and needs a change to stay local.

**Forces**

- Push: one shared runtime; a bounce that takes everyone down; a specialist that forgets you; an off-plan bill. Evidence: successor interview; Switchroom product the dogfood already ran.
- Pull: isolated specialists you text; recover one without touching the rest; stay on the granted plan. Evidence: same interview.
- Anxiety: isolation that breaks chat, voice, or login; an upgrade they cannot see; a surface that works in Telegram and dies on Slack or Desktop. Evidence: interview surfaces and non-goals.
- Habit: keep the private snowflake, or keep the old Claude-native specialist runtime, because it already works. [GUESS] for public adopters.

**The workaround.** Hand-assembled runtime, copied env files, "I know what's running because I just deployed it," and a single chat app treated as the product.

**Behaviour.** The principal texts and expects the same specialist back. The operator reads live state when they distrust docs. Zero tolerance for "deployed" when the profile is still on the previous intended state, and for a turn that billed the wrong plan.

**Language and the buying committee.** Dogfood words: "what's running", "don't disturb the others", "on a leash", "the plan we already pay for." The dogfood human is both champion and signer. Public-adopter committee unknown. [GUESS]

**Their economics.** Fleet is not a paid product. The principal already pays the model plan. A bad month is a change that takes more than one agent down, a "success" that was only published, or an off-plan bill.

**What must be true to win**

- Principals will text specialists rather than one generalist. If they will not, standing-team is the wrong outcome.
- Operators will run one isolated agent rather than one Hermes with many profiles. If they will not, on-a-leash is the wrong outcome.
- The public product can carry generic capability without household identity.
- Live state can be read independently of the author.
- Telegram, Slack, and Desktop can meet one bar. If Desktop cannot, it is not first-class.

**Known unknowns**

- Public-adopter demand and buying committee.
- Whether stock runtime tools already satisfy "understand what is running."
- What private drift proof already covers (HF-107, HF-163 not reviewer-readable from the public tree).
- How TUTR will be scored on Desktop versus Telegram and Slack. Close: first UAT for there-when-you-reach, not a number invented here.

## How it functions, at a high level

Each specialist stays independently bounded. The principal reaches them on Telegram, Slack, or Desktop. A change is supposed to land on one of them and leave the others alone. Claude stays on the granted plan. Deployment identity stays outside the public product. Runtime and packaging live in `docs/` and `contracts/`.

## The job index

Only jobs with a Job Spec are indexed.

### on-a-leash

- [understand-what-is-running.md](jobs/understand-what-is-running.md) – share of intended changes where a fresh-process reader can state the target's live state and whether it matches intent. Job metric: unmeasured. Rolls into on-a-leash's Signal. **Specified.** Retargeted from `change-recoverable` by [rfcs/successor-vision.md](rfcs/successor-vision.md).

Further jobs are not indexed until they have Job Specs. The successor interview named work that is still unwritten; it lives in the decision record, not here.

## Evidence & confidence

Evidence: 2026-09-12 successor interview (one-liner, four outcomes, TUTR, surfaces, audience); Switchroom vision/JTBD as the prior vehicle, not copied wholesale; public contracts and README isolation model.

Confidence: 2. The dogfood principal+operator ratified the sentence. Public adopter demand is unmeasured. TUTR is uninstrumented.

## Related

- [vision.md](vision.md)
- [principles.md](principles.md)
- [invariants.md](invariants.md)
- [rfcs/successor-vision.md](rfcs/successor-vision.md)
- Linear: [HF-282](https://linear.app/switchroom-ai/issue/HF-282/retarget-public-fleet-vision-as-switchroom-successor)
