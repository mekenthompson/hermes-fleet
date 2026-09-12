---
title: Hermes Fleet product vision
last_reviewed: 2026-09-12
---

# Hermes Fleet product vision

Opinionated isolated Hermes. One agent per container and gateway. No shared credentials.

The public product is that isolation layer. A standing team of specialists you text is an **outcome**, not the tagline. A private deployment may dogfood it. The household is not the product.

## Personas

- **Principal.** Lives with the team. Texts specialists, expects the same one back, on the plan they already pay for, asking before consequences. Success: Trusted turns they did not sit through.
- **Operator.** Stands the team up, isolates them, changes one without touching the others. Often the same human on a different day. Success: after an intended change, every in-scope agent still independently working and the target has the change.

## North Star

**Trusted unsupervised turns (TUTR).** Of all turns the principal did not sit through, the share that were Trusted.

A turn is Trusted when it was Unsupervised, completed without a human rescue, stayed inside the granted tools, and made no off-plan model call. Telegram, Slack, and Desktop are scored with the same bar.

If that number does not move, nothing else about Fleet matters.

N of N after an intended change (every in-scope agent still independently working, and the target actually has the change) is a **leash signal**, not the headline.

## Time horizon

When the dogfood production deployment runs the **public Fleet image** (not a private child), principals text the team on Telegram, Slack, and Desktop, and TUTR is the number we score.

## Why now

The same job that lived on a Claude-native specialist runtime now runs on isolated Hermes. Operators still need blast radius of one. Principals still need a team they can text, on the plan they already pay for, that asks before consequences.

## What winning looks like

Outcome and Signal pairs. No numeric targets. Signals are defined in the product spec; repeated here so the vision can be read alone.

- **standing-team.** Named specialists, each with its own identity, memory, tools, and credentials. Signal: independently addressable specialists that still remember and authenticate as themselves.
- **on-a-leash.** One agent cannot see another's secrets; a change to A leaves B alone; failed upgrades recover without touching the others. Signal: intended-change hold rate.
- **there-when-you-reach.** Telegram, Slack, and Desktop all work; a move or restart does not drop identity, tools, or sessions. Signal: first-class surface hold rate.
- **subscription-honest.** Claude stays on the granted plan login, not a silent API meter. Signal: off-plan model-call rate.

## What losing looks like

- A "fleet" that is one shared runtime with profile names.
- Isolation that strips the agent of the integrations that make Hermes useful.
- One messaging surface treated as proof of the others.
- A green publication or unit run reported as production.
- Recreating a profile or borrowing another agent's credentials to get past a blocker.
- Off-plan model calls billed as if they were the subscription.
- Docs, CI, and live state telling three different stories, with the author grading their own work.

## Not this product

- Household identity, live topology, bot tokens, or vault IDs in the public tree.
- Buzz as a Hermes surface.
- Another Hermes. Fleet isolates and operates it.
- A control-plane UI, ledger, or scoreboard.
- Telegram-only, or Slack replacing Telegram.
