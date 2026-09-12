---
title: Successor vision
date: 2026-09-12
commit: pending
---

# Decision: successor vision

Status: ratified in operator interview 2026-09-12. This record exists because it retargets vision, product-spec outcomes, North Star, and the first Job Spec's `serves:` line.

## Decision

Public Hermes Fleet is the successor to how Switchroom was used to run specialist agents. Isolation is the public one-liner. Standing team is an outcome, not the tagline.

## Ratified

- One-liner: opinionated isolated Hermes.
- Audience: principal and operator.
- Outcomes: `standing-team`, `on-a-leash`, `there-when-you-reach`, `subscription-honest`.
- `on-a-leash` includes isolation-holds and change-recoverable.
- North Star: TUTR. N of N after an intended change is a leash signal.
- Surfaces: Telegram, Slack, and Desktop, same bar. Buzz is not a Hermes surface.
- First Job Spec `understand-what-is-running` retargets from `change-recoverable` to `on-a-leash`.

## Not brought over from Switchroom

- Telegram-and-Buzz-only.
- Claude CLI as the only runtime. Hermes is the agent. Claude ACP is how a granted plan stays honest.
- TUTR replacing isolation. Isolation remains the one-liner and a leash invariant.

## Why this record

ProductOS treats `job` / `serves` / `stakes` as diff-protected. The outcome slug `change-recoverable` is removed. This is the cited decision.
