---
title: Successor vision
date: 2026-09-12
commit: 4bf14494ee3d64b8c6d58a8cca4e2b3ad839ca31
---

# Decision: successor vision

## Context

Public Fleet already had ProductOS anchors from an operator interview: isolation as the product, four operator outcomes, N-of-N as North Star.

The same human then said what they are actually implementing is how they used Switchroom to run specialist agents: a standing team you text, Claude on the plan they already pay for, on a leash. Switchroom is no longer the runtime. Isolated Hermes is.

That made the first vision insufficient. Isolation is still the one-liner. Standing team is an outcome, not the pitch. Principal-turn quality is the headline number.

## Decision

Public Hermes Fleet is the successor to that Switchroom use. We changed vision, product-spec outcomes, North Star, and the first Job Spec `serves:` line to match the successor interview.

- One-liner stays opinionated isolated Hermes.
- Audience is principal and operator.
- Outcomes are `standing-team`, `on-a-leash`, `there-when-you-reach`, `subscription-honest`.
- `on-a-leash` includes isolation-holds and change-recoverable.
- North Star is TUTR. N of N after an intended change is a leash signal.
- Surfaces are Telegram, Slack, and Desktop, same bar. Buzz is not a Hermes surface.
- `understand-what-is-running` `serves:` `on-a-leash`, not `change-recoverable`.

## Consequences

Commits us to: scoring principal turns on three surfaces; keeping isolation as the public sentence; treating subscription-honest as an outcome and an invariant; citing this record whenever those diff-protected lines move.

Gives up: N-of-N as the headline; operator-only vision; indexing jobs that do not yet have Job Specs; Switchroom's Telegram-and-Buzz-only rule; Claude CLI as the only runtime.

Blind spots: TUTR is uninstrumented. Public-adopter demand is unmeasured. Desktop may not yet meet the same bar as Telegram and Slack. Private overlay proof of live-state reads was not reviewer-readable from the public tree.

## Related

- [vision.md](../vision.md)
- [product-spec.md](../product-spec.md)
- [jobs/understand-what-is-running.md](../jobs/understand-what-is-running.md)
