---
title: Hermes Fleet product vision
last_reviewed: 2026-09-12
---

# Hermes Fleet product vision

Hermes Fleet is the opinionated way to run Hermes isolated: one agent per container and gateway, no shared credentials, so an operator can change or recover one agent without losing a working, capable setup.

The public product is that isolation and release layer. A private deployment may dogfood it. The household is not the product.

## North Star

After any intended change: **N of N agents still independently working, and the target actually has the change.**

If that number does not move, nothing else about Fleet matters.

## Time horizon

When the dogfood production deployment runs the **public Fleet image** (not a private child) and the North Star holds on a **real one-agent change**.

## Why now

Hermes Agent is one process. Many agents on one host without isolation share filesystems, networks, credentials, and blast radius. Operators already run Hermes. They hire Fleet so a change to one agent is local, recoverable, and visible.

## What winning looks like

- One agent cannot see another's files, network, credentials, or vault.
- The isolated agent is still Hermes: browser handoff, local voice, Linear, and search still work.
- An upgrade can fail and be rolled back without touching the others.
- An existing working agent can come under Fleet management without losing identity, tools, or sessions.
- After a change, a fresh-process reviewer can tell what is running and whether the intended change took effect. Built, published, and deployed are not treated as the same claim.

## What losing looks like

- A "fleet" that is one shared runtime with profile names.
- Isolation that strips the agent of the integrations that make Hermes useful.
- A green publication or unit run reported as production.
- Recreating a profile or borrowing another agent's credentials to get past a blocker.
- Docs, CI, and live state telling three different stories, with the author grading their own work.
