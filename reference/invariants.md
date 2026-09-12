---
title: Hermes Fleet invariants
last_reviewed: 2026-09-12
---

# Hermes Fleet invariants

The third anchor, beside [vision.md](vision.md) (the why) and [principles.md](principles.md) (the built-well standards).

A principle can be traded on a gradient. An invariant is a line we will not cross by construction, however useful a feature seems. It is the "are we even allowed / is this still Fleet" gate in the verdict rule.

Job Specs name the invariants they must never cross in `invariants:` frontmatter, by the slugs below.

A change that breaks one of these is out of scope. Not a redesign. Not a follow-up.

## no-shared-credentials

Credentials, vaults, sessions, and memories are never shared across agents.

By-construction test: can one profile container read another's secret material, auth state, session store, or memory store? If yes, it is out.

Why this is an invariant: shared credentials make isolation theatre. The North Star is false the moment agent B can act as agent A.

## one-agent-change-is-local

A change to one agent must not disturb another.

By-construction test: after an intended change to profile A, do B's process, image, config, network, or tools move, restart, or break without being named in that change? If yes, it is out.

Why this is an invariant: Fleet exists so blast radius is one agent. A "fleet upgrade" that bounces everyone is not this product.

## built-published-deployed-are-different

Built, published, and deployed are different claims. Reporting one as another is a product failure, not a wording issue.

By-construction test: does this path call a source tree, a registry digest, and a running profile the same kind of "done"? If yes, it is out.

Why this is an invariant: the North Star is about live agents after an intended change. A green bake or a pushed digest is not that.

## public-image-has-no-deployment-identity

The public image and this repository contain no deployment identity, secrets, or household topology.

By-construction test: does this add credentials, vault URIs, OAuth state, live compose, hostnames, household names, real profile identities, or private binaries? If yes, it is out.

Why this is an invariant: the public product is the isolation pattern. The overlay owns who runs it. Mixing them makes the public tree unpublishable and the overlay unauditable.
