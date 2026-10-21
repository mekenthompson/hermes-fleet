# Claude Subscription DirectSDK (experimental)

Fleet vendors the official Nous Research `claude-subscription-directsdk` plugin as an optional, default-disabled source plugin. It is not installed in the Fleet image and is not enabled or routed by this repository. Enablement is a separate operator decision.

The plugin uses the official Claude Code CLI and its own logged-in Claude account for that profile. Install and authenticate the CLI separately. Hermes profile isolation must remain intact: do not copy login state between profiles. This plugin does not provide API-key billing fallback; resolve CLI login or entitlement errors at the CLI/account boundary rather than switching to an API key.

Upstream code and manifest are preserved unmodified. `upstream.json` records the upstream repository, exact commit, per-file SHA-256 values, canonical tree hash and MIT license hash. Review upstream changes before updating that pin. The plugin's bundled README and upstream tests document its controls and limitations.

Packaging tests verify provenance, license presence, absence of household literals, and optional/non-image status. See the upstream plugin's `tests/` and `evals/` for its offline suite and loopback compatibility checks; no paid inference is part of those checks.
