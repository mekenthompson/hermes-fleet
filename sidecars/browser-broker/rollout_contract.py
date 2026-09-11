"""Fail-closed compatibility gate for scoped browser-handoff rollout."""
from __future__ import annotations

from dataclasses import dataclass

POLICY_VERSION = 1
PROTOCOL_VERSION = 1


class RolloutContractError(ValueError):
    pass


@dataclass(frozen=True)
class RolloutVersions:
    plugin: int
    broker: int
    policy: int


def _exact(value: int, label: str) -> None:
    if type(value) is not int or value != PROTOCOL_VERSION:
        raise RolloutContractError(f"{label} version is not supported")


def preflight(versions: RolloutVersions) -> None:
    """Admit only the scoped protocol and matching rendered policy.

    This gate is deliberately independent of the physical takeover marker: a
    failed preflight must leave the existing broker and its inherited lock in
    place rather than attempting a partial replacement.
    """
    _exact(versions.plugin, "plugin protocol")
    _exact(versions.broker, "broker protocol")
    if type(versions.policy) is not int or versions.policy != POLICY_VERSION:
        raise RolloutContractError("policy version is not supported")


def verify_running_plugin_protocol(value: object) -> None:
    """Require proof that the loaded plugin knows the v1 wire contract.

    Rendered plugin configuration only expresses deployment intent. An older
    running plugin does not emit this header and cannot operate a broker whose
    environment merely declares v1.
    """
    if not isinstance(value, str) or value != str(PROTOCOL_VERSION):
        raise RolloutContractError("running plugin protocol is not supported")


def staging_plugin_is_compatible(plugin_protocol: int, broker_protocol: int) -> bool:
    """A compatible plugin may be staged against a legacy broker only.

    It is not a readiness signal: legacy broker protocol 0 has weak unscoped
    semantics, so policy v1 must not be activated until atomic preflight.
    """
    return type(plugin_protocol) is int and plugin_protocol == PROTOCOL_VERSION and broker_protocol == 0
