"""Provision one target profile for managed Linear OAuth."""
from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import re
import socket
import subprocess
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from linear_activity import LinearActivityClient
from linear_agent import unauthorized_response_body_from_entry
from linear_handoff import FLEET_AUTHORITY_STORE
from linear_oauth import ConnectItem, LinearOAuth, load_connect_env, validate_private_directory
from linear_project_updates import _publisher_binding

_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}$")
_ID = re.compile(r"[A-Za-z0-9-]{3,64}$")
_POLICY_PATH = Path(__file__).with_name("linear-agents.json")


def expected_container_hostname(profile: str) -> str:
    if _NAME.fullmatch(profile) is None:
        raise ValueError("Linear profile is invalid")
    return f"hermes-{profile}"


def _allowed_linear_user_ids(entry: dict[str, object]) -> list[str] | None:
    if "allowed_linear_user_ids" not in entry:
        return None
    value = entry["allowed_linear_user_ids"]
    if not isinstance(value, list) or not value:
        raise RuntimeError(
            "Linear allowed_linear_user_ids policy must be a non-empty UUID list"
        )
    canonical: list[str] = []
    for user_id in value:
        if (
            not isinstance(user_id, str)
            or not user_id
            or user_id.strip() != user_id
        ):
            raise RuntimeError(
                "Linear allowed_linear_user_ids policy contains an invalid UUID"
            )
        try:
            parsed = str(uuid.UUID(user_id))
        except (AttributeError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "Linear allowed_linear_user_ids policy contains an invalid UUID"
            ) from exc
        if parsed != user_id:
            raise RuntimeError(
                "Linear allowed_linear_user_ids policy contains an invalid UUID"
            )
        canonical.append(parsed)
    if len(set(canonical)) != len(canonical):
        raise RuntimeError(
            "Linear allowed_linear_user_ids policy contains duplicate UUIDs"
        )
    return canonical


def _lifecycle_settings(entry: dict[str, object]) -> dict[str, object]:
    settings: dict[str, object] = {}
    if "terminal_issue_status" in entry:
        status = entry["terminal_issue_status"]
        if type(status) is not str or status not in {"done", "review"}:
            raise RuntimeError("terminal_issue_status must be done or review")
        settings["terminal_issue_status"] = status
    if "reassign_to_requester" in entry:
        reassign = entry["reassign_to_requester"]
        if type(reassign) is not bool:
            raise RuntimeError("reassign_to_requester must be a boolean")
        settings["reassign_to_requester"] = reassign
    if "heartbeat_seconds" in entry:
        heartbeat = entry["heartbeat_seconds"]
        typed_heartbeat = cast(int | float, heartbeat)
        if (
            type(heartbeat) not in {int, float}
            or not math.isfinite(typed_heartbeat)
            or typed_heartbeat <= 0
        ):
            raise RuntimeError("heartbeat_seconds must be a positive finite number")
        settings["heartbeat_seconds"] = heartbeat
    return settings


def verify_managed_oauth(oauth: LinearOAuth) -> None:
    LinearActivityClient(oauth).verify_authenticated()


def provision(
    *,
    profile_home: Path,
    profile: str,
    workspace: str,
    vault_id: str,
    item_id: str,
    auth_verify: Callable[[LinearOAuth], None] = verify_managed_oauth,
    config_set: Callable[[str, str], None],
    plugin_enable: Callable[[str], None],
    config_unset: Callable[[str], None],
    config_load: Callable[[], dict[str, Any]] | None = None,
) -> None:
    if os.environ.get("HERMES_PROFILE") != profile or socket.gethostname() != expected_container_hostname(profile):
        raise RuntimeError("Linear provisioning container identity does not match target profile")
    if _NAME.fullmatch(profile) is None or _NAME.fullmatch(workspace) is None:
        raise ValueError("Linear profile or workspace is invalid")
    if _ID.fullmatch(vault_id) is None or _ID.fullmatch(item_id) is None:
        raise ValueError("Linear OAuth binding is invalid")
    manifest = json.loads(_POLICY_PATH.read_text(encoding="utf-8"))
    agents = manifest.get("agents") if isinstance(manifest, dict) else None
    matches = [entry for entry in agents or [] if isinstance(entry, dict) and entry.get("profile") == profile]
    expected_oauth = {
        "mode": "managed_oauth_v1",
        "vault_id": vault_id,
        "item_id": item_id,
        "local_state": "/opt/data/secrets/linear-oauth.json",
        "connect_env_file": "/opt/data/.op.env",
    }
    if len(matches) != 1 or matches[0].get("logical_agent") != profile or matches[0].get("workspace") != workspace or matches[0].get("rollout_scope") != [profile] or matches[0].get("oauth") != expected_oauth:
        raise RuntimeError("Linear target does not match the immutable managed OAuth manifest")
    allowed_linear_user_ids = _allowed_linear_user_ids(matches[0])
    lifecycle_settings = _lifecycle_settings(matches[0])
    try:
        unauthorized_response_body_from_entry(matches[0])
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    home = profile_home.absolute()
    validate_private_directory(home)
    _publisher_binding(profile, workspace, vault_id, item_id)
    connect_env = home / ".op.env"
    connect_host, connect_token = load_connect_env(connect_env)
    connect_item = ConnectItem(connect_host, connect_token, vault_id, item_id)
    # The Connect binding is verified by fetching the item and resolving all three fields.
    connect_item.credentials()
    oauth_file = home / "secrets" / "linear-oauth.json"
    auth_verify(LinearOAuth(connect_item, oauth_file, profile=profile))
    prefix = "plugins.entries.linear-agent.settings"
    expected_settings: dict[str, Any] = {
        "profile": profile,
        "workspace": workspace,
        "ingress_database": str(home / "workspace" / "linear-agent" / "ingress.db"),
        "state_database": str(home / "linear-agent" / "state.db"),
        "shared_authority_database": str(FLEET_AUTHORITY_STORE),
        "credential_mode": "managed_oauth_v1",
        "oauth_file": str(oauth_file),
        "connect_env_file": str(connect_env),
        "oauth_vault_id": vault_id,
        "oauth_item_id": item_id,
        "dry_run": False,
        "enabled": True,
    }
    if allowed_linear_user_ids is not None:
        expected_settings["allowed_linear_user_ids"] = allowed_linear_user_ids
    expected_settings.update(lifecycle_settings)

    already_configured = False
    runtime_config = None
    actual_settings = None
    if config_load is not None:
        runtime_config = config_load()
        plugins = runtime_config.get("plugins") if isinstance(runtime_config, dict) else None
        enabled_plugins = plugins.get("enabled") if isinstance(plugins, dict) else None
        disabled_plugins = plugins.get("disabled") if isinstance(plugins, dict) else None
        entries = plugins.get("entries") if isinstance(plugins, dict) else None
        linear_entry = entries.get("linear-agent") if isinstance(entries, dict) else None
        actual_settings = linear_entry.get("settings") if isinstance(linear_entry, dict) else None
        if isinstance(actual_settings, dict):
            for key in (
                "terminal_issue_status",
                "reassign_to_requester",
                "heartbeat_seconds",
            ):
                if key not in lifecycle_settings and key in actual_settings:
                    expected_settings[key] = actual_settings[key]
        already_configured = (
            isinstance(enabled_plugins, list)
            and "linear-agent" in enabled_plugins
            and isinstance(disabled_plugins, list)
            and "linear-agent" not in disabled_plugins
            and isinstance(actual_settings, dict)
            and actual_settings == expected_settings
            and all(
                type(actual_settings.get(key)) is type(value)
                for key, value in lifecycle_settings.items()
            )
        )

    if not already_configured:
        settings = (
            (f"{prefix}.profile", profile),
            (f"{prefix}.workspace", workspace),
            (f"{prefix}.ingress_database", expected_settings["ingress_database"]),
            (f"{prefix}.state_database", expected_settings["state_database"]),
            (f"{prefix}.shared_authority_database", expected_settings["shared_authority_database"]),
            (f"{prefix}.credential_mode", "managed_oauth_v1"),
            (f"{prefix}.oauth_file", expected_settings["oauth_file"]),
            (f"{prefix}.connect_env_file", expected_settings["connect_env_file"]),
            (f"{prefix}.oauth_vault_id", vault_id),
            (f"{prefix}.oauth_item_id", item_id),
            (f"{prefix}.dry_run", "false"),
            (f"{prefix}.enabled", "true"),
        )
        if allowed_linear_user_ids is not None:
            settings += ((
                f"{prefix}.allowed_linear_user_ids",
                json.dumps(allowed_linear_user_ids, separators=(",", ":")),
            ),)
        if "terminal_issue_status" in lifecycle_settings:
            settings += ((
                f"{prefix}.terminal_issue_status",
                lifecycle_settings["terminal_issue_status"],
            ),)
        if "reassign_to_requester" in lifecycle_settings:
            settings += ((
                f"{prefix}.reassign_to_requester",
                str(lifecycle_settings["reassign_to_requester"]).lower(),
            ),)
        if "heartbeat_seconds" in lifecycle_settings:
            settings += ((
                f"{prefix}.heartbeat_seconds",
                str(lifecycle_settings["heartbeat_seconds"]),
            ),)
        for key, value in settings:
            config_set(key, value)
        if allowed_linear_user_ids is None:
            config_unset(f"{prefix}.allowed_linear_user_ids")
        plugin_enable("linear-agent")
    (home / ".linear-provisioning-not-ready").unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--vault-id", required=True)
    parser.add_argument("--item-id", required=True)
    args = parser.parse_args()
    home = Path(os.environ.get("HERMES_HOME", "/opt/data"))
    runtime_profile = os.environ.get("HERMES_PROFILE")
    if not runtime_profile:
        parser.error("managed Linear provisioning requires HERMES_PROFILE from the profile wrapper")
    if runtime_profile != args.profile:
        parser.error("target profile does not match runtime profile")

    def load_runtime_config() -> dict[str, Any]:
        config_module = importlib.import_module("hermes_cli.config")
        config = config_module.load_config()
        if not isinstance(config, dict):
            raise RuntimeError("Hermes runtime config is not a mapping")
        return config

    def set_config(key: str, value: str) -> None:
        subprocess.run(["hermes", "config", "set", key, value], check=True)

    def unset_config(key: str) -> None:
        result = subprocess.run(
            ["hermes", "config", "unset", key],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        if result.returncode == 1 and "Config key not set:" in result.stderr:
            return
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )

    def enable_plugin(name: str) -> None:
        subprocess.run(["hermes", "plugins", "enable", name], check=True)

    provision(
        profile_home=home,
        profile=args.profile,
        workspace=args.workspace,
        vault_id=args.vault_id,
        item_id=args.item_id,
        config_load=load_runtime_config,
        config_set=set_config,
        config_unset=unset_config,
        plugin_enable=enable_plugin,
    )
    print(f"Configured managed Linear OAuth for profile {args.profile}; restart is still required.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
