"""Claude Code subscription provider via ACP.

Adapted from https://github.com/mvdbastos/hermes-acp-agents at source
revision 0526610a3945cc376ac517b63ca358a5b838a2fc. The Fleet launcher
fails closed to the Claude subscription login by removing API, proxy,
cloud-provider, and model-override environment variables before launch.
"""

from typing import Any

from providers import register_provider
from providers.base import ProviderProfile

from .client import ClaudeACPClient


class ClaudeACPProfile(ProviderProfile):
    """Claude Code external-process provider."""

    def create_client(self, **client_kwargs: Any) -> Any:
        """Use the profile-local ACP transport and the subscription launcher."""
        command = self.process_command or "/usr/local/bin/hermes-claude-acp-subscription"
        client_kwargs.setdefault("acp_command", command)
        client_kwargs.setdefault("command", command)
        client_kwargs["args"] = list(self.process_args or ())
        return ClaudeACPClient(**client_kwargs)

    def fetch_models(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float = 8.0,
    ) -> list[str] | None:
        return None


register_provider(
    ClaudeACPProfile(
        name="claude-acp",
        aliases=("claude-code-acp",),
        api_mode="chat_completions",
        display_name="Claude Code ACP",
        description="Claude Code subscription via the maintained ACP adapter",
        env_vars=("CLAUDE_CODE_OAUTH_TOKEN",),
        base_url="acp://claude",
        auth_type="external_process",
        supports_health_check=False,
        process_command="/usr/local/bin/hermes-claude-acp-subscription",
        process_args=(),
        fallback_models=("default", "opus[1m]", "sonnet", "haiku"),
    )
)
