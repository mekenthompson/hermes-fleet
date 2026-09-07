ARG AGENT_IMAGE
ARG ONEPASSWORD_CLI_IMAGE=docker.io/1password/op@sha256:d7d12b409ec699c9fa139d3bdfc80671f744380d39db8c539d9dc6e7e553d3c1
FROM ${ONEPASSWORD_CLI_IMAGE} AS onepassword_cli
FROM ${AGENT_IMAGE}

ARG CLAUDE_CODE_VERSION=2.1.261
ARG CLAUDE_AGENT_ACP_VERSION=0.75.1
ARG CLAUDE_ACP_PLUGIN_SOURCE=https://github.com/mvdbastos/hermes-acp-agents
ARG CLAUDE_ACP_PLUGIN_REVISION=0526610a3945cc376ac517b63ca358a5b838a2fc
ENV DISABLE_AUTOUPDATER=1

COPY --from=onepassword_cli --chmod=0755 /usr/local/bin/op /usr/local/bin/op
RUN test "$(/usr/local/bin/op --version)" = "2.39.0"
COPY package.json package-lock.json /opt/coding-clis/
RUN npm ci --omit=dev --prefix /opt/coding-clis --ignore-scripts --no-audit --no-fund \
    && node /opt/coding-clis/node_modules/@anthropic-ai/claude-code/install.cjs \
    && ln -s /opt/coding-clis/node_modules/.bin/claude /usr/local/bin/claude \
    && ln -s /opt/coding-clis/node_modules/.bin/claude-agent-acp /usr/local/bin/claude-agent-acp \
    && test "$(/usr/local/bin/claude --version)" = "${CLAUDE_CODE_VERSION} (Claude Code)" \
    && test "$(node -p 'require("/opt/coding-clis/node_modules/@agentclientprotocol/claude-agent-acp/package.json").version')" = "${CLAUDE_AGENT_ACP_VERSION}" \
    && test -x /usr/local/bin/claude-agent-acp
COPY plugins/model-providers/claude-acp/ /opt/hermes/plugins/model-providers/claude-acp/
COPY --chmod=0755 scripts/claude-acp-subscription /usr/local/bin/hermes-claude-acp-subscription
COPY plugins/web/perplexity/ /opt/hermes/plugins/web/perplexity/
RUN python3 -m py_compile /opt/hermes/plugins/web/perplexity/*.py \
    && HERMES_HOME=/tmp/hermes-plugin-doctor /opt/hermes/bin/hermes plugins doctor /opt/hermes/plugins/web/perplexity --ci
COPY plugins/linear-agent/ /opt/hermes/plugins/linear-agent/
RUN test ! -e /opt/hermes/plugins/linear-agent/linear-agents.json \
    && python3 -m py_compile /opt/hermes/plugins/linear-agent/*.py
COPY scripts/image_ref.py /opt/hermes-fleet/bin/image_ref.py
COPY --chmod=0755 scripts/verify-agent-image-ref.py /opt/hermes-fleet/bin/verify-agent-image-ref
COPY contracts/ /opt/hermes-fleet/contracts/
ARG AGENT_IMAGE
ARG ONEPASSWORD_CLI_IMAGE
ARG FLEET_GIT_SHA=development
ARG FLEET_IMAGE_IDENTITY=local/hermes-fleet
ENV HERMES_FLEET_GIT_SHA=${FLEET_GIT_SHA} \
    HERMES_FLEET_IMAGE_IDENTITY=${FLEET_IMAGE_IDENTITY} \
    HERMES_FLEET_AGENT_IMAGE=${AGENT_IMAGE} \
    HERMES_FLEET_ONEPASSWORD_CLI_IMAGE=${ONEPASSWORD_CLI_IMAGE} \
    HERMES_FLEET_CLAUDE_CODE_VERSION=${CLAUDE_CODE_VERSION} \
    HERMES_FLEET_CLAUDE_AGENT_ACP_VERSION=${CLAUDE_AGENT_ACP_VERSION} \
    HERMES_FLEET_CLAUDE_ACP_PLUGIN_SOURCE=${CLAUDE_ACP_PLUGIN_SOURCE} \
    HERMES_FLEET_CLAUDE_ACP_PLUGIN_REVISION=${CLAUDE_ACP_PLUGIN_REVISION}
LABEL org.opencontainers.image.source="https://github.com/mekenthompson/hermes-fleet" \
      org.opencontainers.image.revision="${FLEET_GIT_SHA}" \
      org.opencontainers.image.base.name="${AGENT_IMAGE}"
RUN python3 -c 'import json, os, pathlib; marker = pathlib.Path("/etc/hermes-fleet/image-provenance.json"); marker.parent.mkdir(parents=True, exist_ok=True); marker.write_text(json.dumps({"schema": 1, "deployment_kind": "fleet_child_image", "image": os.environ["HERMES_FLEET_IMAGE_IDENTITY"], "revision": os.environ["HERMES_FLEET_GIT_SHA"], "parent_agent_image": os.environ["HERMES_FLEET_AGENT_IMAGE"], "onepassword_cli_image": os.environ["HERMES_FLEET_ONEPASSWORD_CLI_IMAGE"], "claude_code_version": os.environ["HERMES_FLEET_CLAUDE_CODE_VERSION"], "claude_agent_acp_version": os.environ["HERMES_FLEET_CLAUDE_AGENT_ACP_VERSION"], "claude_acp_plugin_source": os.environ["HERMES_FLEET_CLAUDE_ACP_PLUGIN_SOURCE"], "claude_acp_plugin_revision": os.environ["HERMES_FLEET_CLAUDE_ACP_PLUGIN_REVISION"]}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"); marker.chmod(0o444)'
