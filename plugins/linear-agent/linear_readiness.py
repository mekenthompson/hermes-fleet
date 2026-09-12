"""Fail-closed authority for Linear issue dependency readiness."""
from __future__ import annotations

from collections.abc import Callable


_RELATIONS_QUERY = """
query IssueDependencyReadiness($issueId: String!) {
  issue(id: $issueId) {
    inverseRelations(first: 250) {
      nodes { type issue { id state { type } } }
      pageInfo { hasNextPage }
    }
  }
}
"""


class LinearDependencyReadiness:
    """Read incoming Linear `blocks` relations; cancellation is not completion."""

    def __init__(self, graphql: Callable[[str, dict[str, object]], object]) -> None:
        self._graphql = graphql

    def is_ready(self, issue_id: str | None) -> bool:
        if not isinstance(issue_id, str) or not issue_id:
            return False
        try:
            response = self._graphql(_RELATIONS_QUERY, {"issueId": issue_id})
        except Exception:  # network/auth/schema uncertainty must not start work
            return False
        if not isinstance(response, dict) or response.get("errors"):
            return False
        data = response.get("data")
        issue = data.get("issue") if isinstance(data, dict) else None
        relations = issue.get("inverseRelations") if isinstance(issue, dict) else None
        if not isinstance(relations, dict):
            return False
        nodes, page_info = relations.get("nodes"), relations.get("pageInfo")
        if not isinstance(nodes, list) or not isinstance(page_info, dict):
            return False
        if page_info.get("hasNextPage") is not False:
            return False
        for relation in nodes:
            if not isinstance(relation, dict) or not isinstance(relation.get("type"), str):
                return False
            if relation["type"] != "blocks":
                continue
            blocker = relation.get("issue")
            state = blocker.get("state") if isinstance(blocker, dict) else None
            # Canceled is intentionally refused until a documented product rule
            # explicitly changes this policy.
            if not isinstance(state, dict) or state.get("type") != "completed":
                return False
        return True
