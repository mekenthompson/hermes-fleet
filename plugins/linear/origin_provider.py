"""Fresh read-only Linear evidence through an already-owned scoped API.

All coordinates come from the independently inspected native admission. This
module never creates a client, loads credentials, starts a service or grants work.
"""
from .transport import Unavailable


def verify(requester, declared_scope, api):
    if not isinstance(requester.get("message_id"), str) or not requester["message_id"]:
        return {"state": "needed", "source": "original_provider_message_id_unavailable"}
    if api is None:
        return {"state": "needed", "source": "existing_linear_api_unavailable"}
    from . import BoundLinearAPI
    from .origin import scope
    if (not isinstance(api, BoundLinearAPI)
            or scope({"identity": api.identity, "specialist_scope": api.specialist_scope}) != declared_scope
            or requester["scope_id"] != api.identity["organization_id"]):
        raise Unavailable("The existing Linear service does not match the inspected requester scope.")
    actor, session_id, message = requester["user_id"], requester["thread_id"], requester["message_id"]
    if actor == api.identity["viewer_id"]:
        raise Unavailable("The Linear recipient app is not the original human requester.")
    with api.requester_session(session_id):
        api.viewer_id()  # existing credential/app/workspace authorization, never a new client
        session = api.agent_session(session_id)
        issue = api.issue(requester["chat_id"])
        if (session.get("id") != session_id or (session.get("issue") or {}).get("id") != issue.get("id")
                or issue.get("id") != requester["chat_id"]
                or (session.get("appUser") or {}).get("id") != api.identity["viewer_id"]):
            raise Unavailable("The original Linear Agent Session no longer verifies its human input.")
        if message == session_id:
            if (session.get("creator") or {}).get("id") != actor:
                raise Unavailable("The Linear assignment actor does not match the native admission.")
        else:
            activity = api.agent_activity(message, session_id)
            if (activity.get("id") != message or (activity.get("agentSession") or {}).get("id") != session_id
                    or (activity.get("user") or {}).get("id") != actor):
                raise Unavailable("The Linear Activity actor does not match the native admission.")
    return {"state": "verified", "source": "existing_scoped_linear_api"}
