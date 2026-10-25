"""Link a native task to its human outcome; linking never changes native execution."""
from .api import LinearError
from .bridge import CLOSED, iso_ms


def authorized(bridge, context):
    if getattr(context, "_serialized_transport", False):
        return getattr(context, "_native_host_verified", False) is True
    validate = getattr(bridge, "validate_chat_admission", None)
    return callable(validate) and validate(context)


def handle(bridge, args, context):
    from .chat import _reply

    if not authorized(bridge, context):
        return _reply(False, "Tracking requires an authenticated native turn in this profile.",
                      code="unverified_host_turn")
    ref = args.get("issue")
    if not isinstance(ref, str) or not ref.strip():
        return _reply(False, "Tracking needs an issue identifier.")
    try:
        issue = bridge.api.issue(ref)
        if ref not in (issue.get("id"), issue.get("identifier")):
            return _reply(False, "Issue resolution did not match the request.")
        issue_id = issue["id"]
        if not bridge.authorize_specialist_effect(issue_id):
            return _reply(False, "Issue authorization is fenced or unavailable.")
        with bridge.lock(issue_id):
            row = bridge.store.get(issue_id)
            if args["action"] == "status":
                task = bridge.kanban.get(row["task_id"]) if row and row.get("task_id") else None
                return _reply(True, "Native task state; no execution was changed.",
                              task_id=task.id if task else None, state=task.status if task else "unbound",
                              legacy_reconciliation_required=bool(row and row["origin"] == "chat"))
            task_id = args.get("task_id")
            if not isinstance(task_id, str) or not task_id.strip():
                return _reply(False, "track needs the existing native task_id. Ordinary chat needs no Linear issue.")
            if row and row["origin"] == "chat":
                return _reply(False, "A retained legacy chat record needs native reconciliation before linking. "
                                    "It was preserved; no task was adopted or resumed.",
                              code="legacy_reconciliation_required")
            if row and row.get("task_id") != task_id:
                return _reply(False, "This outcome already links another task; reconcile the binding.",
                              code="task_binding_conflict", task_id=row.get("task_id"))
            task = bridge.kanban.get(task_id)
            executor = getattr(bridge.kanban, "executor_profile", None) or bridge.profile
            if task is None or task.assignee != executor:
                return _reply(False, "The task does not belong to this native execution profile.")
            if row:
                return _reply(True, "Already linked to the same native task; execution is unchanged.",
                              task_id=task.id, state=task.status)
            me = bridge.api.viewer_id()
            if ((issue.get("state") or {}).get("type") in CLOSED
                    or (issue.get("delegate") or {}).get("id") not in (None, me)):
                return _reply(False, "The human outcome is closed or assigned elsewhere; reconcile it first.")
            watermark = iso_ms(issue.get("updatedAt"))
            if watermark <= 0:
                return _reply(False, "Linear ordering timestamp is unavailable; native execution is unchanged.")
            if not bridge.store.link_task(issue_id, task.id, (issue.get("project") or {}).get("id"),
                                          watermark):
                return _reply(False, "An existing binding or unresolved effect needs reconciliation.")
            bridge.kanban.subscribe(task.id, issue_id)
            # Reporting is queued after native work already exists. Delivery never gates its worker.
            state = "in_progress" if task.status in ("running", "ready", "review") else "blocked"
            bridge.status(issue_id, state, claim=True, seen=(issue.get("delegate") or {}).get("id"))
            return _reply(True, "Linked native task; Linear reporting is queued and execution is unchanged.",
                          task_id=task.id, state=task.status)
    except LinearError as exc:
        if not exc.retryable and exc.authoritative_issue_id:
            bridge._fence_scope_denial(exc.authoritative_issue_id, exc)
        return _reply(False, f"Linear reporting is unavailable ({exc}); native execution was not changed.")
