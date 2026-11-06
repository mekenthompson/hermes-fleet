"""Readable native closeout through the existing issue-comment and Activity outbox."""
import json



def prepare(writes, *, issue_id, owner, completion, summary, evidence, head_note, assessment_id=None):
    """One full issue comment per native completion; a session carries only its pointer."""
    from .bridge import stable_uuid4
    comment_id = assessment_id or stable_uuid4("native-outcome-comment", issue_id,
        json.dumps(completion, sort_keys=True, separators=(",", ":")))
    if not assessment_id:
        proof = "\n".join(f"- {url}" for url in dict.fromkeys(evidence)) or "See the findings above."
        summary = summary or "Completed the agreed issue outcome."
        body = f"**Delivered**\n\n{summary}{head_note}\n\n**Evidence**\n\n{proof}\n\n**Remaining**\n\nNone reported."
        writes.insert(1, ("comment", dict(id=comment_id, issue_id=issue_id, task_id=completion["task_id"],
            body=body, native_completion=completion, native_outcome_comment=True)))
    result = []
    for kind, payload in writes:
        if kind == "activity" and payload.get("content", {}).get("type") == "response":
            if not owner:
                continue  # The full comment already serves a task without an Agent Session.
            payload = {**payload, "native_outcome_pointer": True, "requires_comment_id": comment_id,
                "outcome_assessment": bool(assessment_id),
                "native_completion": completion, "content": {"type": "response",
                    "body": "Completed. The result and checks are in the issue comment."}}
        result.append((kind, payload))
    return result


def pointer_ready(bridge, row):
    """Reuse an existing assessment repair while its sole pointer is still unattempted."""
    payload = row["payload"]
    if bridge.store.outcome_comment_applied(payload):
        return True
    prior = bridge.store.outbox_row(payload["requires_comment_id"])
    if payload.get("write_started") is False and prior and prior["payload"].get("assessment"):
        candidate = bridge.store.assessment_comment_id(payload["issue_id"], prior["payload"]["body"],
            completion=payload["native_completion"])
        if candidate and candidate != prior["id"]:
            row["payload"] = {**payload, "requires_comment_id": candidate}
            bridge.store.rewrite(row["id"], row["payload"], row["next_at"])
    return bridge.store.outcome_comment_applied(row["payload"])
