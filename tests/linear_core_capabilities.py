"""Exact historical-Core qualifications for the optional producer-owned status seam.

Release-Core lanes still require every feature test. Legacy lanes retain all other
coverage and explicitly prove that unanchored typing does not publish an action.
"""
import unittest

STATUS_METADATA_REASON = "requires Core capability: producer-owned typing metadata"
HISTORICAL_STATUS_CORES = {
    "769d1be0a24dd28d44d27894094cc77e9359dbea",
    "1436af4a930a65faff42cf2d79a88b65b69a5fdc",
}
STATUS_METADATA_TESTS = {
    "test_linear_native_prompt_scenarios.NativePromptScenarios.test_approval_withdrawal_after_remote_accept_before_ack_restores_live_turn",
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_clarify_retirement_precedes_the_first_resumed_typing_action',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_clarify_retirement_uses_completed_core_turn_without_reopening_status',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_clarify_timeout_does_not_unpause_another_agent_session_question',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_clarify_timeout_preserves_a_live_approval',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_clarify_timeout_preserves_another_live_question',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_every_read_and_mutation_is_optional',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_live_step_hook_does_not_wait_for_network_and_only_one_is_in_flight',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_live_step_stops_at_final_and_reopens_only_for_new_input',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_live_step_throttles_changed_phrases_and_keeps_chat_alive',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_new_input_supersedes_queued_step_after_store_and_bridge_restart',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_optional_status_obeys_reserve',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_private_argument_not_published',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_question_invalidates_scheduled_live_step',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_queued_step_cannot_follow_a_newly_admitted_input',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_approval_answer_keeps_typing_paused_for_the_next_approval',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_approval_answer_resumes_core_typing_and_posts_a_live_step',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_approval_timeout_does_not_resume_over_a_different_pending_approval',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_approval_timeout_resumes_core_typing_without_a_human_answer',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_approval_withdrawal_resumes_without_a_timeout_notice',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_callbacks_keep_status_phrases_with_independent_agent_sessions',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_clarify_resume_preserves_a_pending_approval',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_clarify_resume_preserves_another_question',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_clarify_timeout_retires_pause_and_restarts_owned_status',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_real_clarify_withdrawal_retires_pause_and_restores_work',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_session_scoped_step_caps_every_request_and_yields_before_auth',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_status_arguments_and_unknown_tool_labels_never_leave_core_callback',
    'test_linear_native_prompt_scenarios.NativePromptScenarios.test_unowned_core_callback_cannot_borrow_environment_identity',
    'test_linear_native_delivery_seam.FinalDeliveryAdmissionScenarios.test_status_phrase_stays_with_its_executing_producer',
}


def status_metadata_available():
    try:
        from gateway.platforms import helpers
    except ImportError:
        return False
    return callable(getattr(helpers, "typing_metadata_for_turn", None))


def requires_status_metadata(test):
    return unittest.skipUnless(status_metadata_available(), STATUS_METADATA_REASON)(test)


def unexpected_skips(skipped, revision):
    """Only this named feature on these two exact historical sources may be absent."""
    legacy = revision in HISTORICAL_STATUS_CORES and not status_metadata_available()
    return [(test, reason) for test, reason in skipped if not (
        legacy and reason == STATUS_METADATA_REASON and test.id() in STATUS_METADATA_TESTS)]
