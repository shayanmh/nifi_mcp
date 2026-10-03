"""The registered tool set stays inside the build/edit scope."""

from nifi_mcp.server import mcp

_FORBIDDEN = {
    "nifi_set_run_status",
    "nifi_schedule_process_group",
    "nifi_set_controller_service_state",
    "nifi_empty_queue",
    "nifi_list_queue",
}


def test_server_does_not_expose_run_or_purge_tools() -> None:
    names = set(mcp._tool_manager._tools)
    assert not names.intersection(_FORBIDDEN)
    assert {
        "nifi_about",
        "nifi_create_processor",
        "nifi_update_processor",
        "nifi_apply_flow_spec",
        "nifi_bind_parameter_context",
        "nifi_list_registry_buckets",
    }.issubset(names)
