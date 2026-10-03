"""MCP tools for a NiFi 2.7.2 single-user instance.

Tool names follow newen-systems/nifi-mcp where the operation is in scope.
Run-status, controller-service enable, and queue drop are intentionally absent.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from mcp.server.mcpserver import MCPServer

from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings
from nifi_mcp.registry import RegistryClient

mcp = MCPServer("nifi-mcp")

_client: NiFiClient | None = None
_registry: RegistryClient | None = None


def _nifi() -> NiFiClient:
    global _client
    if _client is None:
        _client = NiFiClient(Settings())
    return _client


def _registry_client() -> RegistryClient:
    global _registry
    if _registry is None:
        _registry = RegistryClient(Settings())
    return _registry


@mcp.tool()
def nifi_about() -> dict[str, Any]:
    """NiFi version and whether it is the expected 2.7.x release. Call this first.

    Rejects NiFi 1.x and any 2.x line other than 2.7. Other 2.7 patches warn and continue.
    """
    return _nifi().about()


@mcp.tool()
def nifi_diagnostics() -> dict[str, Any]:
    """Heap and repository usage summary. Does not change the flow."""
    return _nifi().diagnostics()


@mcp.tool()
def nifi_get_flow(group_id: str = "root") -> dict[str, Any]:
    """Read a process group: child groups, processors, connections, and queue depth.

    Use a child group id when the canvas is large. ``root`` is the top group.
    Queue fields are observational. This tool does not empty queues.
    """
    return _nifi().get_flow(group_id)


@mcp.tool()
def nifi_get_processor(processor_id: str) -> dict[str, Any]:
    """Read one processor, including validation errors. Sensitive property values are omitted."""
    return _nifi().get_processor(processor_id)


@mcp.tool()
def nifi_search(query: str) -> dict[str, Any]:
    """Search the flow by name. Read-only."""
    return _nifi().search(query)


@mcp.tool()
def nifi_list_processor_types(query: str | None = None, limit: int = 40) -> dict[str, Any]:
    """Find processor types on this NiFi 2.7 node. Pass a short query such as ``GenerateFlowFile``.

    The returned bundle belongs to the running NARs. Use that type string when creating a processor.
    """
    return _nifi().list_processor_types(query, limit)


@mcp.tool()
def nifi_get_processor_definition(processor_type: str) -> dict[str, Any]:
    """Property names and relationships for one processor type on this 2.7 node.

    Call this before setting properties. NiFi 2.7 renamed many property names.
    """
    return _nifi().get_processor_definition(processor_type)


@mcp.tool()
def nifi_list_controller_service_types(query: str | None = None, limit: int = 40) -> dict[str, Any]:
    """Find controller service types on this NiFi 2.7 node."""
    return _nifi().list_controller_service_types(query, limit)


@mcp.tool()
def nifi_get_connection(connection_id: str) -> dict[str, Any]:
    """Read a connection and its queue depth. Does not list or drop FlowFiles."""
    return _nifi().get_connection(connection_id)


@mcp.tool()
def nifi_get_bulletins(limit: int = 20) -> dict[str, Any]:
    """Recent bulletin-board messages."""
    return _nifi().bulletins(limit)


@mcp.tool()
def nifi_list_parameter_contexts() -> dict[str, Any]:
    """List parameter contexts. Sensitive parameter values are not returned."""
    return _nifi().list_parameter_contexts()


@mcp.tool()
def nifi_get_parameter_context(context_id: str) -> dict[str, Any]:
    """Read one parameter context. Sensitive values are omitted."""
    return _nifi().get_parameter_context(context_id)


@mcp.tool()
def nifi_list_registry_buckets() -> dict[str, Any]:
    """Read NiFi Registry buckets. This server does not create or version flows in Registry."""
    return _registry_client().list_buckets()


@mcp.tool()
def nifi_list_registry_flows(bucket_id: str) -> dict[str, Any]:
    """Read flow names in one Registry bucket. Does not import or commit a version."""
    return _registry_client().list_flows(bucket_id)


@mcp.tool()
def nifi_create_process_group(
    name: str,
    parent_id: str = "root",
    x: float = 0,
    y: float = 0,
    comments: str = "",
) -> dict[str, Any]:
    """Create an empty process group. Build flows inside a child group, not on the root canvas.

    Does not start the group.
    """
    return _nifi().create_process_group(parent_id, name, x=x, y=y, comments=comments)


@mcp.tool()
def nifi_update_process_group(
    group_id: str,
    name: str | None = None,
    comments: str | None = None,
    x: float | None = None,
    y: float | None = None,
) -> dict[str, Any]:
    """Rename or move a process group. Does not start or stop it.

    To attach parameters, use nifi_bind_parameter_context.
    """
    return _nifi().update_process_group(
        group_id, name=name, comments=comments, x=x, y=y
    )


@mcp.tool()
def nifi_delete_process_group(group_id: str) -> dict[str, Any]:
    """Delete a process group. If NiFi rejects a running group, stop it in the UI first.

    This tool will not stop the group for you.
    """
    return _nifi().delete_process_group(group_id)


@mcp.tool()
def nifi_create_processor(
    processor_type: str,
    name: str,
    parent_id: str,
    x: float = 0,
    y: float = 0,
) -> dict[str, Any]:
    """Add a processor. ``processor_type`` is the full Java type from nifi_list_processor_types.

    The NAR bundle is taken from this NiFi 2.7 node. The processor is left stopped.
    Configure it with nifi_update_processor. Do not pass a run state.
    """
    return _nifi().create_processor(parent_id, processor_type, name, x=x, y=y)


@mcp.tool()
def nifi_update_processor(
    processor_id: str,
    name: str | None = None,
    properties: dict[str, str | None] | None = None,
    scheduling_period: str | None = None,
    scheduling_strategy: str | None = None,
    auto_terminated_relationships: list[str] | None = None,
    comments: str | None = None,
) -> dict[str, Any]:
    """Update name, properties, scheduling, or auto-terminated relationships.

    Property names must exist on the live 2.7 component unless the type allows dynamic properties.
    Unknown names are refused before the PUT. This tool never sends ``state`` and does not start
    or stop the processor.
    """
    return _nifi().update_processor(
        processor_id,
        name=name,
        properties=properties,
        scheduling_period=scheduling_period,
        scheduling_strategy=scheduling_strategy,
        auto_terminated_relationships=auto_terminated_relationships,
        comments=comments,
    )


@mcp.tool()
def nifi_delete_processor(processor_id: str) -> dict[str, Any]:
    """Delete a processor. A running processor must be stopped in the NiFi UI first."""
    return _nifi().delete_processor(processor_id)


@mcp.tool()
def nifi_create_connection(
    parent_id: str,
    source_id: str,
    destination_id: str,
    relationships: list[str],
    name: str = "",
    source_type: str = "PROCESSOR",
    destination_type: str = "PROCESSOR",
    back_pressure_object_threshold: int | None = None,
    back_pressure_data_size_threshold: str | None = None,
) -> dict[str, Any]:
    """Connect two components that already live in ``parent_id``.

    Processor sources need ``relationships`` such as ``["success"]``. Port endpoints use
    ``source_type`` / ``destination_type`` of INPUT_PORT or OUTPUT_PORT and an empty relationship list.
    """
    return _nifi().create_connection(
        parent_id,
        source_id,
        destination_id,
        relationships,
        name=name,
        source_type=source_type,
        destination_type=destination_type,
        back_pressure_object_threshold=back_pressure_object_threshold,
        back_pressure_data_size_threshold=back_pressure_data_size_threshold,
    )


@mcp.tool()
def nifi_update_connection(
    connection_id: str,
    name: str | None = None,
    relationships: list[str] | None = None,
    back_pressure_object_threshold: int | None = None,
    back_pressure_data_size_threshold: str | None = None,
) -> dict[str, Any]:
    """Change relationships, name, or back-pressure thresholds. Does not empty the queue."""
    return _nifi().update_connection(
        connection_id,
        name=name,
        relationships=relationships,
        back_pressure_object_threshold=back_pressure_object_threshold,
        back_pressure_data_size_threshold=back_pressure_data_size_threshold,
    )


@mcp.tool()
def nifi_delete_connection(connection_id: str) -> dict[str, Any]:
    """Delete a connection. Does not drop queued FlowFiles first; NiFi rejects a non-empty queue."""
    return _nifi().delete_connection(connection_id)


@mcp.tool()
def nifi_create_controller_service(
    service_type: str,
    name: str,
    parent_id: str,
) -> dict[str, Any]:
    """Create a disabled controller service. The bundle comes from this NiFi 2.7 node.

    Configure properties with nifi_update_controller_service. Enabling is not available here;
    enable the service in the NiFi UI after it is valid.
    """
    return _nifi().create_controller_service(parent_id, service_type, name)


@mcp.tool()
def nifi_update_controller_service(
    service_id: str,
    name: str | None = None,
    properties: dict[str, str | None] | None = None,
    comments: str | None = None,
) -> dict[str, Any]:
    """Update controller service properties. Does not enable or disable the service.

    Property names are checked against the service descriptors before the PUT.
    """
    return _nifi().update_controller_service(
        service_id, name=name, properties=properties, comments=comments
    )


@mcp.tool()
def nifi_delete_controller_service(service_id: str) -> dict[str, Any]:
    """Delete a controller service. NiFi only deletes a disabled service that nothing references."""
    return _nifi().delete_controller_service(service_id)


@mcp.tool()
def nifi_create_parameter_context(
    name: str,
    description: str = "",
    parameters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create a parameter context. Each parameter is ``name``, ``value``, ``sensitive``, ``description``.

    The response omits sensitive values. There is no variable registry in NiFi 2.
    """
    return _nifi().create_parameter_context(
        name, description=description, parameters=parameters
    )


@mcp.tool()
def nifi_update_parameter_context(
    context_id: str,
    parameters: list[dict[str, Any]],
) -> dict[str, Any]:
    """Replace the parameter list through NiFi's async update-request.

    Polls until the request finishes, then deletes the request. A parameter object with only
    ``name`` removes that parameter. NiFi may restart referencing components while it applies
    the change; this server does not call run-status itself. Sensitive values are not returned.
    """
    return _nifi().update_parameter_context(context_id, parameters)


@mcp.tool()
def nifi_delete_parameter_context(context_id: str) -> dict[str, Any]:
    """Delete a parameter context that no process group still references."""
    return _nifi().delete_parameter_context(context_id)


@mcp.tool()
def nifi_bind_parameter_context(group_id: str, context_id: str) -> dict[str, Any]:
    """Bind a parameter context to a process group so ``#{name}`` references resolve.

    Does not start the group.
    """
    return _nifi().bind_parameter_context(group_id, context_id)


@mcp.tool()
def nifi_apply_flow_spec(spec: dict[str, Any], parent_id: str = "root") -> dict[str, Any]:
    """Create a child process group from one spec and leave every component stopped.

    ``spec.process_group.name`` is required. ``spec.objects`` may contain
    ``controller_service``, ``processor``, and ``connection`` entries, in that dependency order.
    A property value ``@Name`` points at a controller service created earlier in the same spec.
    Unknown keys are refused before anything is created. ``state``, ``run_status``, and
    ``drop_request`` are refused. Prefer this over many separate create calls.

    Example object keys: processor_type, name, properties, auto_terminated, scheduling_period;
    service_type; source, target, relationships.
    """
    return _nifi().apply_flow_spec(spec, parent_id)


_REGISTRY_TOOLS = ("nifi_list_registry_buckets", "nifi_list_registry_flows")


def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    if not Settings().nifi_registry_enabled:
        for name in _REGISTRY_TOOLS:
            mcp.remove_tool(name)
    mcp.run(transport="stdio")
