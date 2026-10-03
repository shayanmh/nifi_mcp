"""NiFi 2.7 REST client.

REST paths follow Apache NiFi ``nifi-web-api`` the same way
newen-systems/nifi-mcp documents them. This client never calls
``run-status`` or queue ``drop-requests``.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from typing import Any
from urllib.parse import quote

import httpx

from nifi_mcp.config import Settings

logger = logging.getLogger(__name__)

_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")

_PROCESSOR_CONFIG_KEYS = (
    "properties",
    "schedulingPeriod",
    "schedulingStrategy",
    "executionNode",
    "penaltyDuration",
    "yieldDuration",
    "bulletinLevel",
    "runDurationMillis",
    "concurrentlySchedulableTaskCount",
    "comments",
    "autoTerminatedRelationships",
    "annotationData",
    "retryCount",
    "retriedRelationships",
    "backoffMechanism",
    "maxBackoffPeriod",
)

_FLOW_SPEC_KEYS = {"process_group", "objects"}
_GROUP_KEYS = {"name", "comments"}
_SERVICE_KEYS = {"type", "service_type", "name", "properties"}
_PROCESSOR_KEYS = {
    "type",
    "processor_type",
    "name",
    "properties",
    "scheduling_period",
    "scheduling_strategy",
    "auto_terminated",
    "comments",
    "x",
    "y",
}
_CONNECTION_KEYS = {
    "type",
    "source",
    "target",
    "relationships",
    "name",
    "back_pressure_object_threshold",
    "back_pressure_data_size_threshold",
}
_FORBIDDEN_SPEC_KEYS = {"state", "run_status", "scheduled_state", "drop_request"}


class NiFiError(Exception):
    """Base error for NiFi calls. Messages never include submitted secrets."""


class UnsupportedNiFiVersion(NiFiError):
    """The running NiFi is outside the supported 2.7 series."""


class NiFiAuthError(NiFiError):
    """Token login failed."""


class NiFiRequestError(NiFiError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def parse_version(version: str) -> tuple[int, int, int]:
    match = _VERSION_RE.match(version.strip())
    if not match:
        raise UnsupportedNiFiVersion(f"Unrecognised NiFi version {version!r}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def evaluate_version(actual: str, expected: str) -> str | None:
    """Return a warning for other 2.7 patches, or raise outside that series."""
    actual_parts = parse_version(actual)
    expected_parts = parse_version(expected)
    if actual_parts[0] != 2:
        raise UnsupportedNiFiVersion(
            f"NiFi {actual} is not supported. This server only talks to NiFi 2.x "
            f"(expected {expected})."
        )
    if (actual_parts[0], actual_parts[1]) != (expected_parts[0], expected_parts[1]):
        raise UnsupportedNiFiVersion(
            f"NiFi {actual} is outside the {expected_parts[0]}.{expected_parts[1]} "
            f"series. Expected {expected}."
        )
    actual_core = actual.split("-", 1)[0]
    expected_core = expected.split("-", 1)[0]
    if actual_core != expected_core:
        return (
            f"Connected to NiFi {actual}; expected {expected}. "
            f"The {expected_parts[0]}.{expected_parts[1]} series is accepted with this warning."
        )
    return None


def redact_parameter_entity(entity: dict[str, Any]) -> dict[str, Any]:
    """Drop values of sensitive parameters. Non-sensitive values stay."""
    component = entity.get("component") or entity
    parameters: list[dict[str, Any]] = []
    for entry in component.get("parameters") or []:
        raw = entry.get("parameter") if isinstance(entry, dict) and "parameter" in entry else entry
        if not isinstance(raw, dict):
            continue
        cleaned = {
            "name": raw.get("name"),
            "sensitive": bool(raw.get("sensitive")),
            "description": raw.get("description") or "",
        }
        if not cleaned["sensitive"] and "value" in raw:
            cleaned["value"] = raw.get("value")
        parameters.append(cleaned)
    return {
        "id": entity.get("id") or component.get("id"),
        "name": component.get("name"),
        "description": component.get("description") or "",
        "parameters": parameters,
    }


def validate_flow_spec(spec: dict[str, Any]) -> None:
    """Refuse unknown or operational keys before any NiFi write."""
    if not isinstance(spec, dict):
        raise NiFiError("Flow spec must be an object")
    _reject_unknown(spec, _FLOW_SPEC_KEYS, "flow spec")
    group = spec.get("process_group")
    if not isinstance(group, dict) or not group.get("name"):
        raise NiFiError("Flow spec process_group.name is required")
    _reject_unknown(group, _GROUP_KEYS, "process_group")
    objects = spec.get("objects")
    if not isinstance(objects, list):
        raise NiFiError("Flow spec objects must be a list")
    for index, obj in enumerate(objects):
        if not isinstance(obj, dict):
            raise NiFiError(f"objects[{index}] must be an object")
        forbidden = _FORBIDDEN_SPEC_KEYS.intersection(obj)
        if forbidden:
            raise NiFiError(
                f"objects[{index}] includes {sorted(forbidden)}. "
                "This server does not start, stop, or empty queues."
            )
        kind = obj.get("type")
        if kind == "controller_service":
            _reject_unknown(obj, _SERVICE_KEYS, f"objects[{index}]")
            if not obj.get("service_type") or not obj.get("name"):
                raise NiFiError(f"objects[{index}] needs service_type and name")
        elif kind == "processor":
            _reject_unknown(obj, _PROCESSOR_KEYS, f"objects[{index}]")
            if not obj.get("processor_type") or not obj.get("name"):
                raise NiFiError(f"objects[{index}] needs processor_type and name")
        elif kind == "connection":
            _reject_unknown(obj, _CONNECTION_KEYS, f"objects[{index}]")
            if not obj.get("source") or not obj.get("target"):
                raise NiFiError(f"objects[{index}] needs source and target")
        else:
            raise NiFiError(
                f"objects[{index}] type must be controller_service, processor, or connection"
            )


class NiFiClient:
    """Synchronous NiFi 2.7 client. One ``clientId`` for the process lifetime."""

    def __init__(self, settings: Settings, http: httpx.Client | None = None) -> None:
        self.settings = settings
        self.client_id = str(uuid.uuid4())
        self.version_warning: str | None = None
        self.actual_version: str | None = None
        self._about: dict[str, Any] | None = None
        self._version_checked = False
        self._token: str | None = None
        self._http = http or httpx.Client(
            verify=settings.nifi_verify_ssl,
            timeout=settings.nifi_timeout_seconds,
            trust_env=settings.nifi_use_system_proxy,
        )

    def close(self) -> None:
        self._http.close()

    def _url(self, path: str) -> str:
        return f"{self.settings.nifi_api_url}/{path.lstrip('/')}"

    def _login(self) -> None:
        response = self._http.post(
            self._url("/access/token"),
            data={
                "username": self.settings.nifi_username,
                "password": self.settings.nifi_password,
            },
            headers={"Accept": "text/plain"},
        )
        if response.status_code not in {200, 201}:
            raise NiFiAuthError(
                "POST /access/token failed. Check NIFI_USERNAME / NIFI_PASSWORD."
            )
        token = response.text.strip()
        if not token:
            raise NiFiAuthError("POST /access/token returned an empty body")
        self._token = token

    def request(
        self,
        method: str,
        path: str,
        *,
        skip_version: bool = False,
        **kwargs: Any,
    ) -> Any:
        if self._token is None:
            self._login()
        if not skip_version and not self._version_checked:
            self.ensure_version()
        response = self._send(method, path, **kwargs)
        if response.status_code == 401:
            self._login()
            response = self._send(method, path, **kwargs)
        if response.status_code >= 400:
            raise self._request_error(method, path, response)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        headers.update(self._csrf_header())
        extra = kwargs.pop("headers", None) or {}
        headers.update(extra)
        return self._http.request(method, self._url(path), headers=headers, **kwargs)

    def _csrf_header(self) -> dict[str, str]:
        """NiFi 2.7 rejects writes that do not echo the request-token cookie."""
        for cookie in self._http.cookies.jar:
            if cookie.name == "__Secure-Request-Token":
                return {"Request-Token": cookie.value}
        return {}

    def _request_error(self, method: str, path: str, response: httpx.Response) -> NiFiRequestError:
        detail = _error_detail(response)
        if response.status_code == 409:
            detail = (
                f"{detail} If the component is running, stop it in the NiFi UI. "
                "This server does not change run status."
            )
        return NiFiRequestError(
            f"{method} {path} failed ({response.status_code}): {detail}",
            status_code=response.status_code,
        )

    def ensure_version(self) -> dict[str, Any]:
        if self._version_checked and self._about is not None:
            return self._about
        payload = self.request("GET", "/flow/about", skip_version=True)
        version = (payload.get("about") or {}).get("version")
        if not isinstance(version, str) or not version:
            raise NiFiError("NiFi /flow/about did not include a version")
        self.version_warning = evaluate_version(version, self.settings.nifi_expected_version)
        if self.version_warning:
            logger.warning("%s", self.version_warning)
        self.actual_version = version.split("-", 1)[0]
        self._about = payload
        self._version_checked = True
        return payload

    def about(self) -> dict[str, Any]:
        payload = self.ensure_version()
        about = payload.get("about") or {}
        return {
            "title": about.get("title"),
            "version": about.get("version"),
            "expected_version": self.settings.nifi_expected_version,
            "version_warning": self.version_warning,
            "uri": about.get("uri"),
            "timezone": about.get("timezone"),
        }

    def diagnostics(self) -> dict[str, Any]:
        payload = self.request("GET", "/system-diagnostics")
        snapshot = (payload.get("systemDiagnostics") or {}).get("aggregateSnapshot") or {}
        return {
            "version": self.actual_version,
            "heap_used": snapshot.get("usedHeap"),
            "heap_total": snapshot.get("totalHeap"),
            "heap_utilization": snapshot.get("heapUtilization"),
            "available_processors": snapshot.get("availableProcessors"),
            "flowfile_repository": snapshot.get("flowFileRepositoryStorageUsage"),
            "content_repository": snapshot.get("contentRepositoryStorageUsage"),
            "provenance_repository": snapshot.get("provenanceRepositoryStorageUsage"),
        }

    def get_flow(self, group_id: str = "root") -> dict[str, Any]:
        payload = self.request("GET", f"/flow/process-groups/{group_id}")
        return _summarize_flow(payload)

    def get_processor(self, processor_id: str) -> dict[str, Any]:
        return _summarize_processor(self.request("GET", f"/processors/{processor_id}"))

    def search(self, query: str) -> dict[str, Any]:
        payload = self.request("GET", "/flow/search-results", params={"q": query})
        results = payload.get("searchResultsDTO") or payload
        return {
            "processors": _search_hits(results.get("processorResults")),
            "connections": _search_hits(results.get("connectionResults")),
            "process_groups": _search_hits(results.get("processGroupResults")),
            "controller_services": _search_hits(results.get("controllerServiceResults")),
            "parameter_contexts": _search_hits(results.get("parameterContextResults")),
        }

    def list_processor_types(self, query: str | None = None, limit: int = 40) -> dict[str, Any]:
        return self._list_types("/flow/processor-types", "processorTypes", query, limit)

    def list_controller_service_types(
        self, query: str | None = None, limit: int = 40
    ) -> dict[str, Any]:
        return self._list_types(
            "/flow/controller-service-types", "controllerServiceTypes", query, limit
        )

    def get_processor_definition(self, processor_type: str) -> dict[str, Any]:
        matches = self._type_matches("/flow/processor-types", "processorTypes", processor_type)
        bundle = self._select_bundle(matches, processor_type)
        definition = self.request(
            "GET", _definition_path("processor", bundle, processor_type)
        )
        body = definition.get("processorDefinition") or definition
        return _summarize_definition(processor_type, bundle, body)

    def get_connection(self, connection_id: str) -> dict[str, Any]:
        return _summarize_connection(self.request("GET", f"/connections/{connection_id}"))

    def bulletins(self, limit: int = 20) -> dict[str, Any]:
        payload = self.request("GET", "/flow/bulletin-board", params={"limit": limit})
        board = payload.get("bulletinBoard") or {}
        items = []
        for entry in board.get("bulletins") or []:
            bulletin = entry.get("bulletin") or {}
            items.append(
                {
                    "id": entry.get("id") or bulletin.get("id"),
                    "level": bulletin.get("level"),
                    "source": bulletin.get("sourceName"),
                    "message": bulletin.get("message"),
                    "timestamp": bulletin.get("timestamp"),
                    "group_id": entry.get("groupId") or bulletin.get("groupId"),
                }
            )
        return {"bulletins": items}

    def list_parameter_contexts(self) -> dict[str, Any]:
        payload = self.request("GET", "/flow/parameter-contexts")
        contexts = [
            redact_parameter_entity(entity)
            for entity in payload.get("parameterContexts") or []
        ]
        return {"parameter_contexts": contexts}

    def get_parameter_context(self, context_id: str) -> dict[str, Any]:
        return redact_parameter_entity(self.request("GET", f"/parameter-contexts/{context_id}"))

    def create_process_group(
        self,
        parent_id: str,
        name: str,
        *,
        x: float = 0,
        y: float = 0,
        comments: str = "",
    ) -> dict[str, Any]:
        body = self._entity(
            0,
            {
                "name": name,
                "position": {"x": x, "y": y},
                "comments": comments,
            },
        )
        created = self.request("POST", f"/process-groups/{parent_id}/process-groups", json=body)
        return _summarize_group(created)

    def update_process_group(
        self,
        group_id: str,
        *,
        name: str | None = None,
        comments: str | None = None,
        x: float | None = None,
        y: float | None = None,
    ) -> dict[str, Any]:
        current = self.request("GET", f"/process-groups/{group_id}")
        component = current.get("component") or {}
        updated: dict[str, Any] = {"id": group_id, "name": name or component.get("name")}
        if comments is not None:
            updated["comments"] = comments
        if x is not None or y is not None:
            position = dict(component.get("position") or {"x": 0, "y": 0})
            if x is not None:
                position["x"] = x
            if y is not None:
                position["y"] = y
            updated["position"] = position
        body = self._entity(_version_of(current), updated)
        return _summarize_group(self.request("PUT", f"/process-groups/{group_id}", json=body))

    def delete_process_group(self, group_id: str) -> dict[str, Any]:
        self._delete_entity(f"/process-groups/{group_id}")
        return {"outcome": "applied", "id": group_id, "deleted": True}

    def create_processor(
        self,
        parent_id: str,
        processor_type: str,
        name: str,
        *,
        x: float = 0,
        y: float = 0,
    ) -> dict[str, Any]:
        bundle = self._bundle_for_processor(processor_type)
        body = self._entity(
            0,
            {
                "type": processor_type,
                "bundle": _bundle_body(bundle),
                "name": name,
                "position": {"x": x, "y": y},
            },
        )
        created = self.request("POST", f"/process-groups/{parent_id}/processors", json=body)
        return _summarize_processor(created)

    def update_processor(
        self,
        processor_id: str,
        *,
        name: str | None = None,
        properties: dict[str, str | None] | None = None,
        scheduling_period: str | None = None,
        scheduling_strategy: str | None = None,
        auto_terminated_relationships: list[str] | None = None,
        comments: str | None = None,
    ) -> dict[str, Any]:
        current = self.request("GET", f"/processors/{processor_id}")
        component = current.get("component") or {}
        config = _copy_config(component.get("config") or {})
        if properties:
            descriptors = (component.get("config") or {}).get("descriptors") or {}
            self._check_properties(
                properties,
                descriptors,
                dynamic=self._processor_allows_dynamic(component, properties, descriptors),
            )
            merged = dict(config.get("properties") or {})
            merged.update(properties)
            config["properties"] = merged
        if scheduling_period is not None:
            config["schedulingPeriod"] = scheduling_period
        if scheduling_strategy is not None:
            config["schedulingStrategy"] = scheduling_strategy
        if auto_terminated_relationships is not None:
            known = {item.get("name") for item in component.get("relationships") or []}
            unknown = [name for name in auto_terminated_relationships if known and name not in known]
            if unknown:
                raise NiFiError(f"Unknown relationships {unknown}. Known: {sorted(known)}")
            config["autoTerminatedRelationships"] = auto_terminated_relationships
        if comments is not None:
            config["comments"] = comments
        updated = {
            "id": processor_id,
            "name": name or component.get("name"),
            "config": config,
        }
        body = self._entity(_version_of(current), updated)
        if "state" in body["component"] or "state" in body["component"].get("config", {}):
            raise NiFiError("Refusing to send processor state")
        return _summarize_processor(self.request("PUT", f"/processors/{processor_id}", json=body))

    def delete_processor(self, processor_id: str) -> dict[str, Any]:
        self._delete_entity(f"/processors/{processor_id}")
        return {"outcome": "applied", "id": processor_id, "deleted": True}

    def create_connection(
        self,
        parent_id: str,
        source_id: str,
        destination_id: str,
        relationships: list[str],
        *,
        name: str = "",
        source_type: str = "PROCESSOR",
        destination_type: str = "PROCESSOR",
        back_pressure_object_threshold: int | None = None,
        back_pressure_data_size_threshold: str | None = None,
    ) -> dict[str, Any]:
        component: dict[str, Any] = {
            "name": name,
            "source": {"id": source_id, "groupId": parent_id, "type": source_type},
            "destination": {
                "id": destination_id,
                "groupId": parent_id,
                "type": destination_type,
            },
            "selectedRelationships": relationships,
        }
        if back_pressure_object_threshold is not None:
            component["backPressureObjectThreshold"] = back_pressure_object_threshold
        if back_pressure_data_size_threshold is not None:
            component["backPressureDataSizeThreshold"] = back_pressure_data_size_threshold
        created = self.request(
            "POST",
            f"/process-groups/{parent_id}/connections",
            json=self._entity(0, component),
        )
        return _summarize_connection(created)

    def update_connection(
        self,
        connection_id: str,
        *,
        name: str | None = None,
        relationships: list[str] | None = None,
        back_pressure_object_threshold: int | None = None,
        back_pressure_data_size_threshold: str | None = None,
    ) -> dict[str, Any]:
        current = self.request("GET", f"/connections/{connection_id}")
        component = dict(current.get("component") or {})
        component.pop("state", None)
        updated: dict[str, Any] = {
            "id": connection_id,
            "source": component.get("source"),
            "destination": component.get("destination"),
            "selectedRelationships": relationships
            if relationships is not None
            else component.get("selectedRelationships") or [],
        }
        if name is not None:
            updated["name"] = name
        elif component.get("name") is not None:
            updated["name"] = component.get("name")
        if back_pressure_object_threshold is not None:
            updated["backPressureObjectThreshold"] = back_pressure_object_threshold
        elif component.get("backPressureObjectThreshold") is not None:
            updated["backPressureObjectThreshold"] = component.get("backPressureObjectThreshold")
        if back_pressure_data_size_threshold is not None:
            updated["backPressureDataSizeThreshold"] = back_pressure_data_size_threshold
        elif component.get("backPressureDataSizeThreshold") is not None:
            updated["backPressureDataSizeThreshold"] = component.get(
                "backPressureDataSizeThreshold"
            )
        body = self._entity(_version_of(current), updated)
        return _summarize_connection(
            self.request("PUT", f"/connections/{connection_id}", json=body)
        )

    def delete_connection(self, connection_id: str) -> dict[str, Any]:
        self._delete_entity(f"/connections/{connection_id}")
        return {"outcome": "applied", "id": connection_id, "deleted": True}

    def create_controller_service(
        self,
        parent_id: str,
        service_type: str,
        name: str,
        *,
        x: float = 0,
        y: float = 0,
    ) -> dict[str, Any]:
        bundle = self._bundle_for_service(service_type)
        body = self._entity(
            0,
            {
                "name": name,
                "type": service_type,
                "bundle": _bundle_body(bundle),
                "position": {"x": x, "y": y},
            },
        )
        created = self.request(
            "POST",
            f"/process-groups/{parent_id}/controller-services",
            json=body,
        )
        return _summarize_service(created)

    def update_controller_service(
        self,
        service_id: str,
        *,
        name: str | None = None,
        properties: dict[str, str | None] | None = None,
        comments: str | None = None,
    ) -> dict[str, Any]:
        current = self.request("GET", f"/controller-services/{service_id}")
        component = current.get("component") or {}
        updated: dict[str, Any] = {
            "id": service_id,
            "name": name or component.get("name"),
        }
        if properties:
            descriptors = component.get("descriptors") or {}
            self._check_properties(properties, descriptors, dynamic=False)
            merged = dict(component.get("properties") or {})
            merged.update(properties)
            updated["properties"] = merged
        if comments is not None:
            updated["comments"] = comments
        body = self._entity(_version_of(current), updated)
        if "state" in body["component"]:
            raise NiFiError("Refusing to send controller service state")
        return _summarize_service(
            self.request("PUT", f"/controller-services/{service_id}", json=body)
        )

    def delete_controller_service(self, service_id: str) -> dict[str, Any]:
        self._delete_entity(f"/controller-services/{service_id}")
        return {"outcome": "applied", "id": service_id, "deleted": True}

    def create_parameter_context(
        self,
        name: str,
        *,
        description: str = "",
        parameters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        body = self._entity(
            0,
            {
                "name": name,
                "description": description,
                "parameters": [_parameter_entry(item) for item in parameters or []],
            },
        )
        created = self.request("POST", "/parameter-contexts", json=body)
        return redact_parameter_entity(created)

    def update_parameter_context(
        self,
        context_id: str,
        parameters: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Submit an async update-request, poll to 100 percent, then delete it.

        NiFi itself may restart referencing components while applying parameters.
        This method does not call run-status.
        """
        current = self.request("GET", f"/parameter-contexts/{context_id}")
        body = {
            "id": context_id,
            "revision": self._revision(_version_of(current)),
            "disconnectedNodeAcknowledged": False,
            "component": {
                "id": context_id,
                "parameters": [_parameter_entry(item) for item in parameters],
            },
        }
        started = self.request(
            "POST", f"/parameter-contexts/{context_id}/update-requests", json=body
        )
        request_id = (started.get("request") or {}).get("requestId")
        if not request_id:
            raise NiFiError("Parameter update-request did not return a requestId")
        deadline = time.monotonic() + self.settings.nifi_timeout_seconds
        failure: str | None = None
        try:
            while True:
                status = self.request(
                    "GET",
                    f"/parameter-contexts/{context_id}/update-requests/{request_id}",
                )
                ticket = status.get("request") or {}
                if ticket.get("complete") or ticket.get("percentCompleted") == 100:
                    if ticket.get("failureReason"):
                        failure = str(ticket["failureReason"])
                    break
                if time.monotonic() > deadline:
                    failure = "Parameter context update timed out"
                    break
                time.sleep(0.25)
        finally:
            self.request(
                "DELETE",
                f"/parameter-contexts/{context_id}/update-requests/{request_id}",
            )
        if failure:
            raise NiFiError(failure)
        return self.get_parameter_context(context_id)

    def delete_parameter_context(self, context_id: str) -> dict[str, Any]:
        self._delete_entity(f"/parameter-contexts/{context_id}")
        return {"outcome": "applied", "id": context_id, "deleted": True}

    def bind_parameter_context(self, group_id: str, context_id: str) -> dict[str, Any]:
        current = self.request("GET", f"/process-groups/{group_id}")
        component = current.get("component") or {}
        body = self._entity(
            _version_of(current),
            {
                "id": group_id,
                "name": component.get("name"),
                "parameterContext": {"id": context_id},
            },
        )
        bound = self.request("PUT", f"/process-groups/{group_id}", json=body)
        summary = _summarize_group(bound)
        summary["parameter_context_id"] = context_id
        summary["outcome"] = "applied"
        return summary

    def apply_flow_spec(self, spec: dict[str, Any], parent_id: str = "root") -> dict[str, Any]:
        """Create a child process group and the objects in ``spec``. Never schedules them."""
        validate_flow_spec(spec)
        group = self.create_process_group(
            parent_id,
            spec["process_group"]["name"],
            comments=spec["process_group"].get("comments") or "",
        )
        group_id = group["id"]
        created: dict[str, str] = {}
        try:
            y = 0.0
            for obj in spec["objects"]:
                kind = obj["type"]
                if kind == "controller_service":
                    service = self.create_controller_service(
                        group_id, obj["service_type"], obj["name"]
                    )
                    created[obj["name"]] = service["id"]
                    if obj.get("properties"):
                        self.update_controller_service(
                            service["id"],
                            properties=_resolve_refs(obj["properties"], created),
                        )
                elif kind == "processor":
                    processor = self.create_processor(
                        group_id,
                        obj["processor_type"],
                        obj["name"],
                        x=float(obj.get("x", 0)),
                        y=float(obj.get("y", y)),
                    )
                    created[obj["name"]] = processor["id"]
                    y += 160.0
                    if any(
                        obj.get(key) is not None
                        for key in (
                            "properties",
                            "scheduling_period",
                            "scheduling_strategy",
                            "auto_terminated",
                            "comments",
                        )
                    ):
                        self.update_processor(
                            processor["id"],
                            properties=_resolve_refs(obj.get("properties"), created),
                            scheduling_period=obj.get("scheduling_period"),
                            scheduling_strategy=obj.get("scheduling_strategy"),
                            auto_terminated_relationships=obj.get("auto_terminated"),
                            comments=obj.get("comments"),
                        )
                else:
                    source_id = created.get(obj["source"])
                    target_id = created.get(obj["target"])
                    if not source_id or not target_id:
                        raise NiFiError(
                            f"Connection {obj['source']} -> {obj['target']} "
                            "must follow components created in this spec"
                        )
                    connection = self.create_connection(
                        group_id,
                        source_id,
                        target_id,
                        obj.get("relationships") or [],
                        name=obj.get("name") or "",
                        back_pressure_object_threshold=obj.get("back_pressure_object_threshold"),
                        back_pressure_data_size_threshold=obj.get(
                            "back_pressure_data_size_threshold"
                        ),
                    )
                    if obj.get("name"):
                        created[obj["name"]] = connection["id"]
        except NiFiError:
            raise
        except Exception as exc:
            raise NiFiError(f"Flow spec failed inside process group {group_id}") from exc
        return {
            "outcome": "applied",
            "process_group_id": group_id,
            "components": created,
        }

    def _entity(self, version: int, component: dict[str, Any]) -> dict[str, Any]:
        component = {key: value for key, value in component.items() if key != "state"}
        config = component.get("config")
        if isinstance(config, dict):
            component["config"] = {key: value for key, value in config.items() if key != "state"}
        return {
            "revision": self._revision(version),
            "disconnectedNodeAcknowledged": False,
            "component": component,
        }

    def _revision(self, version: int) -> dict[str, Any]:
        return {"clientId": self.client_id, "version": version}

    def _delete_entity(self, path: str) -> None:
        current = self.request("GET", path)
        self.request(
            "DELETE",
            path,
            params={
                "version": _version_of(current),
                "clientId": self.client_id,
                "disconnectedNodeAcknowledged": False,
            },
        )

    def _list_types(
        self, path: str, key: str, query: str | None, limit: int
    ) -> dict[str, Any]:
        payload = self.request("GET", path)
        items = []
        needle = (query or "").casefold()
        for entry in payload.get(key) or []:
            if needle and needle not in _type_haystack(entry):
                continue
            bundle = entry.get("bundle") or {}
            items.append(
                {
                    "type": entry.get("type"),
                    "bundle": _bundle_body(bundle),
                    "description": entry.get("description") or "",
                    "tags": entry.get("tags") or [],
                    "restricted": bool(entry.get("restricted")),
                }
            )
        return {"count": len(items), "types": items[:limit]}

    def _bundle_for_processor(self, processor_type: str) -> dict[str, Any]:
        matches = self._type_matches("/flow/processor-types", "processorTypes", processor_type)
        return self._select_bundle(matches, processor_type)

    def _bundle_for_service(self, service_type: str) -> dict[str, Any]:
        matches = self._type_matches(
            "/flow/controller-service-types", "controllerServiceTypes", service_type
        )
        return self._select_bundle(matches, service_type)

    def _type_matches(self, path: str, key: str, type_name: str) -> list[dict[str, Any]]:
        payload = self.request("GET", path)
        return [entry for entry in payload.get(key) or [] if entry.get("type") == type_name]

    def _select_bundle(self, matches: list[dict[str, Any]], type_name: str) -> dict[str, Any]:
        if not matches:
            raise NiFiError(f"No NiFi 2.7 bundle found for {type_name}")
        if len(matches) == 1:
            return matches[0]["bundle"]
        preferred = self.actual_version or self.settings.nifi_expected_version
        exact = [entry for entry in matches if (entry.get("bundle") or {}).get("version") == preferred]
        if len(exact) == 1:
            return exact[0]["bundle"]
        choices = [
            f"{(entry.get('bundle') or {}).get('artifact')}:"
            f"{(entry.get('bundle') or {}).get('version')}"
            for entry in matches
        ]
        raise NiFiError(f"Multiple bundles for {type_name}: {choices}")

    def _processor_allows_dynamic(
        self,
        component: dict[str, Any],
        properties: dict[str, Any],
        descriptors: dict[str, Any],
    ) -> bool:
        unknown = [name for name in properties if name not in descriptors]
        if not unknown:
            return False
        bundle = component.get("bundle") or {}
        type_name = component.get("type")
        if not type_name or not bundle:
            return False
        definition = self.request("GET", _definition_path("processor", bundle, type_name))
        body = definition.get("processorDefinition") or definition
        return bool(body.get("supportsDynamicProperties"))

    def _check_properties(
        self,
        properties: dict[str, Any],
        descriptors: dict[str, Any],
        *,
        dynamic: bool,
    ) -> None:
        unknown = [name for name in properties if name not in descriptors]
        if unknown and not dynamic:
            known = sorted(descriptors)
            raise NiFiError(
                f"Unknown properties {unknown}. Known: {known}. "
                "NiFi 2.7 renamed many properties; read the component definition first."
            )


def _reject_unknown(payload: dict[str, Any], allowed: set[str], label: str) -> None:
    extra = sorted(set(payload) - allowed)
    if extra:
        raise NiFiError(f"Unknown {label} keys: {extra}")


def _bundle_body(bundle: dict[str, Any]) -> dict[str, str]:
    return {
        "group": bundle["group"],
        "artifact": bundle["artifact"],
        "version": bundle["version"],
    }


def _definition_path(kind: str, bundle: dict[str, Any], type_name: str) -> str:
    group = quote(str(bundle["group"]), safe="")
    artifact = quote(str(bundle["artifact"]), safe="")
    version = quote(str(bundle["version"]), safe="")
    encoded_type = quote(type_name, safe="")
    return f"/flow/{kind}-definition/{group}/{artifact}/{version}/{encoded_type}"


def _version_of(entity: dict[str, Any]) -> int:
    revision = entity.get("revision") or {}
    version = revision.get("version")
    if version is None:
        raise NiFiError("NiFi entity did not include revision.version")
    return int(version)


def _copy_config(config: dict[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for key in _PROCESSOR_CONFIG_KEYS:
        if key in config and config[key] is not None:
            copied[key] = config[key]
    return copied


def _parameter_entry(item: dict[str, Any]) -> dict[str, Any]:
    parameter: dict[str, Any] = {
        "name": item["name"],
        "sensitive": bool(item.get("sensitive", False)),
    }
    if "value" in item and item["value"] is not None:
        parameter["value"] = item["value"]
    if item.get("description"):
        parameter["description"] = item["description"]
    return {"parameter": parameter}


def _resolve_refs(
    properties: dict[str, Any] | None, created: dict[str, str]
) -> dict[str, str | None] | None:
    if not properties:
        return None
    resolved: dict[str, str] = {}
    for name, value in properties.items():
        if isinstance(value, str) and value.startswith("@") and value[1:] in created:
            resolved[name] = created[value[1:]]
        elif value is None:
            resolved[name] = None  # type: ignore[assignment]
        else:
            resolved[name] = str(value)
    return resolved


def _error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and payload.get("message"):
        return str(payload["message"])[:500]
    text = response.text.strip()
    return text[:500] or response.reason_phrase


def _type_haystack(entry: dict[str, Any]) -> str:
    tags = " ".join(entry.get("tags") or [])
    return f"{entry.get('type', '')} {entry.get('description', '')} {tags}".casefold()


def _search_hits(items: Any) -> list[dict[str, Any]]:
    hits = []
    for item in items or []:
        hits.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "group_id": item.get("groupId") or item.get("parentGroupId"),
                "matches": item.get("matches") or [],
            }
        )
    return hits


def _summarize_flow(payload: dict[str, Any]) -> dict[str, Any]:
    wrapper = payload.get("processGroupFlow") or {}
    breadcrumb = (wrapper.get("breadcrumb") or {}).get("breadcrumb") or {}
    flow = wrapper.get("flow") or {}
    return {
        "id": wrapper.get("id"),
        "name": breadcrumb.get("name"),
        "processors": [_summarize_processor(item) for item in flow.get("processors") or []],
        "connections": [_summarize_connection(item) for item in flow.get("connections") or []],
        "process_groups": [_summarize_group(item) for item in flow.get("processGroups") or []],
        "controller_services": [
            _summarize_service(item) for item in flow.get("controllerServices") or []
        ],
        "input_ports": [_port(item) for item in flow.get("inputPorts") or []],
        "output_ports": [_port(item) for item in flow.get("outputPorts") or []],
    }


def _summarize_processor(entity: dict[str, Any]) -> dict[str, Any]:
    component = entity.get("component") or {}
    config = component.get("config") or {}
    descriptors = config.get("descriptors") or {}
    return {
        "id": entity.get("id") or component.get("id"),
        "name": component.get("name"),
        "type": component.get("type"),
        "bundle": component.get("bundle"),
        "parent_group_id": component.get("parentGroupId"),
        "state": component.get("state"),
        "validation_status": component.get("validationStatus"),
        "validation_errors": component.get("validationErrors") or [],
        "relationships": [item.get("name") for item in component.get("relationships") or []],
        "scheduling_period": config.get("schedulingPeriod"),
        "scheduling_strategy": config.get("schedulingStrategy"),
        "auto_terminated_relationships": config.get("autoTerminatedRelationships") or [],
        "properties": _redact_properties(config.get("properties") or {}, descriptors),
        "position": component.get("position"),
    }


def _summarize_connection(entity: dict[str, Any]) -> dict[str, Any]:
    component = entity.get("component") or {}
    status = (entity.get("status") or {}).get("aggregateSnapshot") or {}
    source = component.get("source") or {}
    destination = component.get("destination") or {}
    return {
        "id": entity.get("id") or component.get("id"),
        "name": component.get("name"),
        "source_id": source.get("id"),
        "source_name": source.get("name"),
        "destination_id": destination.get("id"),
        "destination_name": destination.get("name"),
        "selected_relationships": component.get("selectedRelationships") or [],
        "queued": status.get("queued"),
        "queued_count": status.get("queuedCount"),
        "queued_size": status.get("queuedSize"),
        "back_pressure_object_threshold": component.get("backPressureObjectThreshold"),
        "back_pressure_data_size_threshold": component.get("backPressureDataSizeThreshold"),
    }


def _summarize_group(entity: dict[str, Any]) -> dict[str, Any]:
    component = entity.get("component") or {}
    parameter_context = component.get("parameterContext") or {}
    return {
        "id": entity.get("id") or component.get("id"),
        "name": component.get("name"),
        "comments": component.get("comments") or "",
        "parent_group_id": component.get("parentGroupId"),
        "parameter_context_id": parameter_context.get("id"),
        "position": component.get("position"),
    }


def _summarize_service(entity: dict[str, Any]) -> dict[str, Any]:
    component = entity.get("component") or {}
    descriptors = component.get("descriptors") or {}
    return {
        "id": entity.get("id") or component.get("id"),
        "name": component.get("name"),
        "type": component.get("type"),
        "bundle": component.get("bundle"),
        "state": component.get("state"),
        "validation_status": component.get("validationStatus"),
        "validation_errors": component.get("validationErrors") or [],
        "properties": _redact_properties(component.get("properties") or {}, descriptors),
    }


def _summarize_definition(
    type_name: str, bundle: dict[str, Any], body: dict[str, Any]
) -> dict[str, Any]:
    descriptors = body.get("propertyDescriptors") or body.get("descriptors") or {}
    properties = []
    for descriptor in descriptors.values():
        if not isinstance(descriptor, dict):
            continue
        properties.append(
            {
                "name": descriptor.get("name"),
                "display_name": descriptor.get("displayName"),
                "required": bool(descriptor.get("required")),
                "sensitive": bool(descriptor.get("sensitive")),
                "description": descriptor.get("description") or "",
                "default_value": None if descriptor.get("sensitive") else descriptor.get("defaultValue"),
            }
        )
    relationships = [
        item.get("name") for item in body.get("supportedRelationships") or [] if isinstance(item, dict)
    ]
    return {
        "type": type_name,
        "bundle": _bundle_body(bundle),
        "supports_dynamic_properties": bool(body.get("supportsDynamicProperties")),
        "properties": properties,
        "relationships": relationships,
    }


def _port(entity: dict[str, Any]) -> dict[str, Any]:
    component = entity.get("component") or {}
    return {
        "id": entity.get("id") or component.get("id"),
        "name": component.get("name"),
    }


def _redact_properties(
    properties: dict[str, Any], descriptors: dict[str, Any]
) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for name, value in properties.items():
        descriptor = descriptors.get(name) or {}
        if descriptor.get("sensitive"):
            continue
        cleaned[name] = value
    return cleaned
