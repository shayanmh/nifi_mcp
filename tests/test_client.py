"""Client behaviour against an in-process NiFi, with no cluster required."""

from __future__ import annotations

import json

import httpx
import pytest

from nifi_mcp.client import (
    NiFiClient,
    NiFiError,
    UnsupportedNiFiVersion,
    redact_parameter_entity,
    validate_flow_spec,
)
from nifi_mcp.config import Settings

GENERATE = "org.apache.nifi.processors.standard.GenerateFlowFile"
BUNDLE = {
    "group": "org.apache.nifi",
    "artifact": "nifi-standard-nar",
    "version": "2.7.2",
}


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        nifi_api_url="https://nifi.test/nifi-api",
        nifi_username="admin",
        nifi_password="secret",
        nifi_verify_ssl=False,
        nifi_registry_api_url="http://registry.test/nifi-registry-api",
        nifi_expected_version="2.7.2",
    )


def _client(handler) -> tuple[NiFiClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    http = httpx.Client(transport=httpx.MockTransport(transport))
    return NiFiClient(_settings(), http=http), seen


def _about(version: str) -> dict:
    return {"about": {"title": "NiFi", "version": version, "uri": "https://nifi.test"}}


def _json(payload: dict, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


def _dispatch(request: httpx.Request, version: str = "2.7.2", **extra) -> httpx.Response:
    path = request.url.path
    if path.endswith("/access/token"):
        return httpx.Response(201, text="jwt-token")
    if path.endswith("/flow/about"):
        return _json(_about(version))
    responder = extra.get("responder")
    if responder is not None:
        return responder(request)
    return httpx.Response(404, json={"message": f"unexpected {path}"})


def test_rejects_nifi_1x() -> None:
    client, _seen = _client(lambda request: _dispatch(request, version="1.28.0"))
    with pytest.raises(UnsupportedNiFiVersion, match="1.28.0"):
        client.about()


def test_accepts_nifi_2_7_2_without_warning() -> None:
    client, seen = _client(lambda request: _dispatch(request, version="2.7.2"))
    about = client.about()
    assert about["version"] == "2.7.2"
    assert about["version_warning"] is None
    token_call = seen[0]
    assert token_call.method == "POST"
    assert token_call.url.path.endswith("/access/token")
    assert token_call.headers["content-type"].startswith("application/x-www-form-urlencoded")
    assert seen[1].headers["authorization"] == "Bearer jwt-token"


def test_other_2_7_patch_warns_and_other_minor_is_rejected() -> None:
    warned, _seen = _client(lambda request: _dispatch(request, version="2.7.1"))
    assert warned.about()["version_warning"]
    rejected, _seen = _client(lambda request: _dispatch(request, version="2.6.0"))
    with pytest.raises(UnsupportedNiFiVersion, match="2.6.0"):
        rejected.about()


def test_create_processor_uses_bundle_from_type_listing() -> None:
    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/flow/processor-types"):
            return _json({"processorTypes": [{"type": GENERATE, "bundle": BUNDLE}]})
        if request.method == "POST" and request.url.path.endswith("/processors"):
            body = json.loads(request.content)
            return _json(
                {
                    "id": "proc-1",
                    "revision": {"version": 1},
                    "component": {
                        "id": "proc-1",
                        "name": body["component"]["name"],
                        "type": GENERATE,
                        "bundle": body["component"]["bundle"],
                        "state": "STOPPED",
                    },
                },
                status=201,
            )
        return httpx.Response(404, json={"message": request.url.path})

    client, seen = _client(lambda request: _dispatch(request, responder=responder))
    created = client.create_processor("root", GENERATE, "Generate", x=10, y=20)
    posted = json.loads(next(item.content for item in seen if item.method == "POST" and item.url.path.endswith("/processors")))
    assert posted["component"]["bundle"] == BUNDLE
    assert posted["revision"]["version"] == 0
    assert posted["revision"]["clientId"] == client.client_id
    assert "state" not in posted["component"]
    assert created["id"] == "proc-1"
    assert not any("run-status" in item.url.path for item in seen)


def test_update_processor_omits_state_and_rejects_unknown_properties() -> None:
    puts: list[dict] = []

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/processors/proc-1") and request.method == "GET":
            return _json(
                {
                    "revision": {"version": 4},
                    "id": "proc-1",
                    "component": {
                        "id": "proc-1",
                        "name": "Generate",
                        "type": GENERATE,
                        "state": "RUNNING",
                        "bundle": BUNDLE,
                        "relationships": [{"name": "success"}],
                        "config": {
                            "properties": {"File Size": "0B"},
                            "schedulingPeriod": "0 sec",
                            "schedulingStrategy": "TIMER_DRIVEN",
                            "descriptors": {"File Size": {"name": "File Size", "sensitive": False}},
                        },
                    },
                }
            )
        if request.url.path.endswith("/processors/proc-1") and request.method == "PUT":
            body = json.loads(request.content)
            puts.append(body)
            component = body["component"]
            return _json(
                {
                    "revision": {"version": 5},
                    "id": "proc-1",
                    "component": {
                        "id": "proc-1",
                        "name": component["name"],
                        "type": GENERATE,
                        "state": "RUNNING",
                        "config": component["config"],
                    },
                }
            )
        if "/processor-definition/" in request.url.path:
            return _json({"processorDefinition": {"supportsDynamicProperties": False}})
        return httpx.Response(404, json={"message": request.url.path})

    client, seen = _client(lambda request: _dispatch(request, responder=responder))
    updated = client.update_processor(
        "proc-1",
        name="Generate renamed",
        properties={"File Size": "1 KB"},
        scheduling_period="1 sec",
        auto_terminated_relationships=["success"],
    )
    body = puts[0]
    assert "state" not in body["component"]
    assert "state" not in body["component"]["config"]
    assert body["component"]["config"]["properties"]["File Size"] == "1 KB"
    assert body["component"]["config"]["schedulingPeriod"] == "1 sec"
    assert body["revision"] == {"clientId": client.client_id, "version": 4}
    assert updated["name"] == "Generate renamed"
    assert not any("run-status" in item.url.path or "drop-requests" in item.url.path for item in seen)

    with pytest.raises(NiFiError, match="Unknown properties"):
        client.update_processor("proc-1", properties={"Batch Size": "1"})
    assert len(puts) == 1


def test_sensitive_parameter_value_is_removed() -> None:
    secret = "super-secret-value"

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/parameter-contexts/ctx-1"):
            return _json(
                {
                    "id": "ctx-1",
                    "revision": {"version": 2},
                    "component": {
                        "id": "ctx-1",
                        "name": "db",
                        "parameters": [
                            {"parameter": {"name": "user", "value": "admin", "sensitive": False}},
                            {
                                "parameter": {
                                    "name": "password",
                                    "value": secret,
                                    "sensitive": True,
                                }
                            },
                        ],
                    },
                }
            )
        return httpx.Response(404, json={"message": request.url.path})

    client, _seen = _client(lambda request: _dispatch(request, responder=responder))
    context = client.get_parameter_context("ctx-1")
    encoded = json.dumps(context)
    assert secret not in encoded
    by_name = {item["name"]: item for item in context["parameters"]}
    assert by_name["user"]["value"] == "admin"
    assert "value" not in by_name["password"]
    assert by_name["password"]["sensitive"] is True


def test_redact_helper_and_flow_spec_refuse_run_control() -> None:
    secret = "another-secret"
    cleaned = redact_parameter_entity(
        {
            "component": {
                "parameters": [
                    {"parameter": {"name": "password", "value": secret, "sensitive": True}}
                ]
            }
        }
    )
    assert secret not in json.dumps(cleaned)

    with pytest.raises(NiFiError, match="does not start"):
        validate_flow_spec(
            {
                "process_group": {"name": "demo"},
                "objects": [{"type": "processor", "processor_type": GENERATE, "name": "G", "state": "RUNNING"}],
            }
        )

    def fail_if_called(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request {request.url.path}")

    client, _seen = _client(fail_if_called)
    with pytest.raises(NiFiError, match="does not start"):
        client.apply_flow_spec(
            {
                "process_group": {"name": "demo"},
                "objects": [{"type": "processor", "processor_type": GENERATE, "name": "G", "run_status": "RUNNING"}],
            }
        )
