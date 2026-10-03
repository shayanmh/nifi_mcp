"""Read-only client for an unsecured NiFi Registry 2.x."""

from __future__ import annotations

from typing import Any

import httpx

from nifi_mcp.client import NiFiError
from nifi_mcp.config import Settings


class RegistryClient:
    def __init__(self, settings: Settings, http: httpx.Client | None = None) -> None:
        self.settings = settings
        self._http = http or httpx.Client(
            timeout=settings.nifi_timeout_seconds,
            trust_env=settings.nifi_use_system_proxy,
        )

    def close(self) -> None:
        self._http.close()

    def list_buckets(self) -> dict[str, Any]:
        payload = self._get("/buckets")
        buckets = payload if isinstance(payload, list) else payload.get("buckets") or []
        return {
            "buckets": [
                {
                    "id": item.get("identifier") or item.get("id"),
                    "name": item.get("name"),
                    "description": item.get("description") or "",
                }
                for item in buckets
            ]
        }

    def list_flows(self, bucket_id: str) -> dict[str, Any]:
        payload = self._get(f"/buckets/{bucket_id}/flows")
        flows = payload if isinstance(payload, list) else payload.get("flows") or []
        return {
            "bucket_id": bucket_id,
            "flows": [
                {
                    "id": item.get("identifier") or item.get("id"),
                    "name": item.get("name"),
                    "description": item.get("description") or "",
                    "bucket_id": item.get("bucketIdentifier") or bucket_id,
                }
                for item in flows
            ],
        }

    def _get(self, path: str) -> Any:
        response = self._http.get(f"{self.settings.nifi_registry_api_url}/{path.lstrip('/')}")
        if response.status_code >= 400:
            raise NiFiError(
                f"GET {path} on NiFi Registry failed ({response.status_code}). "
                "Registry in this deployment is read-only and unsecured."
            )
        return response.json()
