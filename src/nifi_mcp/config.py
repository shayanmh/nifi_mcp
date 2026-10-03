from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_API_SUFFIX = "/nifi-api"
# Cursor starts global MCP servers from an arbitrary working directory.
_PROJECT_ENV = Path(__file__).resolve().parents[2] / ".env"


def normalize_api_url(raw: str) -> str:
    """Accept a UI origin or an ``/nifi-api`` URL and return the API root."""
    url = raw.strip().rstrip("/")
    if url.endswith("/nifi"):
        url = url[: -len("/nifi")]
    if not url.endswith(_API_SUFFIX):
        url = f"{url}{_API_SUFFIX}"
    return url


class Settings(BaseSettings):
    """Connection settings for a secured NiFi 2.7 instance and its Registry.

    Values load from the process environment and a local ``.env`` file.
    ``NIFI_SENSITIVE_PROPS_KEY`` belongs to NiFi itself and is not read here.
    """

    model_config = SettingsConfigDict(
        env_file=_PROJECT_ENV,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    nifi_api_url: str = "https://127.0.0.1:8443/nifi-api"
    nifi_username: str
    nifi_password: str
    nifi_verify_ssl: bool = False
    nifi_registry_enabled: bool = True
    nifi_registry_api_url: str = "http://127.0.0.1:18081/nifi-registry-api"
    nifi_expected_version: str = "2.7.2"
    nifi_timeout_seconds: float = Field(default=60.0, gt=0)
    # On Windows, httpx picks up the system Internet Options proxy, which breaks TLS to 127.0.0.1.
    nifi_use_system_proxy: bool = False

    @field_validator("nifi_api_url")
    @classmethod
    def normalize_nifi_api_url(cls, value: str) -> str:
        return normalize_api_url(value)

    @field_validator("nifi_registry_api_url")
    @classmethod
    def strip_registry_slash(cls, value: str) -> str:
        return value.rstrip("/")
