"""Discovery of executable Unity AI Gateway model services."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import quote

import requests

from back.core.logging import get_logger
from shared.llm_target import AI_GATEWAY

logger = get_logger(__name__)


class ModelServiceCatalog:
    """List model services the current Databricks principal can invoke."""

    _PERMISSION_WORKERS = 12

    def __init__(self, client) -> None:
        self._client = client

    def _has_execute(self, full_name: str, principal: str) -> bool:
        encoded_name = quote(full_name, safe=".")
        url = (
            f"{self._client.host.rstrip('/')}/api/2.1/unity-catalog/"
            f"effective-permissions/model_service/{encoded_name}"
        )
        response = None
        for attempt in range(2):
            response = requests.get(
                url,
                headers=self._client.get_auth_headers(),
                params={"principal": principal},
                timeout=30,
            )
            if response.status_code != 429 or attempt:
                break
            try:
                retry_after = float(response.headers.get("Retry-After", 0.25))
            except (TypeError, ValueError):
                retry_after = 0.25
            time.sleep(min(max(retry_after, 0.0), 1.0))
        assert response is not None
        response.raise_for_status()
        for assignment in response.json().get("privilege_assignments", []):
            for privilege in assignment.get("privileges", []):
                privilege_name = str(privilege.get("privilege", "")).upper()
                if privilege_name in {"EXECUTE", "ALL_PRIVILEGES"}:
                    return True
        return False

    @staticmethod
    def _full_name(resource_name: str) -> str:
        prefix = "model-services/"
        return (
            resource_name[len(prefix) :]
            if resource_name.startswith(prefix)
            else resource_name
        )

    def _list_page(
        self, principal: str, page_token: str = ""
    ) -> tuple[list[dict[str, str]], str]:
        url = (
            f"{self._client.host.rstrip('/')}/api/2.1/unity-catalog/"
            "model-services"
        )
        results: list[dict[str, str]] = []
        params: dict[str, Any] = {"page_size": 100, "view": "BASIC"}
        if page_token:
            params["page_token"] = page_token
        response = requests.get(
            url,
            headers=self._client.get_auth_headers(),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        services = payload.get("model_services", [])
        with ThreadPoolExecutor(max_workers=self._PERMISSION_WORKERS) as pool:
            resolved = pool.map(
                lambda service: self._as_executable_endpoint(service, principal),
                services,
            )
            results.extend(endpoint for endpoint in resolved if endpoint is not None)
        return results, str(payload.get("next_page_token") or "")

    def _as_executable_endpoint(
        self, service: dict[str, Any], principal: str
    ) -> dict[str, str] | None:
        api_types = service.get("supported_api_types") or []
        if api_types and "mlflow/v1/chat/completions" not in api_types:
            return None
        full_name = self._full_name(str(service.get("name", "")))
        if not full_name:
            return None
        owner = str(service.get("owner") or service.get("effective_owner") or "")
        try:
            executable = owner == principal or self._has_execute(full_name, principal)
        except Exception as exc:
            logger.debug(
                "Cannot verify EXECUTE on AI Gateway service %s: %s",
                full_name,
                exc,
            )
            executable = False
        if not executable:
            return None
        return {
            "name": full_name,
            "kind": AI_GATEWAY,
            "comment": str(service.get("comment") or ""),
        }

    def list_executable(self) -> list[dict[str, str]]:
        """Return executable model services across accessible UC schemas."""
        if not self._client.host or not self._client.has_valid_auth():
            return []
        principal = str(self._client.get_current_user_email() or "").strip()
        if not principal:
            return []

        results: list[dict[str, str]] = []
        page_token = ""
        while True:
            try:
                page, page_token = self._list_page(principal, page_token)
            except Exception as exc:
                logger.warning("Cannot list AI Gateway model services: %s", exc)
                return results
            results.extend(page)
            if not page_token:
                return results
