"""Databricks Secrets API — scope/key listing and live value retrieval.

Backs the Neo4j "Databricks secret" auth flow (Settings → Back end →
Neo4j): the admin picks a scope + key from live dropdowns instead of
typing a scope name and relying on a static Databricks Apps secret
resource bound in ``app.yaml``. The app resolves the actual password at
connection time via :meth:`get_secret_value`, using its own identity
(SP OAuth in the deployed app, PAT/CLI profile in local dev) — the same
identity every other Databricks REST call in this codebase already uses.

**Permission model:** the caller (the app's service principal, or the
developer's own identity in local dev) needs at least ``READ`` ACL on
the secret scope. Without it, ``/api/2.0/secrets/get`` returns 403 —
:meth:`get_secret_value` maps that to a friendly remediation message
rather than a raw HTTP error.
"""

import base64

from typing import Any, Dict, List

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

from .DatabricksAuth import DatabricksAuth
from .constants import SECRETS_GET_PATH, SECRETS_LIST_PATH, SECRETS_SCOPES_LIST_PATH

logger = get_logger(__name__)


class SecretsService:
    """List secret scopes/keys and resolve secret values via the REST API."""

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth

    def list_scopes(self) -> List[str]:
        """Return every secret scope name visible to the current identity."""
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth():
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(f"{host}{SECRETS_SCOPES_LIST_PATH}", headers=headers, timeout=10)
            resp.raise_for_status()
            scopes = sorted(
                s.get("name", "") for s in resp.json().get("scopes", []) if s.get("name")
            )
            logger.debug("Listed %d Databricks secret scopes", len(scopes))
            return scopes
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error listing secret scopes: %s", exc)
            return []

    def list_keys(self, scope: str) -> List[str]:
        """Return every secret key name within *scope*."""
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth() or not scope:
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(
                f"{host}{SECRETS_LIST_PATH}",
                headers=headers,
                params={"scope": scope},
                timeout=10,
            )
            resp.raise_for_status()
            keys = sorted(
                s.get("key", "") for s in resp.json().get("secrets", []) if s.get("key")
            )
            logger.debug("Listed %d secret keys in scope '%s'", len(keys), scope)
            return keys
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error listing secret keys for scope '%s': %s", scope, exc)
            return []

    def get_secret_value(self, scope: str, key: str) -> str:
        """Return the decoded secret value for ``scope``/``key``.

        Raises :class:`InfrastructureError` with a remediation pointer on
        permission-denied (403) or missing-scope/key (404) responses —
        these are the two failure modes an admin can actually fix.
        """
        import requests as req

        if not scope or not key:
            raise InfrastructureError(
                "SecretsService.get_secret_value requires both scope and key"
            )
        if not self._auth.host or not self._auth.has_valid_auth():
            raise InfrastructureError(
                "SecretsService: no valid Databricks credentials to call the Secrets API"
            )

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(
                f"{host}{SECRETS_GET_PATH}",
                headers=headers,
                params={"scope": scope, "key": key},
                timeout=10,
            )
        except req.exceptions.RequestException as exc:
            raise InfrastructureError(
                "SecretsService: could not reach the Databricks Secrets API: %s" % exc
            ) from exc

        if resp.status_code == 403:
            raise InfrastructureError(
                "Databricks secrets/get denied (403) for scope '%s' — grant this app's "
                "identity READ permission on the scope: "
                "`databricks secrets put-acl --scope %s --principal <app-service-principal> "
                "--permission READ`." % (scope, scope)
            )
        if resp.status_code == 404:
            raise InfrastructureError(
                "Secret '%s/%s' not found — check the scope and key names in "
                "Settings → Back end → Neo4j." % (scope, key)
            )
        try:
            resp.raise_for_status()
        except req.exceptions.HTTPError as exc:
            raise InfrastructureError(
                "SecretsService: secrets/get failed for '%s/%s': %s" % (scope, key, exc)
            ) from exc

        payload: Dict[str, Any] = resp.json()
        encoded = payload.get("value", "")
        try:
            return base64.b64decode(encoded).decode("utf-8")
        except Exception as exc:  # noqa: BLE001 — malformed payload, not a network error
            raise InfrastructureError(
                "SecretsService: could not decode secret '%s/%s': %s" % (scope, key, exc)
            ) from exc
