"""Per-server session state for the OntoBricks MCP server.

``create_mcp_server`` builds one :class:`MCPServerSession` per process. It owns
the mutable state the tools thread through — selected domain, per-domain MCP
policy, ontology labels, class Actions metadata, resolved registry config and
the pooled HTTP client — plus the registry-resolution and policy-gating helpers
that operate on that state.

HTTP calls go through :mod:`server.http_client` resolved via the module object
(late binding) so tests can monkeypatch ``_get`` / ``_get_auth_headers`` there.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import httpx

from server import http_client as _http
from server.constants import (
    API_V1_DOMAINS,
    API_V1_DT_REGISTRY,
    GRAPH_TOOLS,
    REGISTRY_TOOLS,
    _USER_AGENT,
)
from server.uri_helpers import _local_name

logger = logging.getLogger(__name__)


class MCPServerSession:
    """Mutable state + registry/policy helpers for one MCP server instance."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.base = _http._base_url(mode)

        self.selected_domain_name: Optional[str] = None
        # Per-domain MCP policy, keyed by domain name, as published by
        # ``GET /api/v1/domains``. Filled by ``list_domains`` and lazily by
        # ``ensure_domain_policies``.
        self.domain_policy: dict[str, dict] = {}
        # Per-domain "has a built graph" flag, same provenance as
        # ``domain_policy``. A domain absent from this map (or mapped True)
        # keeps the full surface; a False value hides every ``GRAPH_TOOLS``
        # entry for that domain.
        self.domain_has_graph: dict[str, bool] = {}
        self.ontology_labels: dict[str, str] = {}  # uri/name (lower) → display label
        self.class_actions: dict[str, dict] = {}   # class URI → {"dataset": …, "bridges": …}
        self.registry: dict = {
            "catalog": "",
            "schema": "",
            "volume": "OntoBricksRegistry",
            "_loaded": False,
        }

        # Single shared client per server so HTTP keep-alive / the connection
        # pool are reused across tool calls instead of paying a fresh
        # handshake (and, in databricks mode, a fresh MCP-App → OntoBricks-App
        # network hop) on every request.
        self._http_client_obj: Optional[httpx.AsyncClient] = None

    @asynccontextmanager
    async def client(self):
        """Yield the shared httpx client with fresh auth headers.

        Intentionally does **not** close the client on exit — it is
        pooled for the lifetime of the process. Auth headers are
        refreshed per call (the underlying M2M token is itself cached).
        """
        c = self._http_client_obj
        if c is None or c.is_closed:
            c = httpx.AsyncClient(
                base_url=self.base,
                headers={"User-Agent": _USER_AGENT},
                timeout=120,
                limits=httpx.Limits(
                    max_keepalive_connections=10, max_connections=20
                ),
            )
            self._http_client_obj = c
        auth = _http._get_auth_headers(self.mode)
        if auth:
            c.headers.update(auth)
        yield c

    async def ensure_registry(self) -> dict:
        """Resolve registry config: volume path → env vars → main app API."""
        if self.registry["_loaded"]:
            return self.registry

        vol_path = os.getenv("REGISTRY_VOLUME_PATH", "")
        if vol_path:
            parts = vol_path.strip("/").split("/")
            if len(parts) >= 4 and parts[0].lower() == "volumes":
                self.registry["catalog"] = parts[1]
                self.registry["schema"] = parts[2]
                self.registry["volume"] = parts[3]
                self.registry["_loaded"] = True
                logger.info(
                    "Registry from volume resource: %s.%s.%s",
                    self.registry["catalog"],
                    self.registry["schema"],
                    self.registry["volume"],
                )
                return self.registry
            logger.warning("Cannot parse REGISTRY_VOLUME_PATH '%s'", vol_path)

        env_cat = os.getenv("REGISTRY_CATALOG", "")
        env_sch = os.getenv("REGISTRY_SCHEMA", "")
        env_vol = os.getenv("REGISTRY_VOLUME", "")

        if env_cat and env_sch:
            self.registry["catalog"] = env_cat
            self.registry["schema"] = env_sch
            self.registry["volume"] = env_vol or "OntoBricksRegistry"
            self.registry["_loaded"] = True
            logger.info(
                "Registry from env vars: %s.%s.%s",
                self.registry["catalog"],
                self.registry["schema"],
                self.registry["volume"],
            )
            return self.registry

        try:
            async with self.client() as client:
                data = await _http._get(client, API_V1_DT_REGISTRY)
            self.registry["catalog"] = data.get("catalog", "")
            self.registry["schema"] = data.get("schema", "")
            self.registry["volume"] = data.get("volume", "OntoBricksRegistry")
            self.registry["_loaded"] = True
            logger.info(
                "Registry from main app: %s.%s.%s",
                self.registry["catalog"],
                self.registry["schema"],
                self.registry["volume"],
            )
        except Exception as exc:
            logger.warning("Could not fetch registry config: %s", exc)
        return self.registry

    def registry_params(self) -> dict:
        """Build registry query params from cached registry config."""
        params: dict = {}
        if self.registry["catalog"]:
            params["registry_catalog"] = self.registry["catalog"]
        if self.registry["schema"]:
            params["registry_schema"] = self.registry["schema"]
        if self.registry["volume"] and self.registry["volume"] != "OntoBricksRegistry":
            params["registry_volume"] = self.registry["volume"]
        return params

    def domain_params(self, extra: dict | None = None) -> dict:
        """Build query params, injecting domain registry name and registry when set."""
        params = self.registry_params()
        if extra:
            params.update(extra)
        if self.selected_domain_name:
            params["domain_name"] = self.selected_domain_name
        return params

    def label_or_local(self, uri: str) -> str:
        """Return the ontology label for a URI, falling back to its local name."""
        key = _local_name(uri).lower()
        return self.ontology_labels.get(
            uri, self.ontology_labels.get(key, _local_name(uri))
        )

    async def ensure_domain_policies(self) -> dict[str, dict]:
        """Populate the policy cache if a tool ran before ``list_domains``.

        Well-behaved clients call ``list_domains`` first, but nothing forces
        them to, and ``select_domain`` must know the policy to compute the
        tool set. Failures are swallowed: an empty policy means "everything
        exposed", which is the safe pre-policy behaviour.
        """
        if self.domain_policy:
            return self.domain_policy
        try:
            await self.ensure_registry()
            async with self.client() as client:
                data = await _http._get(
                    client, API_V1_DOMAINS, params=self.registry_params()
                )
            for d in data.get("domains", []) or []:
                if d.get("name"):
                    self.domain_policy[d["name"]] = d.get("mcp_policy") or {}
                    self.domain_has_graph[d["name"]] = bool(d.get("has_graph", True))
        except Exception as exc:  # noqa: BLE001
            logger.warning("could not preload domain MCP policies: %s", exc)
        return self.domain_policy

    def active_policy(self) -> dict:
        """Policy of the currently selected domain (empty when none)."""
        name = self.selected_domain_name
        return self.domain_policy.get(name, {}) if name else {}

    def active_context_policy(self) -> dict:
        """``{feature: mode}`` mapping for the selected domain."""
        return self.active_policy().get("context") or {}

    def disabled_tools(self, policy: dict) -> set[str]:
        """Configurable tools the policy hides, registry tools excluded."""
        raw = policy.get("disabled_tools")
        if not isinstance(raw, list):
            return set()
        return {t for t in raw if isinstance(t, str)} - REGISTRY_TOOLS

    def has_graph(self, name: Optional[str]) -> bool:
        """Whether *name* serves a built graph (default True when unknown)."""
        return self.domain_has_graph.get(name, True) if name else True

    def graph_hidden_for(self, name: Optional[str]) -> set[str]:
        """Graph tools to hide for *name* — all of them when it has no graph."""
        return set(GRAPH_TOOLS) if not self.has_graph(name) else set()

    def hidden_for(self, name: Optional[str]) -> set[str]:
        """Full hidden set for *name*: policy-disabled tools + graph tools when
        the domain is ontology-only."""
        return self.disabled_tools(
            self.domain_policy.get(name, {})
        ) | self.graph_hidden_for(name)

    def ensure_tool_allowed(self, tool_name: str) -> Optional[str]:
        """Return a refusal message when *tool_name* is disabled, else None.

        Hiding a tool from ``tools/list`` is only a hint: a client that
        ignores ``ToolListChangedNotification`` (or cached an older list) can
        still call it. The policy is therefore re-checked on every call.
        """
        name = self.selected_domain_name
        if tool_name in self.disabled_tools(self.active_policy()):
            return (
                f"The tool '{tool_name}' is not available for domain "
                f"'{name}' — its MCP policy does not expose it."
            )
        if tool_name in self.graph_hidden_for(name):
            return (
                f"The tool '{tool_name}' is not available for domain "
                f"'{name}' — it is published with an ontology only (no graph). "
                "Use describe_ontology to read its structure."
            )
        return None

    def require_domain(self, tool_name: str) -> Optional[str]:
        """Single entry guard for every domain-scoped tool.

        Returns the message to hand back to the model, or None to proceed.
        """
        if not self.selected_domain_name:
            return (
                "No domain selected. Call list_domains first, "
                "then select_domain to choose one."
            )
        return self.ensure_tool_allowed(tool_name)

    def ensure_context_allowed(self, feature: str, label: str) -> Optional[str]:
        """Return a refusal message when *feature* is disabled, else None."""
        if self.active_context_policy().get(feature) != "disabled":
            return None
        return (
            f"{label} are disabled for domain '{self.selected_domain_name}' "
            "by its MCP policy."
        )
