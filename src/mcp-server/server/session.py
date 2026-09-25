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
from collections import OrderedDict
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
from server.session_scope import CURRENT_SESSION_ID
from server.uri_helpers import _local_name

logger = logging.getLogger(__name__)

# Hard cap on tracked connections so an abusive or long-lived process cannot
# grow the per-session store without bound. Oldest sessions are evicted first;
# a returning client simply re-selects its domain.
_MAX_TRACKED_SESSIONS = 512


class _DomainState:
    """Per-connection mutable state, isolated by MCP session id.

    These three fields are what leaks across concurrent clients when shared:
    the selected domain and the label / class-Action caches populated for it.
    Everything else on :class:`MCPServerSession` (registry config, per-domain
    policy, the pooled HTTP client) is server-wide and safely shared.
    """

    __slots__ = ("selected_domain_name", "ontology_labels", "class_actions")

    def __init__(self) -> None:
        self.selected_domain_name: Optional[str] = None
        self.ontology_labels: dict[str, str] = {}  # uri/name (lower) → label
        self.class_actions: dict[str, dict] = {}   # class URI → {dataset, …}


class MCPServerSession:
    """Mutable state + registry/policy helpers for one MCP server instance."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.base = _http._base_url(mode)

        # Per-connection state (selected domain + label/action caches) lives in
        # ``_domain_states`` keyed by MCP session id, so concurrent clients
        # sharing this one process do not clobber each other. Access goes
        # through the ``selected_domain_name`` / ``ontology_labels`` /
        # ``class_actions`` properties, which resolve the current session's
        # state via ``CURRENT_SESSION_ID`` (set per call by
        # ``SessionScopeMiddleware``). LRU-bounded by ``_MAX_TRACKED_SESSIONS``.
        self._domain_states: "OrderedDict[str, _DomainState]" = OrderedDict()

        # Per-domain MCP policy, keyed by domain name, as published by
        # ``GET /api/v1/domains``. Filled by ``list_domains`` and lazily by
        # ``ensure_domain_policies``. Server-wide facts — safely shared.
        self.domain_policy: dict[str, dict] = {}
        # Per-domain "has a built graph" flag, same provenance as
        # ``domain_policy``. A domain absent from this map (or mapped True)
        # keeps the full surface; a False value hides every ``GRAPH_TOOLS``
        # entry for that domain.
        self.domain_has_graph: dict[str, bool] = {}
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

    # ── Per-connection state (isolated by MCP session id) ─────────────

    def _state(self) -> _DomainState:
        """Return the current MCP connection's :class:`_DomainState`.

        The session id is read from ``CURRENT_SESSION_ID`` (set per tool call
        by ``SessionScopeMiddleware``); calls with no session in scope share
        the default bucket. LRU-bounded so the store cannot grow without limit.
        """
        sid = CURRENT_SESSION_ID.get()
        st = self._domain_states.get(sid)
        if st is None:
            st = _DomainState()
            self._domain_states[sid] = st
            while len(self._domain_states) > _MAX_TRACKED_SESSIONS:
                evicted, _ = self._domain_states.popitem(last=False)
                logger.info("Evicted per-session MCP state for %s (LRU cap)", evicted)
        else:
            self._domain_states.move_to_end(sid)
        return st

    @property
    def selected_domain_name(self) -> Optional[str]:
        return self._state().selected_domain_name

    @selected_domain_name.setter
    def selected_domain_name(self, value: Optional[str]) -> None:
        self._state().selected_domain_name = value

    @property
    def ontology_labels(self) -> dict[str, str]:
        """This connection's uri/name → label cache (mutated in place)."""
        return self._state().ontology_labels

    @property
    def class_actions(self) -> dict[str, dict]:
        """This connection's class-URI → Action metadata cache (mutated in place)."""
        return self._state().class_actions

    async def ensure_registry(self) -> dict:
        """Resolve registry config: env vars → main app API."""
        if self.registry["_loaded"]:
            return self.registry

        env_cat = os.getenv("REGISTRY_CATALOG", "")
        env_sch = os.getenv("REGISTRY_SCHEMA", "")

        if env_cat and env_sch:
            self.registry["catalog"] = env_cat
            self.registry["schema"] = env_sch
            self.registry["volume"] = "OntoBricksRegistry"
            self.registry["_loaded"] = True
            logger.info(
                "Registry from env vars: %s.%s",
                self.registry["catalog"],
                self.registry["schema"],
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
