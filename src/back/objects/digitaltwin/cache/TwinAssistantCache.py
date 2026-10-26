"""Graph Chat session cache: history, pending-action tokens, response payload.

Extracted from ``api.routers.internal.dtwin`` (Fowler Extract Class).
The route module keeps ``_chat_*`` aliases for tests that import them there.
"""

from __future__ import annotations

import time
from typing import Any

from back.objects.session import SessionManager


class TwinAssistantCache:
    """In-memory Graph Chat cache keyed on the session."""

    SESSION_KEY = "graph_chat"
    DEFAULT_LIMIT = 20
    MIN_LIMIT = 5
    MAX_LIMIT = 100
    PENDING_ACTION_TTL_SEC = 120
    UPGRADE_INSTANCE_ADVICE = (
        "OntoBricks is under heavy load - the request worker pool is saturated, so "
        "responses may be slow. If this happens often, upgrade the Databricks App "
        "instance size (Apps UI -> Compute) for more concurrency."
    )

    @staticmethod
    def resource_pressure_payload() -> dict:
        """Return a resource-pressure advisory when the blocking pool is saturated."""
        try:
            from back.core.helpers import get_blocking_pool_stats

            stats = get_blocking_pool_stats()
        except Exception:  # noqa: BLE001
            return {"resource_pressure": False}
        if stats.get("saturated"):
            return {
                "resource_pressure": True,
                "resource_advice": TwinAssistantCache.UPGRADE_INSTANCE_ADVICE,
                "pool_stats": stats,
            }
        return {"resource_pressure": False}

    @staticmethod
    def chat_cache(session_mgr: SessionManager) -> dict:
        """Return the Graph Chat session cache, creating an empty one if absent."""
        cache = session_mgr.get(TwinAssistantCache.SESSION_KEY)
        if not isinstance(cache, dict):
            cache = {
                "limit": TwinAssistantCache.DEFAULT_LIMIT,
                "history": {},
                "pending_actions": {},
            }
        else:
            cache.setdefault("limit", TwinAssistantCache.DEFAULT_LIMIT)
            cache.setdefault("history", {})
            cache.setdefault("pending_actions", {})
        return cache

    @staticmethod
    def pending_actions_prune(cache: dict) -> None:
        """Drop expired pending-action tokens in place so the cache stays bounded."""
        now = time.time()
        pending = cache.get("pending_actions") or {}
        expired = [
            tok for tok, entry in pending.items() if entry.get("expires_at", 0) <= now
        ]
        for tok in expired:
            pending.pop(tok, None)

    @staticmethod
    def save_cache(session_mgr: SessionManager, cache: dict) -> None:
        session_mgr.set(TwinAssistantCache.SESSION_KEY, cache)

    @staticmethod
    def resolve_domain_name(domain) -> str:
        """Return the active domain's name from DomainSession / session payloads."""
        if domain is None:
            return ""
        info = getattr(domain, "info", None) or {}
        name = (info.get("name") or "").strip() if isinstance(info, dict) else ""
        if name:
            return name
        d = getattr(domain, "domain", None) or {}
        if isinstance(d, dict):
            name = (d.get("name") or "").strip()
            if name:
                return name
        folder = getattr(domain, "domain_folder", "") or ""
        if isinstance(folder, str) and folder.strip():
            return folder.strip()
        return ""

    @staticmethod
    def domain_key(domain) -> str:
        return TwinAssistantCache.resolve_domain_name(domain) or "__default__"

    @staticmethod
    def clamp_limit(limit) -> int:
        try:
            value = int(limit)
        except (TypeError, ValueError):
            value = TwinAssistantCache.DEFAULT_LIMIT
        return max(
            TwinAssistantCache.MIN_LIMIT,
            min(TwinAssistantCache.MAX_LIMIT, value),
        )

    @staticmethod
    def trim(messages: list, limit: int) -> list:
        """Keep only the last ``limit`` turns (user + assistant messages)."""
        if limit <= 0 or not messages:
            return []
        keep = 2 * limit
        return messages[-keep:] if len(messages) > keep else list(messages)

    @staticmethod
    def chat_response_payload(agent_result, event_type: str | None = None) -> dict:
        """Build the common blocking or SSE-completion Graph Chat response."""
        payload: dict[str, Any] = {
            "success": agent_result.success,
            "reply": agent_result.reply or "",
            "tools": [
                {"name": step.tool_name, "duration_ms": step.duration_ms}
                for step in agent_result.steps
                if step.step_type == "tool_result"
            ],
            "iterations": agent_result.iterations,
            "usage": agent_result.usage,
        }
        if event_type:
            payload["type"] = event_type
        if agent_result.pending_action:
            payload["pending_action"] = agent_result.pending_action
        payload.update(TwinAssistantCache.resource_pressure_payload())
        return payload
