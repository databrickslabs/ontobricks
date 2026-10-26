"""Neo4j connection Settings: secrets, health, labels, and databases.

Extracted from :class:`GraphEngineSettings` (Fowler Extract Class).
``SettingsService`` / ``GraphEngineSettings`` keep delegators for callers.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals.
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class GraphEngineNeo4jSettings:
    """Neo4j named connections and live probes for the Connection tab."""

    @staticmethod
    def _assert_neo4j_connection_refs_safe(
        previous: Dict[str, Any],
        new_config: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> None:
        """Reject deletes/renames of Neo4j connections still referenced by domains."""
        from back.core.graphdb.engine_config import list_neo4j_connections

        old_names = {
            str(c.get("name") or "").strip()
            for c in list_neo4j_connections(previous)
            if str(c.get("name") or "").strip()
        }
        new_names = {
            str(c.get("name") or "").strip()
            for c in list_neo4j_connections(new_config)
            if str(c.get("name") or "").strip()
        }
        removed = sorted(old_names - new_names)
        if not removed:
            return
        refs = SettingsService._domains_referencing_neo4j_connections(
            session_mgr, settings, removed
        )
        if not refs:
            return
        parts = [
            f"{name!r} used by: {', '.join(domains)}"
            for name, domains in sorted(refs.items())
        ]
        raise ValidationError(
            "Cannot delete or rename Neo4j connection(s) still referenced by "
            "domains — re-point those domains first. " + "; ".join(parts)
        )

    @staticmethod
    def _domains_referencing_neo4j_connections(
        session_mgr: SessionManager,
        settings: Settings,
        connection_names: List[str],
    ) -> Dict[str, List[str]]:
        """Map connection name → domain folders that reference it."""
        wanted = {str(n).strip() for n in connection_names if str(n).strip()}
        if not wanted:
            return {}
        try:
            from back.objects.registry.RegistryService import RegistryService

            domain_obj, _, _, _ = SettingsService._resolve_context(
                session_mgr, settings
            )
            svc = RegistryService.from_context(domain_obj, settings)
            ok, details, _msg = svc.list_domain_details()
            if not ok:
                return {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not scan domains for Neo4j connection refs: %s", exc)
            return {}

        refs: Dict[str, List[str]] = {}
        for row in details or []:
            if not isinstance(row, dict):
                continue
            folder = str(row.get("name") or "").strip()
            conn = str(row.get("neo4j_connection") or "").strip()
            if folder and conn in wanted:
                refs.setdefault(conn, []).append(folder)
        return refs

    @staticmethod
    def graph_engine_neo4j_connections_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List named Neo4j connection profiles (no passwords)."""
        from back.core.graphdb.engine_config import list_neo4j_connections

        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            _ss.global_config_service.load(host, token, registry_cfg, force=True)
            gcfg = _ss.global_config_service.get_graph_engine_config(
                host, token, registry_cfg
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_neo4j_connections context failed: %s", exc)
            raise InfrastructureError(
                "Could not load graph engine config", detail=str(exc)
            ) from exc

        connections = []
        for entry in list_neo4j_connections(gcfg):
            safe = dict(entry)
            safe.pop("password", None)
            connections.append(safe)
        return {"success": True, "connections": connections}

    @staticmethod
    def graph_engine_neo4j_test_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
        draft: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Probe Neo4j Bolt connectivity for a named connection (or draft fields).

        Prefers *draft* (unsaved form values), then the named profile from
        Settings, then fails with a config error.
        """
        import time as _time

        from back.core.graphdb.engine_config import (
            list_neo4j_connections,
            resolve_neo4j_connection,
        )
        from back.core.graphdb.neo4j.Neo4jConnection import (
            Neo4jConnection,
            resolve_neo4j_database,
        )

        gcfg: Dict[str, Any] = {}
        if isinstance(draft, dict) and str(draft.get("uri") or "").strip():
            gcfg = dict(draft)
        else:
            try:
                _, host, token, registry_cfg = SettingsService._resolve_context(
                    session_mgr, settings
                )
                _ss.global_config_service.load(host, token, registry_cfg, force=True)
                root = _ss.global_config_service.get_graph_engine_config(
                    host, token, registry_cfg
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("graph_engine_neo4j_test context failed: %s", exc)
                raise InfrastructureError(
                    "Could not load graph engine config", detail=str(exc)
                ) from exc

            name = str(connection_name or "").strip()
            if not name and list_neo4j_connections(root):
                return {
                    "success": True,
                    "ok": False,
                    "error": "Select a Neo4j connection to test.",
                    "category": "config",
                }
            gcfg = resolve_neo4j_connection(root, name) if name else {}
            if name and not gcfg:
                return {
                    "success": True,
                    "ok": False,
                    "error": f"Neo4j connection {name!r} not found in Settings.",
                    "category": "config",
                }

        if not isinstance(gcfg, dict) or not gcfg:
            return {
                "success": True,
                "ok": False,
                "error": "No Neo4j connection configured — add one under Settings → Neo4j.",
                "category": "config",
            }

        uri = str(gcfg.get("uri") or "").strip()
        if not uri:
            return {
                "success": True,
                "ok": False,
                "error": "Bolt URI is missing on this connection.",
                "category": "config",
            }

        try:
            conn = Neo4jConnection(
                uri=uri,
                database=resolve_neo4j_database(gcfg),
                auth_method=str(gcfg.get("auth_method") or "databricks_secret").strip()
                or "databricks_secret",
                engine_config=gcfg,
                encrypted=bool(gcfg.get("encrypted", True)),
            )
        except ValidationError as exc:
            return {"success": True, "ok": False, "error": str(exc), "category": "config"}
        except ImportError as exc:
            return {
                "success": True,
                "ok": False,
                "error": str(exc),
                "category": "driver-missing",
            }

        t0 = _time.monotonic()
        cypher_rows = None
        try:
            driver = conn.get_driver()
            driver.verify_connectivity()
            cypher_rows = conn.run("RETURN 1 AS probe")
        except InfrastructureError as exc:
            return {
                "success": True,
                "ok": False,
                "error": str(exc),
                "category": "auth",
            }
        except ValidationError as exc:
            return {
                "success": True,
                "ok": False,
                "error": str(exc),
                "category": "config",
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "success": True,
                "ok": False,
                "error": "%s: %s" % (type(exc).__name__, exc),
                "category": "connectivity",
            }
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        latency_ms = round((_time.monotonic() - t0) * 1000.0, 1)

        return {
            "success": True,
            "ok": True,
            "uri": uri,
            "database": conn.database,
            "connection_name": str(gcfg.get("name") or connection_name or "").strip(),
            "latency_ms": latency_ms,
            "cypher_probe": (
                {"rows": len(cypher_rows or []), "echo": (cypher_rows[0] if cypher_rows else None)}
                if cypher_rows is not None
                else None
            ),
            "credentials_source": SettingsService._neo4j_credentials_source(gcfg),
        }

    @staticmethod
    def _neo4j_credentials_source(gcfg: Dict[str, Any]) -> str:
        """Human-readable description of where the Neo4j password came from."""
        from back.core.graphdb.neo4j.Neo4jConnection import NEO4J_PASSWORD_ENV

        auth_method = str(gcfg.get("auth_method") or "basic").strip() or "basic"
        if auth_method == "databricks_secret":
            scope = str(gcfg.get("secret_scope") or "").strip()
            key = str(gcfg.get("secret_key") or "").strip()
            return "Databricks secret (%s/%s)" % (scope, key)
        if _ss.is_neo4j_password_from_secret():
            return "env var (%s — Databricks Apps secret)" % NEO4J_PASSWORD_ENV
        return "engine_config (local-dev fallback)"

    @staticmethod
    def graph_engine_neo4j_secret_scopes_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Databricks secret scopes for the Neo4j "Databricks secret" dropdown.

        Uses the app's own identity (SP OAuth in the deployed app, PAT/CLI
        profile in local dev) — the same identity every other Databricks
        REST call in this codebase uses. A scope only shows up here if that
        identity has at least READ access to it.
        """
        from back.core.databricks.DatabricksClient import DatabricksClient

        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        client = DatabricksClient(host=host, token=token)
        return {"success": True, "scopes": client.list_secret_scopes()}

    @staticmethod
    def graph_engine_neo4j_secret_keys_result(
        scope: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List secret keys within *scope* for the Neo4j "Secret key" dropdown."""
        from back.core.databricks.DatabricksClient import DatabricksClient

        scope = (scope or "").strip()
        if not scope:
            return {"success": True, "keys": []}
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        client = DatabricksClient(host=host, token=token)
        return {"success": True, "keys": client.list_secret_keys(scope)}

    @staticmethod
    def _neo4j_connection_from_config(
        session_mgr,
        settings,
        *,
        connection_name: str = "",
    ):
        """Build a :class:`Neo4jConnection` from a named Settings profile.

        Shared by the Neo4j admin endpoints (objects list, health, drop).
        Returns ``(conn, profile)`` or raises the mapped error.
        """
        from back.core.graphdb.engine_config import (
            list_neo4j_connections,
            resolve_neo4j_connection,
        )
        from back.core.graphdb.neo4j.Neo4jConnection import (
            Neo4jConnection,
            resolve_neo4j_database,
        )

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        _ss.global_config_service.load(host, token, registry_cfg, force=True)
        root = _ss.global_config_service.get_graph_engine_config(host, token, registry_cfg)
        name = str(connection_name or "").strip()
        if not name:
            conns = list_neo4j_connections(root)
            if len(conns) == 1:
                name = str(conns[0].get("name") or "").strip()
            else:
                raise ValidationError(
                    "Select a Neo4j connection first (Settings → Neo4j list)."
                )
        gcfg = resolve_neo4j_connection(root, name)
        if not gcfg or not gcfg.get("uri"):
            raise ValidationError(
                f"Neo4j connection {name!r} is missing or has no Bolt URI."
            )
        conn = Neo4jConnection(
            uri=str(gcfg["uri"]).strip(),
            database=resolve_neo4j_database(gcfg),
            auth_method=str(gcfg.get("auth_method") or "databricks_secret").strip()
            or "databricks_secret",
            engine_config=gcfg,
            encrypted=bool(gcfg.get("encrypted", True)),
        )
        return conn, gcfg

    @staticmethod
    def graph_engine_neo4j_databases_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """List Neo4j databases on the server for a named connection (admin)."""
        from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

        conn, gcfg = SettingsService._neo4j_connection_from_config(
            session_mgr, settings, connection_name=connection_name
        )
        try:
            names = Neo4jReadOps(conn).list_databases()
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        configured = conn.database
        if configured and configured not in names:
            names = [configured] + names
        return {
            "success": True,
            "databases": names,
            "configured": configured,
            "connection_name": str(gcfg.get("name") or connection_name or "").strip(),
        }

    @staticmethod
    def graph_engine_neo4j_labels_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """List materialised Neo4j graphs (marker labels) + counts for the admin Objects tab."""
        from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

        conn, gcfg = SettingsService._neo4j_connection_from_config(
            session_mgr, settings, connection_name=connection_name
        )
        try:
            labels = Neo4jReadOps(conn).list_labels()
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        return {
            "success": True,
            "graphs": labels,
            "database": conn.database,
            "connection_name": str(gcfg.get("name") or connection_name or "").strip(),
        }

    @staticmethod
    def graph_engine_neo4j_health_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """Bolt health probe for the Neo4j admin Health tab."""
        import time as _time

        conn, gcfg = SettingsService._neo4j_connection_from_config(
            session_mgr, settings, connection_name=connection_name
        )
        t0 = _time.monotonic()
        try:
            conn.get_driver().verify_connectivity()
            conn.run("RETURN 1 AS probe")
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            ok, err = False, "%s: %s" % (type(exc).__name__, exc)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        return {
            "success": True,
            "ok": ok,
            "error": err,
            "uri": conn.uri,
            "database": conn.database,
            "connection_name": str(gcfg.get("name") or connection_name or "").strip(),
            "latency_ms": round((_time.monotonic() - t0) * 1000.0, 1),
        }

    @staticmethod
    def graph_engine_neo4j_drop_label_result(
        label: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """Drop one Neo4j graph (marker label): its nodes, rels, constraint, schema map."""
        from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps, sanitise_label

        clean = (label or "").strip()
        if not clean:
            raise ValidationError("No graph label provided to drop.")
        conn, _ = SettingsService._neo4j_connection_from_config(
            session_mgr, settings, connection_name=connection_name
        )
        try:
            Neo4jWriteOps(conn).drop_table(clean)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        return {"success": True, "dropped": sanitise_label(clean)}
