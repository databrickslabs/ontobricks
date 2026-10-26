"""Domain folders, versions, and permissions on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import ConflictError, InfrastructureError
from back.core.mcp_tools import coerce_mcp_policy
from back.objects.registry.registry_cache import invalidate_registry_cache
from ..base import DomainSummary

from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class Catalog:
    """Domain folders, versions, and per-domain permissions."""

    def list_domain_folders(self) -> Tuple[bool, List[str], str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT folder FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s ORDER BY folder",
                    (self._registry(),),
                )
                names = [r[0] for r in cur.fetchall()]
            return True, names, ""
        except Exception as exc:  # noqa: BLE001
            return False, [], str(exc)

    def list_domains_with_metadata(self) -> Tuple[bool, List[DomainSummary], str]:
        try:
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            self._ensure_domains_mcp_policy_column()
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        SELECT d.id, d.folder, d.description, d.base_uri,
                               d.review_quorum, d.mcp_policy
                        FROM {self._q(self._schema)}.domains d
                        WHERE d.registry_id = %s
                        ORDER BY d.folder
                        """,
                        (self._registry(),),
                    )
                    domain_rows = cur.fetchall()
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        SELECT v.domain_id, v.version, v.mcp_enabled, v.status,
                               v.last_update, v.last_build, v.info, v.ontology
                        FROM {self._q(self._schema)}.domain_versions v
                        JOIN {self._q(self._schema)}.domains d ON d.id = v.domain_id
                        WHERE d.registry_id = %s
                        ORDER BY v.domain_id,
                                 string_to_array(v.version, '.')::int[] DESC
                        """,
                        (self._registry(),),
                    )
                    version_rows = cur.fetchall()

            by_domain: Dict[str, List[Dict[str, Any]]] = {}
            for v in version_rows:
                by_domain.setdefault(str(v["domain_id"]), []).append(v)

            from back.core.graphdb.GraphDBFactory import normalize_graph_backend

            result: List[DomainSummary] = []
            for d in domain_rows:
                versions = by_domain.get(str(d["id"]), [])
                description = d["description"] or ""
                base_uri = d["base_uri"] or ""
                graph_backend = normalize_graph_backend(None)
                neo4j_connection = ""
                if versions:
                    latest = versions[0]
                    info = latest["info"] or {}
                    description = description or info.get("description", "")
                    ont = latest["ontology"] or {}
                    base_uri = base_uri or ont.get("base_uri", "")
                    graph_backend = normalize_graph_backend(info.get("graph_backend"))
                    neo4j_connection = str(info.get("neo4j_connection") or "").strip()
                result.append(
                    {
                        "name": d["folder"],
                        "base_uri": base_uri,
                        "description": description,
                        "graph_backend": graph_backend,
                        "neo4j_connection": neo4j_connection,
                        "review_quorum": max(1, int(d.get("review_quorum") or 1)),
                        "mcp_policy": coerce_mcp_policy(d.get("mcp_policy")),
                        "versions": [
                            {
                                "version": v["version"],
                                "active": bool(v["mcp_enabled"]),
                                "status": v["status"] or "DRAFT",
                                "last_update": v["last_update"] or "",
                                "last_build": v["last_build"] or "",
                                "graph_backend": normalize_graph_backend(
                                    (v["info"] or {}).get("graph_backend")
                                ),
                                "has_ontology": bool(
                                    (v["ontology"] or {}).get("classes")
                                ),
                            }
                            for v in versions
                        ],
                    }
                )
            return True, result, ""
        except Exception as exc:  # noqa: BLE001
            logger.exception("list_domains_with_metadata failed")
            return False, [], str(exc)

    def domain_exists(self, folder: str) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s AND folder = %s",
                    (self._registry(), folder),
                )
                return cur.fetchone() is not None
        except Exception as exc:  # noqa: BLE001
            logger.debug("domain_exists(%s) failed: %s", folder, exc)
            return False

    def get_domain_quorum(self, folder: str) -> int:
        """Per-domain review sign-off quorum (>= 1). Default ``1`` when the
        domain is missing; raises :class:`InfrastructureError` when the
        quorum cannot be read so the publish gate fails closed.
        """
        try:
            self._ensure_domains_review_quorum_column()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT review_quorum FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s AND folder = %s",
                    (self._registry(), folder),
                )
                row = cur.fetchone()
            if not row or row[0] is None:
                return 1
            return max(1, int(row[0]))
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                "Failed to read the domain review quorum", detail=str(exc)
            ) from exc

    def delete_domain(self, folder: str) -> List[str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s AND folder = %s",
                    (self._registry(), folder),
                )
            invalidate_registry_cache(self.cache_key)
            return []
        except Exception as exc:  # noqa: BLE001
            return [str(exc)]

    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT v.version
                    FROM {self._q(self._schema)}.domain_versions v
                    JOIN {self._q(self._schema)}.domains d ON d.id = v.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                    ORDER BY string_to_array(v.version, '.')::int[]
                    """,
                    (self._registry(), folder),
                )
                versions = [r[0] for r in cur.fetchall()]
            return True, versions, ""
        except Exception as exc:  # noqa: BLE001
            return False, [], str(exc)

    def read_version(
        self, folder: str, version: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        try:
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            self._ensure_domains_mcp_policy_column()
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT v.info, v.ontology, v.assignment, v.design_layout,
                           v.metadata, v.version, v.mcp_enabled, v.status,
                           v.last_update, v.last_build, d.review_quorum,
                           d.mcp_policy, d.base_uri AS domain_base_uri
                    FROM {self._q(self._schema)}.domain_versions v
                    JOIN {self._q(self._schema)}.domains d ON d.id = v.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s AND v.version = %s
                    """,
                    (self._registry(), folder, version),
                )
                row = cur.fetchone()
            if not row:
                return False, {}, f"Version {version} not found for domain {folder}"
            info = row["info"] or {}
            info.setdefault("mcp_enabled", bool(row["mcp_enabled"]))
            info["review_quorum"] = max(1, int(row.get("review_quorum") or 1))
            info["mcp_policy"] = coerce_mcp_policy(row.get("mcp_policy"))
            info["status"] = row["status"] or "DRAFT"
            if row["last_update"]:
                info["last_update"] = row["last_update"]
            if row["last_build"]:
                info["last_build"] = row["last_build"]
            # Merge: the domains.base_uri column is the canonical source of truth.
            # If the ontology JSON has no base_uri (e.g. legacy data), fall back to
            # the dedicated column so that generation always has the correct value.
            ontology = row["ontology"] or {}
            domain_base_uri = row.get("domain_base_uri") or ""
            if not ontology.get("base_uri") and domain_base_uri:
                ontology["base_uri"] = domain_base_uri
            doc = {
                "info": info,
                "versions": {
                    row["version"]: {
                        "ontology": ontology,
                        "assignment": row["assignment"] or {},
                        "design_layout": row["design_layout"] or {},
                        "metadata": row["metadata"] or {},
                    }
                },
            }
            return True, doc, ""
        except Exception as exc:  # noqa: BLE001
            return False, {}, str(exc)

    def write_version(
        self, folder: str, version: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        try:
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            self._ensure_domains_mcp_policy_column()
            info = data.get("info", {}) or {}
            ver_blob = (data.get("versions") or {}).get(version, {}) or {}
            ontology = ver_blob.get("ontology", data.get("ontology", {})) or {}
            assignment = ver_blob.get("assignment", data.get("assignment", {})) or {}
            design = ver_blob.get("design_layout", data.get("design_layout", {})) or {}
            metadata = ver_blob.get("metadata", data.get("metadata", {})) or {}
            mcp_enabled = bool(info.get("mcp_enabled"))
            status = info.get("status") or "DRAFT"
            last_update = info.get("last_update", "") or ""
            last_build = info.get("last_build", "") or ""
            description = info.get("description", "") or ""
            base_uri = ontology.get("base_uri", "") or ""
            review_quorum = max(1, int(info.get("review_quorum") or 1))
            mcp_policy = coerce_mcp_policy(info.get("mcp_policy"))

            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.domains
                        (registry_id, folder, description, base_uri,
                         review_quorum, mcp_policy)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (registry_id, folder)
                    DO UPDATE SET description   = EXCLUDED.description,
                                  base_uri      = CASE
                                                    WHEN EXCLUDED.base_uri != ''
                                                    THEN EXCLUDED.base_uri
                                                    ELSE {self._q(self._schema)}.domains.base_uri
                                                  END,
                                  review_quorum = EXCLUDED.review_quorum,
                                  mcp_policy    = EXCLUDED.mcp_policy,
                                  updated_at    = now()
                    RETURNING id
                    """,
                    (
                        self._registry(),
                        folder,
                        description,
                        base_uri,
                        review_quorum,
                        json.dumps(mcp_policy),
                    ),
                )
                domain_id = cur.fetchone()[0]
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.domain_versions
                        (domain_id, version, info, ontology, assignment,
                         design_layout, metadata, mcp_enabled, status,
                         last_update, last_build)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                            %s::jsonb, %s::jsonb, %s, %s, %s, %s)
                    ON CONFLICT (domain_id, version)
                    DO UPDATE SET info          = EXCLUDED.info,
                                  ontology      = EXCLUDED.ontology,
                                  assignment    = EXCLUDED.assignment,
                                  design_layout = EXCLUDED.design_layout,
                                  metadata      = EXCLUDED.metadata,
                                  mcp_enabled   = EXCLUDED.mcp_enabled,
                                  status        = EXCLUDED.status,
                                  last_update   = EXCLUDED.last_update,
                                  last_build    = EXCLUDED.last_build,
                                  updated_at    = now()
                    """,
                    (
                        domain_id,
                        version,
                        json.dumps(info),
                        json.dumps(ontology),
                        json.dumps(assignment),
                        json.dumps(design),
                        json.dumps(metadata),
                        mcp_enabled,
                        status,
                        last_update,
                        last_build,
                    ),
                )
            invalidate_registry_cache(self.cache_key)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.exception("write_version failed for %s/%s", folder, version)
            return False, str(exc)

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        try:
            self._ensure_domain_versions_status_column()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {self._q(self._schema)}.domain_versions AS v
                    USING {self._q(self._schema)}.domains AS d
                    WHERE v.domain_id = d.id
                      AND d.registry_id = %s
                      AND d.folder = %s
                      AND v.version = %s
                      AND v.status = %s
                    """,
                    (self._registry(), folder, version, "DRAFT"),
                )
                if cur.rowcount == 0:
                    raise ConflictError(
                        f"Version {version} is no longer Draft or no longer exists; "
                        "refresh and try again"
                    )
            invalidate_registry_cache(self.cache_key)
            return True, ""
        except ConflictError:
            raise
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def update_version_status(
        self, folder: str, version: str, status: str
    ) -> Tuple[bool, str]:
        """Set the lifecycle ``status`` of a single (domain, version).

        Targeted single-row UPDATE so a status transition never rewrites
        the full version document. Also mirrors ``status`` into the
        version ``info`` blob so cached reads stay consistent.
        """
        try:
            self._ensure_domain_versions_status_column()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.domain_versions v
                    SET status = %s,
                        info = jsonb_set(v.info, '{{status}}', to_jsonb(%s::text)),
                        updated_at = now()
                    FROM {self._q(self._schema)}.domains d
                    WHERE v.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND v.version = %s
                    """,
                    (status, status, self._registry(), folder, version),
                )
                if cur.rowcount == 0:
                    return False, (
                        f"Version {version} not found for domain {folder}"
                    )
            invalidate_registry_cache(self.cache_key)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "update_version_status failed for %s/%s", folder, version
            )
            return False, str(exc)

    def get_version_status(
        self, folder: str, version: str
    ) -> Optional[str]:
        """Cheap single-column lifecycle status lookup (no document read)."""
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT v.status
                    FROM {self._q(self._schema)}.domain_versions v
                    JOIN {self._q(self._schema)}.domains d
                      ON d.id = v.domain_id
                    WHERE d.registry_id = %s
                      AND d.folder = %s
                      AND v.version = %s
                    """,
                    (self._registry(), folder, version),
                )
                row = cur.fetchone()
            return row[0] if row else None
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "get_version_status(%s/%s) failed: %s", folder, version, exc
            )
            return None

    def load_domain_permissions(self, folder: str) -> Dict[str, Any]:
        """Empty permissions when the domain has none; raises
        :class:`InfrastructureError` when they cannot be read, so callers
        never mistake an outage for "no grants".
        """
        try:
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT p.principal, p.principal_type, p.display_name, p.role
                    FROM {self._q(self._schema)}.domain_permissions p
                    JOIN {self._q(self._schema)}.domains d ON d.id = p.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                    ORDER BY lower(p.principal)
                    """,
                    (self._registry(), folder),
                )
                rows = cur.fetchall()
            return {"version": 1, "permissions": [dict(r) for r in rows]}
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                "Failed to read domain permissions", detail=str(exc)
            ) from exc

    def save_domain_permissions(
        self, folder: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        entries = data.get("permissions") or []
        try:
            with (
                self._connect() as conn,
                conn.transaction(),
                conn.cursor() as cur,
            ):
                cur.execute(
                    f"""
                    SELECT id FROM {self._q(self._schema)}.domains
                    WHERE registry_id = %s AND folder = %s
                    """,
                    (self._registry(), folder),
                )
                row = cur.fetchone()
                if not row:
                    return False, f"Domain '{folder}' not found"
                domain_id = row[0]
                cur.execute(
                    f"DELETE FROM {self._q(self._schema)}.domain_permissions "
                    "WHERE domain_id = %s",
                    (domain_id,),
                )
                for e in entries:
                    cur.execute(
                        f"""
                        INSERT INTO {self._q(self._schema)}.domain_permissions
                            (domain_id, principal, principal_type,
                             display_name, role)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            domain_id,
                            e.get("principal", ""),
                            e.get("principal_type", "user"),
                            e.get("display_name", ""),
                            e.get("role", "viewer"),
                        ),
                    )
            return True, "Domain permissions saved"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def _ensure_domain_versions_status_column(self) -> bool:
        """Lazily add ``domain_versions.status`` (+ index) if missing.

        Self-heals deployments created before the lifecycle status column
        existed: the full DDL only runs from the Settings *Initialize*
        action. Idempotent (``ADD COLUMN IF NOT EXISTS`` /
        ``CREATE INDEX IF NOT EXISTS``) and guarded by a per-instance flag
        so we only pay the round-trip once per store. Best-effort: on
        failure it logs and returns ``False`` so callers can no-op.
        """
        if self._status_column_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                # Check first: if the column already exists (created by
                # bootstrap as the schema owner), skip all DDL.  Both
                # ALTER TABLE and CREATE INDEX require table ownership in
                # Postgres — attempting them as the SP (who doesn't own
                # domain_versions) raises "must be owner of table …"
                # even with IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'domain_versions' "
                    "AND column_name = 'status'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._status_column_ready = True
                    return True
                # Column absent — attempt DDL (requires schema owner to
                # have not yet run bootstrap-lakebase-perms.sh).
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.domain_versions
                        ADD COLUMN IF NOT EXISTS status text NOT NULL
                        DEFAULT 'DRAFT'
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_versions_status
                        ON {sch}.domain_versions(domain_id, status)
                    """
                )
            self._status_column_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not add domain_versions.status column — "
                "run `make bootstrap-lakebase` (or scripts/bootstrap-lakebase-perms.sh) "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    def _ensure_domains_review_quorum_column(self) -> bool:
        """Lazily add ``domains.review_quorum`` if missing.

        Self-heals deployments created before the per-domain sign-off
        quorum existed. Same idempotent, ownership-aware pattern as
        :meth:`_ensure_domain_versions_status_column`. Best-effort: on
        failure it logs and returns ``False`` so callers can fall back to
        the default quorum.
        """
        if self._quorum_column_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'domains' "
                    "AND column_name = 'review_quorum'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._quorum_column_ready = True
                    return True
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.domains
                        ADD COLUMN IF NOT EXISTS review_quorum integer
                        NOT NULL DEFAULT 1
                    """
                )
            self._quorum_column_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not add domains.review_quorum column — "
                "run `make bootstrap-lakebase` (or scripts/bootstrap-lakebase-perms.sh) "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    def _ensure_domains_mcp_policy_column(self) -> bool:
        """Lazily add ``domains.mcp_policy`` if missing.

        Self-heals deployments created before the per-domain MCP policy
        existed. Same idempotent, ownership-aware pattern as
        :meth:`_ensure_domains_review_quorum_column`. Best-effort: on failure
        it logs and returns ``False`` so callers fall back to the empty
        policy, which is exactly the pre-policy behaviour.
        """
        if self._mcp_policy_column_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'domains' "
                    "AND column_name = 'mcp_policy'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._mcp_policy_column_ready = True
                    return True
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.domains
                        ADD COLUMN IF NOT EXISTS mcp_policy jsonb
                        NOT NULL DEFAULT '{{}}'::jsonb
                    """
                )
            self._mcp_policy_column_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not add domains.mcp_policy column — "
                "run `make bootstrap-lakebase` (or scripts/bootstrap-lakebase-perms.sh) "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False
