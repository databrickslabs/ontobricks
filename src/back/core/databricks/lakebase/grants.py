"""Reusable Lakebase grant primitives.

In-app equivalents of the ``GRANT`` statements in
``scripts/bootstrap-lakebase-perms.sh``. Two call sites share them so the
grant logic lives in exactly one place:

- :class:`~back.core.graphdb.lakebase.provisioner.LakebaseGraphProvisioner`
  — the *Create graph DB* flow (graph schema).
- :meth:`~back.objects.registry.store.lakebase.store.LakebaseRegistryStore.grant_app_permissions`
  — the *Initialize* / *Repair permissions* flow (registry schema).

Permission model (identical to the bash script):

- The connecting principal must **own** the schema to run the Postgres
  ``GRANT`` statements. The app's service principal owns it because it
  ran ``CREATE SCHEMA`` during *Initialize* / *Create graph DB*.
- ``CAN_USE`` on the Lakebase project and ``ALL_PRIVILEGES`` on the Unity
  Catalog catalog are control-plane grants that need *manage* rights the
  SP may not hold — they are therefore best-effort and downgrade to a
  warning instead of aborting (same tolerance as the bash script).

Every function returns ``(granted, warnings)`` — two lists of
human-readable strings the caller aggregates into its task/route result.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from back.core.logging import get_logger

logger = get_logger(__name__)


def resolve_app_service_principals(
    api: Any, app_names: List[str]
) -> Tuple[Dict[str, str], List[str]]:
    """Resolve each app's ``service_principal_client_id`` via the Apps API.

    Missing apps are skipped with a warning (mirrors the bash ``SKIP``
    path). Returns an ordered ``{app_name: sp_client_id}`` mapping plus the
    list of warnings for apps that could not be resolved.
    """
    sp_ids: Dict[str, str] = {}
    warnings: List[str] = []
    for app_name in app_names:
        if not app_name:
            continue
        try:
            resp = api.do("GET", f"/api/2.0/apps/{app_name}") or {}
            sp_id = resp.get("service_principal_client_id") or ""
        except Exception as exc:  # noqa: BLE001
            logger.debug("apps get %s failed: %s", app_name, exc)
            sp_id = ""
        if sp_id:
            sp_ids[app_name] = sp_id
        else:
            warnings.append(
                f"{app_name}: could not resolve service principal "
                f"(app may not exist) — grants skipped"
            )
    return sp_ids, warnings


def grant_can_use_on_project(
    api: Any, project_short: str, sp_ids: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """Grant ``CAN_USE`` on the Lakebase project to each service principal.

    Tries both the Autoscaling (``database-projects``) and Provisioned
    (``database-instances``) permission securables; success on either is
    enough. Best-effort — a failure on both becomes a warning.
    """
    granted: List[str] = []
    warnings: List[str] = []
    for app_name, sp_id in sp_ids.items():
        ok = False
        for securable in ("database-projects", "database-instances"):
            try:
                api.do(
                    "PATCH",
                    f"/api/2.0/permissions/{securable}/{project_short}",
                    body={
                        "access_control_list": [
                            {
                                "service_principal_name": sp_id,
                                "permission_level": "CAN_USE",
                            }
                        ]
                    },
                )
                ok = True
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "CAN_USE grant via %s for %s failed: %s",
                    securable,
                    app_name,
                    exc,
                )
        if ok:
            granted.append(f"{app_name}: CAN_USE on project")
        else:
            warnings.append(
                f"{app_name}: could not grant CAN_USE on project (need "
                f"manage permission on the Lakebase project)"
            )
    return granted, warnings


def grant_schema_privileges(
    conn: Any, schema: str, sp_ids: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """Grant ``USAGE``/``CREATE``/DML + default privileges on *schema*.

    ``conn`` is an open (autocommit) psycopg connection owned by the
    schema owner. Granting per service principal is best-effort: a failure
    on one SP (e.g. its Postgres role does not exist yet) does not stop the
    others.
    """
    granted: List[str] = []
    warnings: List[str] = []
    sch = schema
    for app_name, sp_id in sp_ids.items():
        try:
            with conn.cursor() as cur:
                cur.execute(f'GRANT USAGE, CREATE ON SCHEMA "{sch}" TO "{sp_id}"')
                cur.execute(
                    f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES "
                    f'IN SCHEMA "{sch}" TO "{sp_id}"'
                )
                cur.execute(
                    f"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES "
                    f'IN SCHEMA "{sch}" TO "{sp_id}"'
                )
                cur.execute(
                    f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{sch}" '
                    f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES "
                    f'TO "{sp_id}"'
                )
                cur.execute(
                    f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{sch}" '
                    f"GRANT USAGE, SELECT, UPDATE ON SEQUENCES "
                    f'TO "{sp_id}"'
                )
            granted.append(f"{app_name}: USAGE + DML on schema {sch}")
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Schema grant for %s (%s) failed: %s", app_name, sp_id, exc
            )
            warnings.append(
                f"{app_name}: schema grant failed ({exc}). The Postgres "
                f"role may not exist yet — re-run after the app has "
                f"connected once, or use scripts/bootstrap-lakebase-perms.sh."
            )
    return granted, warnings


def grant_uc_catalog(
    api: Any, uc_catalog: str, sp_ids: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """Grant ``ALL_PRIVILEGES`` on the Unity Catalog catalog to each SP.

    Required so the SP can read back synced tables regardless of who
    created them. Best-effort — needs ``MANAGE`` on the catalog.
    """
    granted: List[str] = []
    warnings: List[str] = []
    for app_name, sp_id in sp_ids.items():
        try:
            api.do(
                "PATCH",
                f"/api/2.1/unity-catalog/permissions/catalog/{uc_catalog}",
                body={"changes": [{"principal": sp_id, "add": ["ALL_PRIVILEGES"]}]},
            )
            granted.append(f"{app_name}: ALL_PRIVILEGES on catalog {uc_catalog}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("UC catalog grant for %s failed: %s", app_name, exc)
            warnings.append(
                f"{app_name}: UC catalog grant on {uc_catalog} failed "
                f"({exc}). You may lack MANAGE on the catalog."
            )
    return granted, warnings
