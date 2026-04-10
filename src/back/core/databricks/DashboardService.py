"""Databricks Dashboard service (Lakeview AI/BI + legacy SQL dashboards).

Isolates dashboard listing and parameter extraction from the rest of
the Databricks client surface area.
"""
import json
from typing import Any, Dict, List

from back.core.logging import get_logger
from .constants import (
    _LAKEVIEW_MAX_PAGES,
    _LAKEVIEW_PAGE_SIZE,
    _LEGACY_MAX_PAGES,
    _LEGACY_PAGE_SIZE,
    _REQUEST_TIMEOUT,
    LAKEVIEW_DASHBOARDS_PATH,
    LEGACY_DASHBOARDS_PATH,
)
from .DatabricksAuth import DatabricksAuth

logger = get_logger(__name__)


class DashboardService:
    """List and inspect Databricks dashboards."""

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth

    def get_dashboards(self) -> List[Dict[str, str]]:
        """Return all AI/BI (Lakeview) and legacy SQL dashboards.

        Handles pagination for both APIs.  Each dict has ``id``, ``name``,
        ``path``, ``url``, and ``type`` keys.
        """
        import requests

        if not self._auth.host:
            logger.warning("No host configured")
            return []
        if not self._auth.has_valid_auth():
            logger.warning("No valid auth")
            return []

        dashboards: list = []
        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()

        try:
            page_token = None
            pages = 0
            while pages < _LAKEVIEW_MAX_PAGES:
                params: dict = {"page_size": _LAKEVIEW_PAGE_SIZE}
                if page_token:
                    params["page_token"] = page_token

                response = requests.get(
                    f"{host}{LAKEVIEW_DASHBOARDS_PATH}",
                    headers=headers,
                    params=params,
                    timeout=_REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()

                for dash in data.get("dashboards", []):
                    did = dash.get("dashboard_id", "")
                    dashboards.append({
                        "id": did,
                        "name": dash.get("display_name", dash.get("name", "Unnamed")),
                        "path": dash.get("path", dash.get("warehouse_id", "")),
                        "url": f"{host}/dashboardsv3/{did}" if did else "",
                        "type": "lakeview",
                    })

                page_token = data.get("next_page_token")
                pages += 1
                if not page_token:
                    break

            logger.info("Found %d Lakeview dashboards (%d page(s))", len(dashboards), pages)
        except Exception as exc:
            logger.exception("Lakeview API error: %s", exc)

        lakeview_count = len(dashboards)

        try:
            page = 1
            while page <= _LEGACY_MAX_PAGES:
                response = requests.get(
                    f"{host}{LEGACY_DASHBOARDS_PATH}",
                    headers=headers,
                    params={"page_size": _LEGACY_PAGE_SIZE, "page": page},
                    timeout=_REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])

                for dash in results:
                    did = dash.get("id", "")
                    dashboards.append({
                        "id": did,
                        "name": dash.get("name", "Unnamed"),
                        "path": dash.get("slug", ""),
                        "url": f"{host}/sql/dashboards/{did}" if did else "",
                        "type": "legacy",
                    })

                if len(results) < _LEGACY_PAGE_SIZE:
                    break
                page += 1

            legacy_count = len(dashboards) - lakeview_count
            logger.info("Found %d legacy dashboards (%d page(s))", legacy_count, page)
        except Exception as exc:
            logger.exception("Legacy API error: %s", exc)

        logger.info("Total dashboards found: %d", len(dashboards))
        return dashboards

    def get_dashboard_parameters(self, dashboard_id: str) -> Dict[str, Any]:
        """Fetch dashboard details including parameters.

        Returns a dict with ``id``, ``name``, ``path``, ``parameters``,
        ``embed_url``, and ``debug`` keys.
        """
        import requests

        if not self._auth.host or not dashboard_id:
            return {"parameters": [], "error": "Missing host or dashboard_id"}
        if not self._auth.has_valid_auth():
            return {"parameters": [], "error": "No valid authentication"}

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()

        try:
            response = requests.get(
                f"{host}{LAKEVIEW_DASHBOARDS_PATH}/{dashboard_id}",
                headers=headers,
            )
            response.raise_for_status()
            dashboard = response.json()

            parameters: list = []
            serialized = dashboard.get("serialized_dashboard", "")

            if serialized:
                try:
                    dash_def = json.loads(serialized) if isinstance(serialized, str) else serialized
                    parameters = self._extract_parameters(dash_def)
                    self._link_filter_widgets(dash_def, parameters)
                except json.JSONDecodeError:
                    pass

            debug_info: dict = {}
            if serialized:
                try:
                    dd = json.loads(serialized) if isinstance(serialized, str) else serialized
                    debug_info["datasets"] = dd.get("datasets", [])
                    debug_info["pages"] = dd.get("pages", [])
                except Exception:
                    logger.debug("Could not parse dashboard debug structure", exc_info=True)

            return {
                "id": dashboard_id,
                "name": dashboard.get("display_name", dashboard.get("name", "")),
                "path": dashboard.get("path", ""),
                "parameters": parameters,
                "embed_url": f"{host}/embed/dashboardsv3/{dashboard_id}",
                "debug": debug_info,
            }
        except requests.exceptions.HTTPError as exc:
            logger.exception("HTTP error getting dashboard details: %s", exc)
            return {"parameters": [], "error": str(exc)}
        except Exception as exc:
            logger.exception("Error getting dashboard details: %s", exc)
            return {"parameters": [], "error": str(exc)}

    @staticmethod
    def _extract_parameters(dash_def: dict) -> list:
        parameters: list = []
        for dataset in dash_def.get("datasets", []):
            dataset_id = dataset.get("name", "")
            dataset_display = dataset.get("displayName", dataset_id)

            for param in dataset.get("parameters", []):
                keyword = param.get("keyword", "")
                display = param.get("displayName", keyword)
                name = param.get("name", keyword)
                ptype = param.get("dataType", param.get("type", "STRING"))
                internal_id = (
                    param.get("id", "") or param.get("parameterId", "") or param.get("fieldId", "")
                )
                effective = keyword or display or name
                if effective:
                    parameters.append({
                        "name": display or effective,
                        "keyword": keyword or effective,
                        "type": ptype.lower() if ptype else "string",
                        "dataset": dataset_display,
                        "datasetId": dataset_id,
                        "paramId": internal_id,
                    })

        for param in dash_def.get("parameters", []):
            pname = param.get("name", "")
            pkeyword = param.get("keyword", pname)
            if pname and not any(p["name"] == pname for p in parameters):
                parameters.append({
                    "name": pname,
                    "keyword": pkeyword,
                    "type": param.get("type", "string"),
                    "dataset": "",
                })
        return parameters

    @staticmethod
    def _link_filter_widgets(dash_def: dict, parameters: list) -> None:
        mappings: dict = {}
        for page in dash_def.get("pages", []):
            page_name = page.get("name", "")
            for item in page.get("layout", []):
                widget = item.get("widget", {})
                widget_name = widget.get("name", "")
                spec = widget.get("spec", {})
                wtype = spec.get("widgetType", "")
                if wtype and "filter" in wtype.lower():
                    for field in spec.get("encodings", {}).get("fields", []):
                        pname = field.get("parameterName", "")
                        if pname:
                            mappings[pname] = {"pageId": page_name, "widgetId": widget_name}

        for param in parameters:
            kw = param.get("keyword", "")
            if kw in mappings:
                param["pageId"] = mappings[kw]["pageId"]
                param["widgetId"] = mappings[kw]["widgetId"]
