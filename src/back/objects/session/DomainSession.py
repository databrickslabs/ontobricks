"""
Domain session service — unified session management for OntoBricks.

Runtime payload is a nested dict with **domain** (info, current_version,
domain_folder, is_active_version, UC **metadata** tables, triplestore), **ontology**
(classes, rules, constraints), **assignment**,
**design_layout**, and **settings**.

Instance-wide options (**warehouse_id**, **default_base_uri**, **default_emoji**)
are *not* stored here; they are read from ``GlobalConfigService`` (registry
volume ``.global_config.json``), admin-only.

Excluded from export: environment credentials and preferences;
``domain.is_active_version``; generated OWL, R2RML, and SQL; and
``assignment.r2rml_output`` (all regenerated from source).

**Domain change stamps** — ``domain.last_update``, ``domain.ontology_changed``,
and ``domain.assignment_changed`` could be inferred from ontology/assignment
content hashes, but they are stored explicitly: updating the stamp and flags on
save is cheap and avoids recomputing hashes on every read.
"""

import re
from datetime import datetime, timezone
from typing import Dict, Any, List

from back.core.logging import get_logger
from shared.config.constants import (
    DEFAULT_BASE_URI,
    DEFAULT_GRAPH_NAME,
    DEFAULT_LADYBUG_PATH,
)

logger = get_logger(__name__)

EXPRESSION_TYPES = frozenset({"unionOf", "intersectionOf", "complementOf", "oneOf"})


def _split_axioms_expressions(items: list) -> tuple:
    """Partition a mixed axioms list into (axioms, expressions) based on type."""
    axioms, expressions = [], []
    for item in items:
        if item.get("type") in EXPRESSION_TYPES:
            expressions.append(item)
        else:
            axioms.append(item)
    return axioms, expressions


def sanitize_domain_folder(name: str) -> str:
    """Turn a domain name into a safe folder name (lowercase, underscores)."""
    name = name.strip().lower().replace(" ", "_").replace("-", "_")
    name = re.sub(r"[^a-z0-9_]", "", name)
    return name or "untitled_domain"


# Default empty domain structure
def get_empty_domain() -> Dict[str, Any]:
    """Return an empty domain data structure."""
    return {
        "domain": {
            "info": {
                "name": "NewDomain",
                "description": "",
                "author": "",
                "llm_endpoint": "",
                "mcp_enabled": False,
            },
            "triplestore": {
                "stats": {},
                "delta": {"catalog": "", "schema": "", "table_name": ""},
                "source_versions": {},
            },
            "current_version": "1",
            "domain_folder": "",
            "is_active_version": True,
            "last_update": "",
            "last_build": "",
            "ontology_changed": False,
            "assignment_changed": False,
            "metadata": {},
        },
        "ontology": {
            "name": "",
            "base_uri": "",
            "description": "",
            "classes": [],
            "properties": [],
            "constraints": [],
            "shacl_shapes": [],
            "swrl_rules": [],
            "decision_tables": [],
            "sparql_rules": [],
            "aggregate_rules": [],
            "axioms": [],
            "expressions": [],
            "groups": [],
        },
        "assignment": {"entities": [], "relationships": []},
        "design_layout": {"current_view": "default", "views": {}, "map": {}},
        "settings": {
            "databricks": {
                "host": "",
            },
            "registry": {"catalog": "", "schema": "", "volume": "OntoBricksRegistry"},
        },
    }


class DomainSession:
    """
    Unified domain session manager.

    Usage:
        domain = DomainSession(session_mgr)

        # Get values
        name = domain.info.name
        classes = domain.ontology.classes

        # Set values
        domain.info.name = "My Domain"
        domain.ontology.classes.append(new_class)

        # Save changes
        domain.save()
    """

    SESSION_KEY = "domain_data"

    def __init__(self, session_mgr):
        """Initialize with a session manager."""
        self._session_mgr = session_mgr
        self._data = self._load()
        self._initial_config_snapshot = self._config_snapshot()
        ont_snap, asgn_snap = self._split_snapshots()
        self._initial_ontology_snapshot = ont_snap
        self._initial_assignment_snapshot = asgn_snap

    def _load(self) -> Dict[str, Any]:
        """Load domain data from session, migrating legacy keys if needed."""
        # Try unified domain_data first (fallback: legacy project_data bucket key)
        data = self._session_mgr.get(self.SESSION_KEY)
        if not data:
            data = self._session_mgr.get("project_data")

        if data:
            # Ensure all sections exist
            return self._ensure_structure(data)

        # Migrate from legacy session keys
        return self._migrate_legacy()

    def _ensure_structure(self, data: Dict) -> Dict:
        """Ensure all required keys exist in domain data and migrate legacy structures."""
        empty = get_empty_domain()

        # Legacy top-level payload key "project" → "domain"
        if "domain" not in data and "project" in data:
            data["domain"] = data.pop("project")

        # Legacy domain_folder stored as project_folder
        if (
            "domain" in data
            and "domain_folder" not in data["domain"]
            and "project_folder" in data["domain"]
        ):
            data["domain"]["domain_folder"] = data["domain"].pop("project_folder", "")

        # Migrate from old flat structure to new /domain structure
        if "domain" not in data:
            data["domain"] = empty["domain"].copy()
            if "info" in data:
                data["domain"]["info"] = data.pop("info")
            if "current_version" in data:
                data["domain"]["current_version"] = data.pop("current_version")

        # Always migrate is_active_version from root to domain (even if domain exists)
        if "is_active_version" in data:
            data["domain"]["is_active_version"] = data.pop("is_active_version")

        # --- Migrate uc_location → domain.domain_folder -----------------
        old_uc = data.get("domain", {}).pop("uc_location", None) or data.pop(
            "uc_location", None
        )
        if old_uc and old_uc.get("project_folder"):
            data["domain"].setdefault("domain_folder", old_uc["project_folder"])
        data.pop("uc_location", None)

        # --- Migrate databricks + preferences → settings ------------------
        if "settings" not in data:
            data["settings"] = empty["settings"].copy()

        # Move old top-level databricks into settings.databricks
        old_db = data.pop("databricks", None)
        if old_db:
            data["settings"]["databricks"] = {
                **empty["settings"]["databricks"],
                **old_db,
            }

        # Move old top-level preferences into settings
        old_prefs = data.pop("preferences", None)
        if old_prefs:
            # Move registry_* keys into settings.registry sub-node
            reg = data["settings"].setdefault(
                "registry", dict(empty["settings"]["registry"])
            )
            for rk in ("registry_catalog", "registry_schema", "registry_volume"):
                val = old_prefs.pop(rk, None)
                if val:
                    reg[rk.replace("registry_", "")] = val
            # Override settings with remaining pref keys (default_emoji, ...)
            for k, v in old_prefs.items():
                data["settings"][k] = v

        # Ensure all settings sub-keys exist
        for sk in empty["settings"]:
            if sk not in data["settings"]:
                data["settings"][sk] = empty["settings"][sk]
            elif isinstance(empty["settings"][sk], dict):
                for ssk in empty["settings"][sk]:
                    if ssk not in data["settings"][sk]:
                        data["settings"][sk][ssk] = empty["settings"][sk][ssk]

        # Remove stale/unused root-level keys
        data.pop("query_result", None)
        data.pop("success", None)
        data.pop("available_versions", None)

        # Ensure domain sub-keys exist
        for subkey in empty["domain"]:
            if subkey not in data["domain"]:
                data["domain"][subkey] = empty["domain"][subkey]
            elif isinstance(empty["domain"][subkey], dict):
                for subsubkey in empty["domain"][subkey]:
                    if subsubkey not in data["domain"][subkey]:
                        data["domain"][subkey][subsubkey] = empty["domain"][subkey][
                            subsubkey
                        ]

        # Add missing top-level keys (excluding 'domain' and 'settings' already handled)
        for key in empty:
            if key in ("domain", "settings"):
                continue
            if key not in data:
                data[key] = empty[key]
            elif isinstance(empty[key], dict):
                for subkey in empty[key]:
                    if subkey not in data[key]:
                        data[key][subkey] = empty[key][subkey]

        # ----------------------------------------------------------
        # Triplestore node migration
        # ----------------------------------------------------------
        empty_ts = empty["domain"]["triplestore"]
        ts = data["domain"].setdefault("triplestore", {})
        for k in empty_ts:
            ts.setdefault(
                k,
                (empty_ts[k].copy() if isinstance(empty_ts[k], dict) else empty_ts[k]),
            )

        info = data["domain"].get("info", {})

        # Migrate domain.delta -> domain.triplestore.delta
        if "delta" in data["domain"] and data["domain"]["delta"]:
            ts["delta"].update(data["domain"].pop("delta"))

        # Drop legacy lakebase data (backend removed)
        if "lakebase" in data["domain"]:
            data["domain"].pop("lakebase")
        data.pop("lakebase", None)
        ts.pop("lakebase", None)
        for old_key in [k for k in info if k.startswith("lakebase_")]:
            info.pop(old_key)

        # Drop legacy ladybug config and snapshot_table (both are now computed)
        ts.pop("ladybug", None)
        ts.pop("snapshot_table", None)

        # Remove legacy backend selector (dual digital twin: both view + graph)
        ts.pop("backend", None)
        info.pop("triplestore_backend", None)

        # Migrate domain.info.triplestore_stats -> domain.triplestore.stats
        if "triplestore_stats" in info:
            ts["stats"] = info.pop("triplestore_stats")

        # Migrate legacy domain.info.triplestore_table -> domain.triplestore.delta
        if "triplestore_table" in info:
            ts_full = info.pop("triplestore_table", "")
            delta = ts["delta"]
            if ts_full and not delta.get("catalog") and not delta.get("table_name"):
                parts = ts_full.split(".")
                if len(parts) >= 3:
                    delta["catalog"] = parts[0]
                    delta["schema"] = parts[1]
                    delta["table_name"] = ".".join(parts[2:])
                else:
                    delta["table_name"] = ts_full

        # Migrate root-level constraints/swrl_rules/axioms into ontology (legacy fix)
        if "constraints" in data and data["constraints"]:
            data["ontology"]["constraints"] = data["constraints"]
        if "swrl_rules" in data and data["swrl_rules"]:
            data["ontology"]["swrl_rules"] = data["swrl_rules"]
        if "axioms" in data and data["axioms"]:
            data["ontology"]["axioms"] = data["axioms"]

        # Remove legacy root-level keys (they now live in ontology)
        data.pop("constraints", None)
        data.pop("swrl_rules", None)
        data.pop("axioms", None)

        # Split mixed axioms list into axioms + expressions (idempotent)
        ont = data.get("ontology", {})
        if "expressions" not in ont or not ont["expressions"]:
            mixed = ont.get("axioms", [])
            if mixed and any(a.get("type") in EXPRESSION_TYPES for a in mixed):
                pure_axioms, expressions = _split_axioms_expressions(mixed)
                ont["axioms"] = pure_axioms
                ont["expressions"] = expressions

        # Migrate legacy 'mapping' key to 'assignment' (old key/sub-keys to new)
        if "mapping" in data:
            old_m = data.pop("mapping", {})
            data["assignment"] = {
                "entities": old_m.get(
                    "data_source_mappings", old_m.get("entities", [])
                ),
                "relationships": old_m.get(
                    "relationship_mappings", old_m.get("relationships", [])
                ),
                "r2rml_output": old_m.get("r2rml_output", ""),
            }

        # Migrate design_layout from old structure to new views/map structure
        if "design_layout" in data:
            dl = data["design_layout"]

            # Remove old 'positions' key (no longer used)
            dl.pop("positions", None)

            # Ensure new structure exists
            if "views" not in dl:
                dl["views"] = {}
            if "map" not in dl:
                dl["map"] = {}

            # Migrate old root-level entities/relationships/inheritances into default view
            # then remove them from root level
            old_entities = dl.pop("entities", None)
            old_relationships = dl.pop("relationships", None)
            old_inheritances = dl.pop("inheritances", None)
            old_visibility = dl.pop("visibility", None)

            # If default view is empty and we have old data, migrate it
            if "default" not in dl["views"] or not dl["views"].get("default"):
                if old_entities or old_relationships or old_inheritances:
                    dl["views"]["default"] = {
                        "entities": old_entities or [],
                        "relationships": old_relationships or [],
                        "inheritances": old_inheritances or [],
                    }
                    if old_visibility:
                        dl["views"]["default"]["visibility"] = old_visibility

            # Set current_view: only to 'default' if it exists, otherwise None
            if "current_view" not in dl:
                dl["current_view"] = (
                    "default"
                    if "default" in dl["views"]
                    else (list(dl["views"].keys())[0] if dl["views"] else None)
                )

        # Drop legacy persisted reasoning (results are only in task completion payloads)
        data.pop("reasoning", None)
        ont = data.get("ontology")
        if isinstance(ont, dict):
            ont.pop("reasoning", None)

        # Migrate root-level UC table metadata -> domain.metadata
        if "metadata" in data:
            legacy_m = data.pop("metadata", None)
            if isinstance(legacy_m, dict):
                data["domain"]["metadata"] = legacy_m

        # Note: r2rml_output is kept in session for runtime use
        # It's only excluded from export (see export_for_save)

        return data

    def _migrate_legacy(self) -> Dict:
        """Migrate from legacy separate session keys to unified structure."""
        data = get_empty_domain()

        # Migrate domain_info / legacy project_name session key into domain.info
        domain_info = self._session_mgr.get("domain_info", {})
        if domain_info:
            data["domain"]["info"].update(domain_info)
        legacy_name = self._session_mgr.get("project_name")
        if legacy_name:
            data["domain"]["info"]["name"] = legacy_name

        # Migrate ontology_config (including constraints, swrl_rules, axioms)
        ontology_config = self._session_mgr.get("ontology_config", {})
        legacy_all_axioms = self._session_mgr.get("owl_axioms", [])
        legacy_axioms, legacy_expressions = _split_axioms_expressions(legacy_all_axioms)
        if ontology_config:
            data["ontology"].update(
                {
                    "name": ontology_config.get("name", ""),
                    "base_uri": ontology_config.get("base_uri", ""),
                    "description": ontology_config.get("description", ""),
                    "classes": ontology_config.get("classes", []),
                    "properties": ontology_config.get("properties", []),
                    "constraints": self._session_mgr.get("property_constraints", []),
                    "swrl_rules": self._session_mgr.get("swrl_rules", []),
                    "axioms": legacy_axioms,
                    "expressions": legacy_expressions,
                }
            )
        else:
            data["ontology"]["constraints"] = self._session_mgr.get(
                "property_constraints", []
            )
            data["ontology"]["swrl_rules"] = self._session_mgr.get("swrl_rules", [])
            data["ontology"]["axioms"] = legacy_axioms
            data["ontology"]["expressions"] = legacy_expressions

        # Migrate mapping_config
        mapping_config = self._session_mgr.get("mapping_config", {})
        if mapping_config:
            data["assignment"] = {
                "entities": mapping_config.get(
                    "entities", mapping_config.get("data_source_mappings", [])
                ),
                "relationships": mapping_config.get(
                    "relationships", mapping_config.get("relationship_mappings", [])
                ),
                "r2rml_output": mapping_config.get("r2rml_output", "")
                or self._session_mgr.get("r2rml_output", ""),
            }

        # Migrate design_layout (clean up old structure)
        design_layout = self._session_mgr.get("design_layout", {})
        if design_layout:
            # Remove old 'positions' key
            design_layout.pop("positions", None)

            # Ensure new structure
            if "views" not in design_layout:
                design_layout["views"] = {}
            if "map" not in design_layout:
                design_layout["map"] = {}

            # Migrate old root-level entities/relationships/inheritances into default view
            old_entities = design_layout.pop("entities", None)
            old_relationships = design_layout.pop("relationships", None)
            old_inheritances = design_layout.pop("inheritances", None)
            old_visibility = design_layout.pop("visibility", None)

            if "default" not in design_layout["views"] or not design_layout[
                "views"
            ].get("default"):
                if old_entities or old_relationships or old_inheritances:
                    design_layout["views"]["default"] = {
                        "entities": old_entities or [],
                        "relationships": old_relationships or [],
                        "inheritances": old_inheritances or [],
                    }
                    if old_visibility:
                        design_layout["views"]["default"]["visibility"] = old_visibility

            # Set current_view: only to existing views
            if "current_view" not in design_layout:
                design_layout["current_view"] = (
                    "default"
                    if "default" in design_layout["views"]
                    else (
                        list(design_layout["views"].keys())[0]
                        if design_layout["views"]
                        else None
                    )
                )

            data["design_layout"] = design_layout

        # Migrate databricks settings and preferences into settings
        # Note: warehouse_id, default_emoji, default_base_uri are
        # no longer stored per-session (they are instance-global).
        # Note: token is NOT migrated — resolved at runtime from env/OAuth.
        data["settings"] = {
            "databricks": {
                "host": self._session_mgr.get("databricks_host", ""),
            },
            "registry": {"catalog": "", "schema": "", "volume": "OntoBricksRegistry"},
        }

        # Note: generated content is runtime-only, not migrated from legacy
        # It will be lazily initialized when accessed via self.generated property

        return data

    def _config_snapshot(self) -> str:
        """Return a quick hash of ontology + mapping for change detection.

        Strips volatile runtime flags (``excluded``, ``r2rml_output``) so
        the hash only reflects real user-controlled content.
        """
        import copy, hashlib, json

        ont = copy.deepcopy(self._data.get("ontology", {}))
        for cls in ont.get("classes", []):
            cls.pop("excluded", None)
        for prop in ont.get("properties", []):
            prop.pop("excluded", None)
        asgn = self._data.get("assignment", {})
        payload = json.dumps(
            {
                "ontology": ont,
                "entities": asgn.get("entities", []),
                "relationships": asgn.get("relationships", []),
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def _rebake_snapshots(self):
        """Re-capture initial snapshots from current data so save() sees no diff."""
        self._initial_config_snapshot = self._config_snapshot()
        ont_snap, asgn_snap = self._split_snapshots()
        self._initial_ontology_snapshot = ont_snap
        self._initial_assignment_snapshot = asgn_snap

    def _split_snapshots(self):
        """Return separate (ontology_hash, assignment_hash) for granular change detection."""
        import copy, hashlib, json

        ont = copy.deepcopy(self._data.get("ontology", {}))
        for cls in ont.get("classes", []):
            cls.pop("excluded", None)
        for prop in ont.get("properties", []):
            prop.pop("excluded", None)
        asgn = self._data.get("assignment", {})

        ont_payload = json.dumps(ont, sort_keys=True, default=str)
        asgn_payload = json.dumps(
            {
                "entities": asgn.get("entities", []),
                "relationships": asgn.get("relationships", []),
            },
            sort_keys=True,
            default=str,
        )
        return (
            hashlib.sha256(ont_payload.encode()).hexdigest(),
            hashlib.sha256(asgn_payload.encode()).hexdigest(),
        )

    def save(self):
        """Save project data to session.

        Stamps ``domain.last_update`` when the ontology or mapping data
        has changed during this request.

        ``domain.last_update``, ``domain.ontology_changed``, and
        ``domain.assignment_changed`` are stored explicitly (they could be
        derived from content hashes, but the stamp and flags are cheap and
        avoid recomputation on every read).

        Note: Runtime-only data is excluded from session storage:
        - 'generated' (owl, r2rml, sql) - calculated on demand
        - 'available_versions' - fetched from UC on demand
        - 'assignment.r2rml_output' - generated on demand (mapping data)
        - 'excluded' on ontology classes/properties (lives in mapping entries)
        """
        # Strip runtime-only 'excluded' flag from ontology objects
        # (stamped by frontend for UI; the source of truth is in mapping entries)
        for cls in self._data.get("ontology", {}).get("classes", []):
            cls.pop("excluded", None)
        for prop in self._data.get("ontology", {}).get("properties", []):
            prop.pop("excluded", None)

        if self._config_snapshot() != self._initial_config_snapshot:
            self._data["domain"]["last_update"] = datetime.now(timezone.utc).isoformat()

        ont_snap, asgn_snap = self._split_snapshots()
        if ont_snap != self._initial_ontology_snapshot:
            self._data["domain"]["ontology_changed"] = True
        if asgn_snap != self._initial_assignment_snapshot:
            self._data["domain"]["assignment_changed"] = True

        # Create a copy without runtime-only data
        data_to_save = self._data.copy()

        # Remove generated content (calculated on demand)
        data_to_save.pop("generated", None)
        data_to_save.pop("available_versions", None)

        # Remove r2rml_output from mapping (generated on demand)
        if "assignment" in data_to_save:
            assignment_copy = data_to_save["assignment"].copy()
            assignment_copy.pop("r2rml_output", None)
            data_to_save["assignment"] = assignment_copy

        # Strip secrets — token is resolved at runtime from env/OAuth
        if "settings" in data_to_save:
            settings_copy = dict(data_to_save["settings"])
            if "databricks" in settings_copy:
                db_copy = dict(settings_copy["databricks"])
                db_copy.pop("token", None)
                settings_copy["databricks"] = db_copy
            data_to_save["settings"] = settings_copy

        # Legacy top-level keys (migrated under domain / ontology)
        data_to_save.pop("metadata", None)
        data_to_save.pop("reasoning", None)
        if "ontology" in data_to_save and isinstance(data_to_save["ontology"], dict):
            ont_save = dict(data_to_save["ontology"])
            ont_save.pop("reasoning", None)
            data_to_save["ontology"] = ont_save

        self._session_mgr.set(self.SESSION_KEY, data_to_save)

    def reset(self):
        """Reset project to empty state, preserving settings."""
        saved_settings = dict(self._data.get("settings", {}))
        self._data = get_empty_domain()
        if saved_settings:
            self._data["settings"] = saved_settings
        self._rebake_snapshots()
        self.save()
        self._clear_legacy_keys()

    def clear_generated_content(self):
        """Clear all generated/cached content (OWL, R2RML, query results).

        Call this when creating a new version or switching versions to ensure
        fresh generation of derived content.

        Note: Generated content is runtime-only, so no need to save().
        """
        # Clear generated OWL, SQL, R2RML (use property for lazy init)
        gen = self.generated  # This ensures the key exists
        gen["owl"] = ""
        gen["sql"] = ""
        gen["r2rml"] = ""
        # Clear R2RML output in mapping (legacy location)
        if "assignment" in self._data:
            self._data["assignment"]["r2rml_output"] = ""

    def get_session_status(self) -> Dict[str, Any]:
        """Return a summary dict of session state for navbar indicators."""
        return {
            "success": True,
            "class_count": len(self.get_classes()),
            "property_count": len(self.get_properties()),
            "entities": len(self.get_entity_mappings()),
            "relationships": len(self.get_relationship_mappings()),
            "has_r2rml": bool(self.get_r2rml()),
            "has_design": bool(self.design_layout.get("views")),
            "domain_name": self.info.get("name", "NewDomain"),
        }

    def reset_ontology(self) -> None:
        """Clear ontology, mappings, and design layout in one atomic operation.

        Resets the session to a blank ontology state while preserving
        Databricks/project settings.  Saves the session afterwards.
        """
        self.ontology.update(
            {
                "name": "",
                "base_uri": "",
                "description": "",
                "classes": [],
                "properties": [],
                "constraints": [],
                "swrl_rules": [],
                "axioms": [],
                "expressions": [],
                "groups": [],
            }
        )
        self.assignment.update(
            {
                "entities": [],
                "relationships": [],
            }
        )
        self.design_layout.update(
            {
                "current_view": "default",
                "views": {},
                "map": {},
            }
        )
        self.clear_generated_content()
        self.save()

    def clear_uc_metadata(self):
        """Clear Unity Catalog metadata for a fresh project.

        Call this when creating a new project to ensure no stale UC references.
        """
        self._data["domain"]["domain_folder"] = ""
        self._data["domain"]["is_active_version"] = True
        self._data.pop("uc_location", None)
        self._data.pop("available_versions", None)
        self._data.pop("is_active_version", None)
        self.save()

    def _clear_legacy_keys(self):
        """Remove legacy session keys."""
        legacy_keys = [
            "domain_info",
            "project_name",
            "project_config",
            "ontology_config",
            "property_constraints",
            "swrl_rules",
            "owl_axioms",
            "mapping_config",
            "r2rml_output",
            "design_layout",
            "databricks_host",
            "databricks_token",
            "warehouse_id",
            "catalog",
            "schema",
            "default_emoji",
            "generated_owl",
            "generated_sql",
            "query_result",
        ]
        for key in legacy_keys:
            self._session_mgr.delete(key)

    # ===========================================
    # Property Accessors
    # ===========================================

    @property
    def data(self) -> Dict[str, Any]:
        """Get raw project data."""
        return self._data

    @property
    def domain(self) -> Dict[str, Any]:
        """Get domain metadata (info, current_version, domain_folder)."""
        return self._data["domain"]

    @property
    def info(self) -> Dict[str, Any]:
        """Get project info."""
        return self._data["domain"]["info"]

    @property
    def current_version(self) -> str:
        """Get current version."""
        return self._data["domain"].get("current_version", "1")

    @current_version.setter
    def current_version(self, value: str):
        """Set current version."""
        self._data["domain"]["current_version"] = value

    @property
    def domain_folder(self) -> str:
        """The folder name under which this domain is saved in the registry."""
        return self._data["domain"].get("domain_folder", "")

    @domain_folder.setter
    def domain_folder(self, value: str):
        self._data["domain"]["domain_folder"] = value

    @property
    def uc_domain_folder(self) -> str:
        """Sanitized project folder name for registry storage."""
        folder = self.domain_folder
        if folder:
            return folder
        return sanitize_domain_folder(self.info.get("name", "untitled_domain"))

    @property
    def registry(self) -> Dict[str, str]:
        """Get the registry configuration (catalog, schema, volume)."""
        return self._data["settings"].get("registry", {})

    @property
    def uc_domain_path(self) -> str:
        """Full /Volumes/.../domains/<folder> path, or '' if registry not set.

        This is the **domain-level** path (shared across all versions).
        Use :attr:`uc_version_path` for version-scoped operations
        (documents, LadybugDB archives).

        Resolves catalog/schema/volume via :class:`RegistryCfg` so the injected
        app-bound volume path (``Settings.registry_volume_path``) matches
        listing/upload code paths that already use ``RegistryCfg.from_domain``.

        Uses the ``domains/`` sub-folder (the canonical name).  The actual
        on-disk folder may be ``projects/`` for legacy registries; that
        resolution happens inside :class:`RegistryService` at I/O time.
        """
        from shared.config.settings import get_settings
        from back.objects.registry.service import RegistryCfg, _DOMAINS_FOLDER

        cfg = RegistryCfg.from_domain(self, get_settings())
        if not cfg.is_configured:
            return ""
        return f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}/{_DOMAINS_FOLDER}/{self.uc_domain_folder}"

    @property
    def uc_version_path(self) -> str:
        """Full /Volumes/.../domains/<folder>/V<version> path, or '' if registry not set.

        This is the **version-scoped** path for per-version artifacts
        (``V{N}.json``, ``documents/``, LadybugDB archives).
        """
        base = self.uc_domain_path
        if not base:
            return ""
        version = self.current_version or "1"
        return f"{base}/V{version}"

    @property
    def is_active_version(self) -> bool:
        """Get whether current version is active (latest)."""
        return self._data["domain"].get("is_active_version", True)

    @is_active_version.setter
    def is_active_version(self, value: bool):
        """Set whether current version is active."""
        self._data["domain"]["is_active_version"] = value

    @property
    def last_update(self) -> str:
        """ISO timestamp of the last ontology or mapping modification."""
        return self._data["domain"].get("last_update", "")

    @last_update.setter
    def last_update(self, value: str):
        self._data["domain"]["last_update"] = value

    @property
    def last_build(self) -> str:
        """ISO timestamp of the last successful Digital Twin build."""
        return self._data["domain"].get("last_build", "")

    @last_build.setter
    def last_build(self, value: str):
        self._data["domain"]["last_build"] = value

    @property
    def snapshot_table(self) -> str:
        """Fully-qualified name of the incremental-sync snapshot Delta table (computed)."""
        from back.core.triplestore.IncrementalBuildService import (
            IncrementalBuildService,
        )

        delta = self.delta
        if not delta.get("catalog") or not delta.get("schema"):
            return ""
        return IncrementalBuildService.snapshot_table_name(
            self.info.get("name", DEFAULT_GRAPH_NAME),
            delta,
            version=self.current_version,
        )

    @property
    def source_versions(self) -> Dict[str, Any]:
        """Source Delta table versions recorded at the last successful build."""
        return self._data["domain"].get("triplestore", {}).get("source_versions", {})

    @source_versions.setter
    def source_versions(self, value: Dict[str, Any]):
        self._data["domain"].setdefault("triplestore", {})["source_versions"] = value

    @property
    def ontology_changed(self) -> bool:
        """Whether the ontology has been modified since the last project save."""
        return self._data["domain"].get("ontology_changed", False)

    @ontology_changed.setter
    def ontology_changed(self, value: bool):
        self._data["domain"]["ontology_changed"] = value

    @property
    def assignment_changed(self) -> bool:
        """Whether the mapping has been modified since the last project save."""
        return self._data["domain"].get("assignment_changed", False)

    @assignment_changed.setter
    def assignment_changed(self, value: bool):
        self._data["domain"]["assignment_changed"] = value

    def clear_change_flags(self):
        """Reset both change flags to False (called when saving the project to registry)."""
        self._data["domain"]["ontology_changed"] = False
        self._data["domain"]["assignment_changed"] = False

    @property
    def ontology(self) -> Dict[str, Any]:
        """Get ontology configuration."""
        return self._data["ontology"]

    @property
    def constraints(self) -> List[Dict]:
        """Get property constraints."""
        return self._data["ontology"].get("constraints", [])

    @constraints.setter
    def constraints(self, value: List[Dict]):
        self._data["ontology"]["constraints"] = value

    @property
    def shacl_shapes(self) -> List[Dict]:
        """Get SHACL data-quality shapes."""
        return self._data["ontology"].get("shacl_shapes", [])

    @shacl_shapes.setter
    def shacl_shapes(self, value: List[Dict]):
        self._data["ontology"]["shacl_shapes"] = value

    def deduplicate_shacl_shapes(self) -> None:
        """One-time cleanup: remove duplicate SHACL shapes in place.

        Two shapes are considered duplicates when they share the same
        content fingerprint (category + target_class + property_path +
        shacl_type + message).  Keeps only the first occurrence.

        Call once after loading a project (e.g. from the list endpoint)
        rather than on every request.
        """
        existing = self.shacl_shapes
        if not existing:
            return

        seen_ids: set = set()
        seen_fingerprints: set = set()
        result: List[Dict] = []

        for s in existing:
            sid = s.get("id", "")
            fp = (
                s.get("category", ""),
                s.get("target_class", ""),
                s.get("property_path", ""),
                s.get("shacl_type", ""),
                s.get("message", ""),
            )
            if sid in seen_ids or fp in seen_fingerprints:
                continue
            seen_ids.add(sid)
            seen_fingerprints.add(fp)
            result.append(s)

        dropped = len(existing) - len(result)
        if dropped:
            self.shacl_shapes = result
            logger.info(
                "Deduplicated SHACL shapes: removed %d duplicates (%d → %d)",
                dropped,
                len(existing),
                len(result),
            )

    @property
    def swrl_rules(self) -> List[Dict]:
        """Get SWRL rules."""
        return self._data["ontology"].get("swrl_rules", [])

    @swrl_rules.setter
    def swrl_rules(self, value: List[Dict]):
        self._data["ontology"]["swrl_rules"] = value

    @property
    def decision_tables(self) -> List[Dict]:
        return self._data["ontology"].get("decision_tables", [])

    @decision_tables.setter
    def decision_tables(self, value: List[Dict]):
        self._data["ontology"]["decision_tables"] = value

    @property
    def sparql_rules(self) -> List[Dict]:
        return self._data["ontology"].get("sparql_rules", [])

    @sparql_rules.setter
    def sparql_rules(self, value: List[Dict]):
        self._data["ontology"]["sparql_rules"] = value

    @property
    def aggregate_rules(self) -> List[Dict]:
        return self._data["ontology"].get("aggregate_rules", [])

    @aggregate_rules.setter
    def aggregate_rules(self, value: List[Dict]):
        self._data["ontology"]["aggregate_rules"] = value

    @property
    def axioms(self) -> List[Dict]:
        """Get OWL axioms (logical assertions: equivalentClass, disjointWith, etc.)."""
        return self._data["ontology"].get("axioms", [])

    @axioms.setter
    def axioms(self, value: List[Dict]):
        self._data["ontology"]["axioms"] = value

    @property
    def expressions(self) -> List[Dict]:
        """Get OWL class expressions (unionOf, intersectionOf, complementOf, oneOf)."""
        return self._data["ontology"].get("expressions", [])

    @expressions.setter
    def expressions(self, value: List[Dict]):
        self._data["ontology"]["expressions"] = value

    @property
    def groups(self) -> List[Dict]:
        """Get entity groups (OWL defined classes using owl:unionOf)."""
        return self._data["ontology"].get("groups", [])

    @groups.setter
    def groups(self, value: List[Dict]):
        self._data["ontology"]["groups"] = value

    @property
    def catalog_metadata(self) -> Dict[str, Any]:
        """Unity Catalog table/column metadata (stored under ``domain``)."""
        return self._data["domain"].setdefault("metadata", {})

    @property
    def assignment(self) -> Dict[str, Any]:
        """Get mapping configuration (entity and relationship mappings)."""
        return self._data["assignment"]

    @property
    def design_layout(self) -> Dict[str, Any]:
        """Get design layout."""
        return self._data["design_layout"]

    @property
    def triplestore(self) -> Dict[str, Any]:
        """Get the triplestore configuration node (stats, delta, ladybug)."""
        return self._data["domain"].get("triplestore", {})

    @property
    def delta(self) -> Dict[str, str]:
        """Get Delta triplestore settings (catalog, schema, table_name)."""
        return self.triplestore.get("delta", {})

    @property
    def ladybug(self) -> Dict[str, str]:
        """LadybugDB settings (computed constant, no longer stored in session)."""
        return {"db_path": DEFAULT_LADYBUG_PATH}

    @property
    def databricks(self) -> Dict[str, str]:
        """Get Databricks connection settings (inside settings node)."""
        return self._data["settings"].get("databricks", {})

    @property
    def settings(self) -> Dict[str, Any]:
        """Get settings (replaces old preferences + databricks)."""
        return self._data["settings"]

    @property
    def preferences(self) -> Dict[str, Any]:
        """Backward-compatible alias for settings."""
        return self._data["settings"]

    @property
    def generated(self) -> Dict[str, str]:
        """Get generated content (OWL, SQL, R2RML).

        Note: This is runtime-only data, not persisted to session.
        """
        if "generated" not in self._data:
            self._data["generated"] = {"owl": "", "sql": "", "r2rml": ""}
        return self._data["generated"]

    # ===========================================
    # Convenience Methods
    # ===========================================

    def get_classes(self) -> List[Dict]:
        """Get ontology classes."""
        return self._data["ontology"].get("classes", [])

    def _ensure_inherited_properties(self) -> None:
        """Propagate inherited dataProperties down the subClassOf hierarchy.

        Mutates classes in-place so that child classes include their
        ancestors' datatype properties (marked ``inherited: True``).
        Safe to call multiple times -- already-inherited entries are
        detected by name and not duplicated.
        """
        from back.core.w3c.owl.OntologyParser import OntologyParser

        classes = self._data["ontology"].get("classes", [])
        if classes:
            OntologyParser._propagate_inherited_properties(classes)

    def get_properties(self) -> List[Dict]:
        """Get ontology properties (relationships)."""
        return self._data["ontology"].get("properties", [])

    def get_entity_mappings(self) -> List[Dict]:
        """Get entity/data source mappings."""
        return self._data["assignment"].get("entities", [])

    def get_relationship_mappings(self) -> List[Dict]:
        """Get relationship mappings."""
        return self._data["assignment"].get("relationships", [])

    def get_r2rml(self) -> str:
        """Get R2RML output (runtime-only, not persisted)."""
        # Check both locations for backwards compatibility
        r2rml = self.generated.get("r2rml", "")
        if not r2rml:
            r2rml = self._data.get("assignment", {}).get("r2rml_output", "")
        return r2rml

    def set_r2rml(self, content: str):
        """Set R2RML output (runtime-only, not persisted)."""
        self.generated["r2rml"] = content
        # Also set in mapping data for backwards compatibility
        if "assignment" not in self._data:
            self._data["assignment"] = {}
        self._data["assignment"]["r2rml_output"] = content

    def is_ontology_valid(self) -> bool:
        """Check if ontology has required data."""
        ont = self._data["ontology"]
        return bool(ont.get("base_uri") and ont.get("classes"))

    def ensure_generated_content(self) -> Dict[str, bool]:
        """Ensure OWL and R2RML are generated if source data exists.

        This method regenerates content that was not saved to file
        (R2RML and OWL are session-only, not saved to project files).

        Returns:
            Dict with 'owl' and 'r2rml' keys indicating what was generated.
        """
        result = {"owl": False, "r2rml": False}

        # Generate OWL if ontology exists but OWL is missing
        # Use self.generated property for lazy initialization
        if self.get_classes() and not self.generated.get("owl"):
            try:
                from back.objects.ontology import Ontology

                owl_content = Ontology.generate_owl(
                    self.ontology,
                    self.constraints,
                    self.swrl_rules,
                    self.axioms,
                    self.expressions,
                    self.groups,
                )
                self.generated["owl"] = owl_content
                result["owl"] = True
            except Exception as e:
                logger.warning("Could not auto-generate OWL: %s", e)

        # Generate R2RML if mappings exist but R2RML is missing
        if self.get_entity_mappings() and not self.get_r2rml():
            try:
                from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator

                base_uri = self.ontology.get("base_uri", DEFAULT_BASE_URI)
                generator = R2RMLGenerator(base_uri)
                r2rml_content = generator.generate_mapping(
                    self.assignment, self.ontology
                )
                self.set_r2rml(r2rml_content)
                result["r2rml"] = True
            except Exception as e:
                logger.warning("Could not auto-generate R2RML: %s", e)

        if result["owl"] or result["r2rml"]:
            self.save()

        return result

    def export_for_save(self) -> Dict[str, Any]:
        """Export project data in versioned format suitable for saving to file.

        Top-level shape (conceptual; keys under ``versions`` hold per-version payloads)::

            {
                "info": { "name", "description", "author", "llm_endpoint", ... },
                "versions": {
                    "1.0": {
                        "ontology": { classes, properties, constraints, shacl_shapes, ... },
                        "assignment": { entities, relationships },
                        "design_layout": { current_view, views, map },
                        "metadata": { catalog, schema, tables, table_count }
                    }
                }
            }

        Excludes: Databricks/registry settings and preferences; generated R2RML, OWL,
        and SQL (rebuilt from source). Constraints, SWRL rules, and axioms live only
        under ``ontology``, not at the project root.
        """
        # Get current version from domain node
        version = self._data["domain"].get("current_version", "1")

        # Export info from project.info (without version - version is at versions level)
        info_export = {
            "name": self._data["domain"]["info"].get("name", "NewDomain"),
            "description": self._data["domain"]["info"].get("description", ""),
            "author": self._data["domain"]["info"].get("author", ""),
            "llm_endpoint": self._data["domain"]["info"].get("llm_endpoint", ""),
            "mcp_enabled": self._data["domain"]["info"].get("mcp_enabled", False),
            "last_update": self._data["domain"].get("last_update", ""),
            "last_build": self._data["domain"].get("last_build", ""),
        }

        ontology_export = {
            "name": self._data["ontology"].get("name", ""),
            "base_uri": self._data["ontology"].get("base_uri", ""),
            "description": self._data["ontology"].get("description", ""),
            "classes": self._data["ontology"].get("classes", []),
            "properties": self._data["ontology"].get("properties", []),
            "constraints": self._data["ontology"].get("constraints", []),
            "shacl_shapes": self._data["ontology"].get("shacl_shapes", []),
            "swrl_rules": self._data["ontology"].get("swrl_rules", []),
            "decision_tables": self._data["ontology"].get("decision_tables", []),
            "sparql_rules": self._data["ontology"].get("sparql_rules", []),
            "aggregate_rules": self._data["ontology"].get("aggregate_rules", []),
            "axioms": self._data["ontology"].get("axioms", []),
            "expressions": self._data["ontology"].get("expressions", []),
            "groups": self._data["ontology"].get("groups", []),
        }

        # Export mapping WITHOUT r2rml_output (R2RML is generated, not saved)
        assignment_export = {
            "entities": self._data["assignment"].get("entities", []),
            "relationships": self._data["assignment"].get("relationships", []),
        }

        # Export design layout (views with entities/relationships/inheritances, and map layout)
        design_export = self._data.get("design_layout", {})

        # Export metadata (Unity Catalog metadata)
        metadata_export = self.catalog_metadata

        version_data = {
            "ontology": ontology_export,
            "assignment": assignment_export,
            "design_layout": design_export,
        }

        # Only include metadata if it has content
        if metadata_export and metadata_export.get("tables"):
            version_data["metadata"] = metadata_export

        export = {"info": info_export, "versions": {version: version_data}}

        ts = self.triplestore
        delta = ts.get("delta", {})
        if any(v for v in delta.values()):
            export["delta"] = delta

        return export

    def import_from_file(self, data: Dict[str, Any], version: str = None):
        """Import domain data from loaded file.

        Args:
            data: Domain data dictionary
            version: Optional specific version to load (for versioned format)

        Supports multiple formats:
        1. New versioned format: { info: {...}, versions: { "1": { ontology, mapping, design_layout } } }
        2. Legacy flat format: { info: {...}, ontology: {...}, mapping: {...}, ... }
        3. Very old format: constraints/swrl_rules/axioms at top level

        Note:
        - Databricks connection settings are NOT imported (credentials stay local).
        - R2RML output is NOT imported (it's regenerated from mappings).
        - Root-level constraints/swrl_rules/axioms are migrated to ontology.
        """
        if isinstance(data, dict):
            wrapped = data.get("domain") or data.get("project")
            if isinstance(wrapped, dict) and "info" in wrapped and "info" not in data:
                data = wrapped

        empty = get_empty_domain()

        # Import info into domain.info
        if "info" in data:
            info = data["info"]
            self._data["domain"]["info"]["name"] = info.get("name", "NewDomain")
            self._data["domain"]["info"]["description"] = info.get("description", "")
            self._data["domain"]["info"]["author"] = info.get("author", "")
            self._data["domain"]["info"]["llm_endpoint"] = info.get("llm_endpoint", "")
            self._data["domain"]["info"]["mcp_enabled"] = info.get("mcp_enabled", False)
            self._data["domain"]["last_update"] = info.get("last_update", "")
            self._data["domain"]["last_build"] = info.get("last_build", "")
            ts = self._data["domain"].setdefault(
                "triplestore", get_empty_domain()["domain"]["triplestore"].copy()
            )
            ts.pop("backend", None)
            # Migrate legacy triplestore_table into triplestore.delta
            ts_full = info.get("triplestore_table", "")
            if ts_full:
                parts = ts_full.split(".")
                d = ts.setdefault(
                    "delta", get_empty_domain()["domain"]["triplestore"]["delta"].copy()
                )
                if len(parts) >= 3:
                    d["catalog"], d["schema"] = parts[0], parts[1]
                    d["table_name"] = ".".join(parts[2:])
                else:
                    d["table_name"] = ts_full

        # Check for new versioned format
        if "versions" in data:
            versions = data["versions"]
            version_keys = sorted(versions.keys(), reverse=True)

            if version_keys:
                # Use specified version or default to latest
                if version and version in versions:
                    selected_version = version
                else:
                    selected_version = version_keys[0]  # Latest version

                self._data["domain"]["current_version"] = selected_version
                version_data = versions[selected_version]

                if "ontology" in version_data:
                    merged_ont = {**empty["ontology"], **version_data["ontology"]}
                    # Migrate old single-list format that has no expressions key
                    if "expressions" not in version_data.get("ontology", {}):
                        ax, ex = _split_axioms_expressions(merged_ont.get("axioms", []))
                        merged_ont["axioms"] = ax
                        merged_ont["expressions"] = ex
                    self._data["ontology"] = merged_ont

                if "assignment" in version_data or "mapping" in version_data:
                    # Import mapping but strip out r2rml_output (it's regenerated)
                    ad = version_data.get("assignment") or version_data.get(
                        "mapping", {}
                    )
                    self._data["assignment"] = {
                        "entities": ad.get(
                            "entities", ad.get("data_source_mappings", [])
                        ),
                        "relationships": ad.get(
                            "relationships", ad.get("relationship_mappings", [])
                        ),
                        "r2rml_output": "",  # Always empty - R2RML is regenerated
                    }

                if "design_layout" in version_data:
                    self._data["design_layout"] = version_data["design_layout"]

                # Import metadata (Unity Catalog metadata)
                if "metadata" in version_data:
                    self._data["domain"]["metadata"] = version_data["metadata"]
                else:
                    self._data["domain"]["metadata"] = {}
        else:
            # Legacy flat format
            # Handle version from info (old format had version in info)
            if "info" in data and "version" in data["info"]:
                self._data["domain"]["current_version"] = data["info"]["version"]

            if "ontology" in data:
                merged_ont = {**empty["ontology"], **data["ontology"]}
                if "expressions" not in data.get("ontology", {}):
                    ax, ex = _split_axioms_expressions(merged_ont.get("axioms", []))
                    merged_ont["axioms"] = ax
                    merged_ont["expressions"] = ex
                self._data["ontology"] = merged_ont

            # Handle very old files where constraints/swrl_rules/axioms are at top level
            if "constraints" in data and "constraints" not in data.get("ontology", {}):
                self._data["ontology"]["constraints"] = data["constraints"]

            if "swrl_rules" in data and "swrl_rules" not in data.get("ontology", {}):
                self._data["ontology"]["swrl_rules"] = data["swrl_rules"]

            if "axioms" in data and "axioms" not in data.get("ontology", {}):
                all_ax = data["axioms"]
                ax, ex = _split_axioms_expressions(all_ax)
                self._data["ontology"]["axioms"] = ax
                self._data["ontology"]["expressions"] = ex

            if "assignment" in data or "mapping" in data:
                # Import mapping but strip out r2rml_output (it's regenerated)
                ad = data.get("assignment") or data.get("mapping", {})
                self._data["assignment"] = {
                    "entities": ad.get("entities", ad.get("data_source_mappings", [])),
                    "relationships": ad.get(
                        "relationships", ad.get("relationship_mappings", [])
                    ),
                    "r2rml_output": "",  # Always empty - R2RML is regenerated
                }

            if "design_layout" in data:
                self._data["design_layout"] = data["design_layout"]

            # Import metadata (Unity Catalog metadata) from legacy format
            if "metadata" in data:
                self._data["domain"]["metadata"] = data["metadata"]
            else:
                self._data["domain"]["metadata"] = {}

        # Import delta into domain.triplestore
        empty_ts = get_empty_domain()["domain"]["triplestore"]
        ts = self._data["domain"].setdefault("triplestore", empty_ts.copy())
        if "delta" in data:
            ts["delta"] = {**empty_ts["delta"], **data["delta"]}
        ts.pop("lakebase", None)
        ts.pop("backend", None)

        # Clear cached triplestore stats — they belong to the previous project
        ts.pop("stats", None)
        ts.pop("build_last_update", None)
        ts.pop("_ts_cache_timestamp", None)

        # Ensure inherited dataProperties are propagated for saved domains
        # whose classes may have been stored before inheritance resolution.
        self._ensure_inherited_properties()

        # A freshly loaded project has no unsaved changes
        self.clear_change_flags()
        self._rebake_snapshots()

        # Databricks settings are NOT imported - they stay local to the environment


# ===========================================
# Helper function for routes
# ===========================================


def get_domain(session_mgr) -> DomainSession:
    """Get or create a DomainSession for this request.

    The instance is cached on ``request.state`` so that multiple
    callers within the same request (e.g. middleware + route handler)
    share one ``DomainSession`` and avoid repeated
    ``_ensure_structure()`` work.
    """
    request = getattr(session_mgr, "request", None)
    if request is not None:
        cached = getattr(request.state, "_domain_session", None)
        if cached is not None:
            return cached
    ps = DomainSession(session_mgr)
    if request is not None:
        request.state._domain_session = ps
    return ps
