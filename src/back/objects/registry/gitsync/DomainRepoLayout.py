"""Map a registry version document to/from the GitHub repository file tree.

Repository layout (relative to the configured base path)::

    domains/<Folder>/domain.json                 # summary, not read on import
    domains/<Folder>/versions/V<n>/
        manifest.json
        info.json
        ontology/ontology.json                   # classes, properties, groups, ...
        ontology/{constraints,axioms,expressions,shacl_shapes}.json
        ontology/rules/{swrl,sparql,decision_tables,aggregate,cohort}.json
        ontology/<Folder>.owl.ttl                # generated OWL (Turtle)
        ontology/shacl.ttl                       # generated SHACL (when shapes exist)
        mapping/assignment.json
        mapping/<Folder>.r2rml.ttl               # generated R2RML (when mappings exist)
        layout/design.json
        layout/views/<View>.json
        layout/map.json
        metadata/tables.json
        simulation/scenarios.json            # what-if scenario specs
        simulation/runs.json                 # simulation run history

JSON files are the source of truth; ``.ttl`` files and ``manifest.json``
are derived and ignored by :meth:`DomainRepoLayout.merge_version`.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from shared.config.constants import APP_VERSION, DEFAULT_BASE_URI

logger = get_logger(__name__)


class DomainRepoLayout:
    """Pure split/merge between version documents and repository files."""

    FORMAT_VERSION = 1

    ONTOLOGY_SPLIT: Dict[str, str] = {
        "constraints": "ontology/constraints.json",
        "axioms": "ontology/axioms.json",
        "expressions": "ontology/expressions.json",
        "shacl_shapes": "ontology/shacl_shapes.json",
        "swrl_rules": "ontology/rules/swrl.json",
        "sparql_rules": "ontology/rules/sparql.json",
        "decision_tables": "ontology/rules/decision_tables.json",
        "aggregate_rules": "ontology/rules/aggregate.json",
        "cohort_rules": "ontology/rules/cohort.json",
    }

    VOLATILE_INFO_KEYS = ("last_update", "last_build", "graph_cache_state")
    SECRET_KEYS = frozenset({"password", "client_secret", "access_token", "token"})

    _VERSION_PATH_RE = re.compile(r"^domains/([^/]+)/versions/V([^/]+)/(.+)$")
    _UNSAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @staticmethod
    def domain_dir(folder: str) -> str:
        return f"domains/{folder}"

    @staticmethod
    def version_dir(folder: str, version: str) -> str:
        return f"domains/{folder}/versions/V{version}"

    @staticmethod
    def owl_path(folder: str) -> str:
        return f"ontology/{folder}.owl.ttl"

    @staticmethod
    def r2rml_path(folder: str) -> str:
        return f"mapping/{folder}.r2rml.ttl"

    @classmethod
    def parse_version_path(cls, path: str) -> Optional[Tuple[str, str, str]]:
        """Return ``(folder, version, relative_path)`` for a version file path."""
        m = cls._VERSION_PATH_RE.match(path)
        return (m.group(1), m.group(2), m.group(3)) if m else None

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def dump(value: Any) -> str:
        return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"

    @staticmethod
    def sha256(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @classmethod
    def _scrub(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                k: cls._scrub(v)
                for k, v in value.items()
                if str(k).lower() not in cls.SECRET_KEYS
            }
        if isinstance(value, list):
            return [cls._scrub(v) for v in value]
        return value

    @staticmethod
    def _version_blob(doc: Dict[str, Any], version: str) -> Dict[str, Any]:
        versions = doc.get("versions")
        blob = versions.get(version) if isinstance(versions, dict) else None
        if isinstance(blob, dict):
            return blob
        return {
            k: doc.get(k)
            for k in (
                "ontology",
                "assignment",
                "design_layout",
                "metadata",
                "simulations",
                "simulation_runs",
            )
            if k in doc
        }

    # ------------------------------------------------------------------
    # Split
    # ------------------------------------------------------------------

    @classmethod
    def split_source(cls, doc: Dict[str, Any], version: str) -> Dict[str, str]:
        """Return the JSON source files (``{relative_path: text}``) of one version."""
        blob = cls._version_blob(doc, version)
        files: Dict[str, str] = {}

        info = cls._scrub(dict(doc.get("info") or {}))
        for key in cls.VOLATILE_INFO_KEYS:
            info.pop(key, None)
        files["info.json"] = cls.dump(info)

        ontology = dict(blob.get("ontology") or {})
        for key, rel in cls.ONTOLOGY_SPLIT.items():
            if key in ontology:
                files[rel] = cls.dump(ontology.pop(key))
        files["ontology/ontology.json"] = cls.dump(ontology)

        files["mapping/assignment.json"] = cls.dump(blob.get("assignment") or {})

        layout = dict(blob.get("design_layout") or {})
        if "map" in layout:
            files["layout/map.json"] = cls.dump(layout.pop("map"))
        views = layout.get("views")
        if isinstance(views, dict):
            layout.pop("views")
            index: List[Dict[str, str]] = []
            used: set = set()
            for name, view in views.items():
                stem = cls._UNSAFE_NAME_RE.sub("_", str(name)).strip("._") or "view"
                candidate, n = stem, 2
                while candidate.lower() in used:
                    candidate, n = f"{stem}_{n}", n + 1
                used.add(candidate.lower())
                rel = f"layout/views/{candidate}.json"
                files[rel] = cls.dump(view)
                index.append({"name": str(name), "file": rel})
            layout["view_files"] = index
        files["layout/design.json"] = cls.dump(layout)

        metadata = blob.get("metadata")
        if metadata:
            files["metadata/tables.json"] = cls.dump(metadata)
        if "simulations" in blob:
            files["simulation/scenarios.json"] = cls.dump(blob.get("simulations") or [])
        if "simulation_runs" in blob:
            files["simulation/runs.json"] = cls.dump(blob.get("simulation_runs") or [])
        return files

    @classmethod
    def generate_turtle(
        cls, doc: Dict[str, Any], folder: str, version: str
    ) -> Tuple[Dict[str, str], List[str]]:
        """Generate the OWL, R2RML and SHACL Turtle files of one version.

        Returns ``(files, errors)``; a failing generator is reported in
        *errors* and never prevents the JSON sources from being mirrored.
        """
        blob = cls._version_blob(doc, version)
        ontology = blob.get("ontology") or {}
        assignment = blob.get("assignment") or {}
        base_uri = ontology.get("base_uri") or DEFAULT_BASE_URI
        files: Dict[str, str] = {}
        errors: List[str] = []

        try:
            from back.objects.ontology import Owl

            files[cls.owl_path(folder)] = Owl.generate_owl(
                ontology,
                ontology.get("constraints"),
                ontology.get("swrl_rules"),
                ontology.get("axioms"),
                ontology.get("expressions"),
                ontology.get("groups"),
            )
        except Exception as exc:  # noqa: BLE001 - reported, not fatal
            logger.warning("OWL generation failed for %s/V%s: %s", folder, version, exc)
            errors.append(f"OWL generation failed: {exc}")

        if assignment.get("entities"):
            try:
                from back.core.w3c import R2RMLGenerator

                files[cls.r2rml_path(folder)] = R2RMLGenerator(
                    base_uri
                ).generate_mapping(assignment, ontology)
            except Exception as exc:  # noqa: BLE001 - reported, not fatal
                logger.warning(
                    "R2RML generation failed for %s/V%s: %s", folder, version, exc
                )
                errors.append(f"R2RML generation failed: {exc}")

        shapes = ontology.get("shacl_shapes") or []
        if shapes:
            try:
                from back.core.w3c import SHACLGenerator

                files["ontology/shacl.ttl"] = SHACLGenerator(base_uri).generate(shapes)
            except Exception as exc:  # noqa: BLE001 - reported, not fatal
                logger.warning(
                    "SHACL generation failed for %s/V%s: %s", folder, version, exc
                )
                errors.append(f"SHACL generation failed: {exc}")
        return files, errors

    @classmethod
    def split_version(
        cls, doc: Dict[str, Any], folder: str, version: str
    ) -> Tuple[Dict[str, str], List[str]]:
        """Return every file of one version folder plus generation errors."""
        files = cls.split_source(doc, version)
        generated, errors = cls.generate_turtle(doc, folder, version)
        files.update(generated)
        info = doc.get("info") or {}
        manifest = {
            "format_version": cls.FORMAT_VERSION,
            "ontobricks_version": APP_VERSION,
            "domain": folder,
            "version": str(version),
            "status": info.get("status") or "DRAFT",
            "last_update": info.get("last_update") or "",
            "generated": sorted(generated),
            "files": {rel: cls.sha256(text) for rel, text in sorted(files.items())},
        }
        files["manifest.json"] = cls.dump(manifest)
        return files, errors

    @classmethod
    def domain_summary(
        cls, doc: Dict[str, Any], folder: str, version: str, versions: List[str]
    ) -> str:
        info = doc.get("info") or {}
        ontology = cls._version_blob(doc, version).get("ontology") or {}
        return cls.dump(
            {
                "folder": folder,
                "name": info.get("name") or folder,
                "description": info.get("description") or "",
                "base_uri": ontology.get("base_uri") or "",
                "versions": list(versions),
            }
        )

    @classmethod
    def domain_readme(
        cls, doc: Dict[str, Any], folder: str, version: str, versions: List[str]
    ) -> str:
        """Human summary of a domain (contents of *version* + version list)."""
        info = doc.get("info") or {}
        ontology = cls._version_blob(doc, version).get("ontology") or {}
        classes = sorted(
            str(c.get("label") or c.get("name") or "")
            for c in ontology.get("classes") or []
            if isinstance(c, dict)
        )
        properties = ontology.get("properties") or []
        lines = [f"# {info.get('name') or folder}", ""]
        if info.get("description"):
            lines += [str(info["description"]), ""]
        lines += [
            f"- Base URI: `{ontology.get('base_uri') or ''}`",
            f"- Summary of: V{version} ({info.get('status') or 'DRAFT'})",
            f"- Classes: {len(classes)} · Properties: {len(properties)}",
            "",
            "| Version | Folder |",
            "|---------|--------|",
        ]
        lines += [f"| V{v} | [versions/V{v}](versions/V{v}) |" for v in versions]
        if classes:
            lines += ["", "## Classes", ""] + [f"- {name}" for name in classes if name]
        return "\n".join(lines) + "\n"

    @classmethod
    def domain_files(
        cls, doc: Dict[str, Any], folder: str, version: str, versions: List[str]
    ) -> Dict[str, str]:
        """``domain.json`` + ``README.md`` for ``domains/<folder>/``."""
        ddir = cls.domain_dir(folder)
        return {
            f"{ddir}/domain.json": cls.domain_summary(doc, folder, version, versions),
            f"{ddir}/README.md": cls.domain_readme(doc, folder, version, versions),
        }

    @classmethod
    def readme(cls, domains: Dict[str, List[str]]) -> str:
        lines = [
            "# OntoBricks ontology repository",
            "",
            "Mirrored automatically by OntoBricks. Edit domains in OntoBricks; "
            "changes made here are only applied through *Import from GitHub*.",
            "",
            "| Domain | Versions |",
            "|--------|----------|",
        ]
        for folder in sorted(domains):
            versions = ", ".join(f"V{v}" for v in domains[folder]) or "-"
            lines.append(f"| [{folder}](domains/{folder}) | {versions} |")
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    @classmethod
    def merge_version(cls, files: Dict[str, str], version: str) -> Dict[str, Any]:
        """Rebuild a registry version document from its JSON source files."""

        def load(rel: str, default: Any) -> Any:
            text = files.get(rel)
            return json.loads(text) if text else default

        manifest = load("manifest.json", {})
        fmt = manifest.get("format_version", cls.FORMAT_VERSION)
        if isinstance(fmt, int) and fmt > cls.FORMAT_VERSION:
            from back.core.errors import ValidationError

            raise ValidationError(
                f"Repository layout format_version={fmt} is newer than this "
                f"OntoBricks build supports ({cls.FORMAT_VERSION}); please upgrade."
            )

        ontology = load("ontology/ontology.json", {})
        for key, rel in cls.ONTOLOGY_SPLIT.items():
            if rel in files:
                ontology[key] = load(rel, [])

        layout = load("layout/design.json", {})
        index = layout.pop("view_files", None)
        if isinstance(index, list):
            layout["views"] = {
                entry["name"]: load(entry["file"], {})
                for entry in index
                if isinstance(entry, dict) and entry.get("name") and entry.get("file")
            }
        if "layout/map.json" in files:
            layout["map"] = load("layout/map.json", {})

        blob = {
            "ontology": ontology,
            "assignment": load("mapping/assignment.json", {}),
            "design_layout": layout,
            "metadata": load("metadata/tables.json", {}),
        }
        if "simulation/scenarios.json" in files:
            blob["simulations"] = load("simulation/scenarios.json", [])
        if "simulation/runs.json" in files:
            blob["simulation_runs"] = load("simulation/runs.json", [])
        return {
            "info": load("info.json", {}),
            "versions": {str(version): blob},
        }
