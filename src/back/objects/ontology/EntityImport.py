"""Copy entities, and the relationships between them, from another domain.

Fowler Extract Class. ``Ontology`` keeps a one-line delegator.
"""

from __future__ import annotations

import copy
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.objects.ontology.ClassModel import ClassModel

if TYPE_CHECKING:
    from back.objects.session.DomainSession import DomainSession

logger = get_logger(__name__)

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DOMAIN_BOUND_FIELDS = ("dataset", "bridges", "actions", "dashboard", "dashboardParams")

Resolver = Callable[[str], Tuple[Optional[str], bool]]


def _uri_base(base_uri: str) -> str:
    base = base_uri or ""
    return base if base.endswith(("#", "/")) else base + "#"


def _normalized(source: Dict[str, Any]) -> Dict[str, Any]:
    """Deep copy of *source* with top-level datatype properties folded into classes."""
    source = copy.deepcopy(source)
    ClassModel.sync_class_data_properties(source)
    return source


def _relationships(ontology: Dict[str, Any]) -> List[Dict[str, Any]]:
    names = {c.get("name") for c in ontology.get("classes") or [] if c.get("name")}
    return [
        p for p in ontology.get("properties") or []
        if p.get("name") and p.get("domain") in names and p.get("range") in names
    ]


class EntityImport:
    """Import a selection of entities from a source ontology into the session."""

    def __init__(self, session: "DomainSession") -> None:
        self._domain = session

    @staticmethod
    def build_catalog(source: Dict[str, Any], target: Dict[str, Any]) -> Dict[str, Any]:
        """Entities and relationships of *source*, flagged against *target* names."""
        source = _normalized(source)
        existing = {(c.get("name") or "").lower() for c in target.get("classes") or []}
        entities = [
            {
                "name": c["name"],
                "label": c.get("label") or c["name"],
                "emoji": c.get("emoji") or "📦",
                "description": c.get("description") or c.get("comment") or "",
                "parent": c.get("parent") or c.get("parentClass") or "",
                "attributes": len(c.get("dataProperties") or []),
                "exists": c["name"].lower() in existing,
            }
            for c in source.get("classes") or []
            if c.get("name")
        ]
        relationships = [
            {"name": p["name"], "domain": p["domain"], "range": p["range"]}
            for p in _relationships(source)
        ]
        return {"entities": entities, "relationships": relationships}

    def import_entities(
        self,
        source: Dict[str, Any],
        names: List[str],
        renames: Dict[str, str],
        *,
        source_domain: str,
        source_version: str,
    ) -> Dict[str, Any]:
        """Copy *names* (plus resolvable relationships) from *source* into the session."""
        s = self._domain
        source = _normalized(source)
        names = list(dict.fromkeys(names))
        source_by_name = {c["name"]: c for c in source.get("classes") or [] if c.get("name")}
        unknown = [n for n in names if n not in source_by_name]
        if unknown:
            raise ValidationError(f"Unknown entities in source domain: {', '.join(unknown)}")

        classes = list(s.get_classes())
        properties = list(s.get_properties())
        existing = {c["name"].lower(): c["name"] for c in classes if c.get("name")}
        renames = {k: (v or "").strip() for k, v in (renames or {}).items() if k in names}
        self._validate_renames(renames, existing)
        selected = set(names)

        def resolve(name: str) -> Tuple[Optional[str], bool]:
            if name in renames:
                return renames[name], True
            if name.lower() in existing:
                return existing[name.lower()], False
            if name in selected:
                return name, True
            return None, False

        base = _uri_base(s.ontology.get("base_uri") or "")
        imported: List[str] = []
        skipped: List[Dict[str, str]] = []
        for name in names:
            new_name, is_new = resolve(name)
            if not is_new:
                skipped.append({"name": name, "reason": "exists"})
                continue
            classes.append(self._copy_class(
                source_by_name[name], new_name, base, resolve, source_domain, source_version
            ))
            imported.append(new_name)

        used_uris = {p.get("uri") for p in properties if p.get("uri")}
        relationships: List[str] = []
        skipped_relationships: List[Dict[str, str]] = []
        for prop in _relationships(source):
            domain, domain_new = resolve(prop["domain"])
            range_, range_new = resolve(prop["range"])
            if not domain or not range_ or not (domain_new or range_new):
                continue
            uri = base + prop["name"]
            if uri in used_uris:
                skipped_relationships.append({"name": prop["name"], "reason": "uri_exists"})
                continue
            new_prop = copy.deepcopy(prop)
            new_prop.update(domain=domain, range=range_, uri=uri)
            properties.append(new_prop)
            used_uris.add(uri)
            relationships.append(prop["name"])

        if imported or relationships:
            s.ontology["classes"] = classes
            s.ontology["properties"] = properties
            s.clear_generated_content()
            s.record_change(
                "entities_imported",
                entity_type="class",
                entity_ref=source_domain,
                summary=(
                    f"{len(imported)} entities, {len(relationships)} relationships "
                    f"from {source_domain}"
                ),
                meta={
                    "source_version": source_version,
                    "entities": imported,
                    "relationships": relationships,
                },
            )
            s.save()
        logger.info(
            "Entity import from %s v%s: imported=%d skipped=%d relationships=%d",
            source_domain, source_version, len(imported), len(skipped), len(relationships),
        )
        return {
            "success": True,
            "imported": imported,
            "skipped": skipped,
            "relationships": relationships,
            "skipped_relationships": skipped_relationships,
        }

    @staticmethod
    def _validate_renames(renames: Dict[str, str], existing: Dict[str, str]) -> None:
        seen = set()
        for source_name, new_name in renames.items():
            key = new_name.lower()
            if not _NAME_RE.match(new_name):
                raise ValidationError(f'Invalid name "{new_name}" for {source_name}')
            if key in existing or key in seen:
                raise ValidationError(f'An entity named "{new_name}" already exists')
            seen.add(key)

    @staticmethod
    def _copy_class(
        src: Dict[str, Any],
        new_name: str,
        base: str,
        resolve: Resolver,
        source_domain: str,
        source_version: str,
    ) -> Dict[str, Any]:
        cls = copy.deepcopy(src)
        for field in _DOMAIN_BOUND_FIELDS:
            cls.pop(field, None)
        cls.pop("parentClass", None)
        if cls.get("label") in (None, "", src["name"]):
            cls["label"] = new_name
        cls["name"] = new_name
        cls["localName"] = new_name
        cls["uri"] = base + new_name
        parent = src.get("parent") or src.get("parentClass") or ""
        cls["parent"] = (resolve(parent)[0] or "") if parent else ""
        for attr in cls.get("dataProperties") or []:
            if attr.get("name"):
                attr["uri"] = base + attr["name"]
        cls["importedFrom"] = {
            "domain": source_domain,
            "version": source_version,
            "uri": src.get("uri", ""),
        }
        return cls
