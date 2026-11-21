"""Instance IRI scope: which class segment goes into minted instance URIs.

``class`` (default) mints ``{base}{Class}/{id}``. ``hierarchy`` mints
``{base}{RootClass}/{id}`` so a parent and a subclass mapped with the same
business id become one node (issue #228). The root follows the designer
``parent`` chain only.
"""

import re
from typing import Any, Dict, List, Optional

from back.core.w3c.rdf_utils import uri_local_name

SCOPE_CLASS = "class"
SCOPE_HIERARCHY = "hierarchy"
_SCOPES = frozenset({SCOPE_CLASS, SCOPE_HIERARCHY})


class InstanceIdentity:
    """Resolve instance-URI class segments for the configured IRI scope."""

    @staticmethod
    def normalize_scope(raw: Any) -> str:
        """Return ``hierarchy`` or ``class`` (default for any other value)."""
        value = str(raw or "").strip().lower() if isinstance(raw, str) else ""
        return value if value in _SCOPES else SCOPE_CLASS

    @staticmethod
    def sanitize_segment(name: Any) -> str:
        """Make *name* safe for an IRI path segment."""
        if name is None:
            return "unknown"
        return re.sub(r"[^a-zA-Z0-9_]", "_", str(name)) or "unknown"

    @staticmethod
    def segment_map(ontology_config: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """Map class segments, names and labels to their root-class segment.

        Empty unless the ontology opts into the hierarchy scope, so callers can
        always use ``segment_map.get(segment, segment)``.
        """
        by_name = InstanceIdentity._hierarchy_classes(ontology_config)
        result: Dict[str, str] = {}
        labels: Dict[str, str] = {}
        for name, cls in by_name.items():
            root = by_name[InstanceIdentity._ancestry(by_name, name)[-1]]
            root_segment = InstanceIdentity._class_segment(root)
            result[InstanceIdentity._class_segment(cls)] = root_segment
            result[InstanceIdentity.sanitize_segment(name)] = root_segment
            if cls.get("label"):
                labels[InstanceIdentity.sanitize_segment(cls["label"])] = root_segment
        return {**labels, **result}

    @staticmethod
    def sibling_collisions(
        ontology_config: Optional[Dict[str, Any]], entity_mappings: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """Return mapped classes whose equal ids would merge with an unrelated peer.

        Keyed by the mapping ``ontology_class``; values are the sorted names of
        mapped classes under the same root that are neither ancestors nor
        descendants of it.
        """
        by_name = InstanceIdentity._hierarchy_classes(ontology_config)
        if not by_name:
            return {}
        mapped: Dict[str, str] = {}
        for mapping in entity_mappings or []:
            class_ref = mapping.get("ontology_class") or ""
            name = InstanceIdentity._class_name_for(by_name, class_ref)
            if name and not mapping.get("excluded"):
                mapped[class_ref] = name
        chains = {
            name: InstanceIdentity._ancestry(by_name, name) for name in set(mapped.values())
        }
        result: Dict[str, List[str]] = {}
        for class_ref, name in mapped.items():
            peers = sorted(
                other
                for other, chain in chains.items()
                if other != name
                and chain[-1] == chains[name][-1]
                and other not in chains[name]
                and name not in chain
            )
            if peers:
                result[class_ref] = peers
        return result

    @staticmethod
    def flag_collisions(
        ontology_config: Optional[Dict[str, Any]],
        entity_mappings: List[Dict[str, Any]],
        entity_results: List[Dict[str, Any]],
    ) -> None:
        """Append an ``identity_scope`` warning to colliding mapping diagnostics."""
        collisions = InstanceIdentity.sibling_collisions(
            ontology_config, entity_mappings
        )
        for mapping, result in zip(entity_mappings, entity_results):
            peers = collisions.get(mapping.get("ontology_class") or "")
            if not peers:
                continue
            result["checks"].append(
                {
                    "check": "identity_scope",
                    "status": "warning",
                    "detail": "Hierarchy IRI scope: equal IDs merge with "
                    + ", ".join(peers),
                }
            )
            if result.get("status") == "ok":
                result["status"] = "warning"

    @staticmethod
    def apply_scope(ontology: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Store ``data['identity_scope']`` on *ontology*; return True when it changed."""
        if "identity_scope" not in data:
            return False
        scope = InstanceIdentity.normalize_scope(data.get("identity_scope"))
        if scope == InstanceIdentity.normalize_scope(ontology.get("identity_scope")):
            return False
        ontology["identity_scope"] = scope
        return True

    @staticmethod
    def _hierarchy_classes(
        ontology_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict]:
        ontology_config = ontology_config or {}
        scope = InstanceIdentity.normalize_scope(
            ontology_config.get("identity_scope")
        )
        if scope != SCOPE_HIERARCHY:
            return {}
        return {
            c["name"]: c
            for c in ontology_config.get("classes") or []
            if c.get("name")
        }

    @staticmethod
    def _ancestry(by_name: Dict[str, Dict], class_name: str) -> List[str]:
        """Nearest-first chain including *class_name*; stops on cycles and gaps."""
        chain: List[str] = []
        current = by_name.get(class_name)
        while current is not None and current["name"] not in chain:
            chain.append(current["name"])
            current = by_name.get(current.get("parent") or "")
        return chain

    @staticmethod
    def _class_segment(cls: Dict[str, Any]) -> str:
        uri = cls.get("uri") or ""
        return InstanceIdentity.sanitize_segment(
            (uri_local_name(uri) if uri else "") or cls.get("name")
        )

    @staticmethod
    def _class_name_for(by_name: Dict[str, Dict], class_ref: str) -> str:
        if not class_ref:
            return ""
        for name, cls in by_name.items():
            if cls.get("uri") == class_ref:
                return name
        local = uri_local_name(class_ref) or class_ref
        for name, cls in by_name.items():
            if local in (name, uri_local_name(cls.get("uri") or "")):
                return name
        return ""
