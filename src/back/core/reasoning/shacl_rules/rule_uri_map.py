"""Build the lowercase-name -> URI map for rule translation.

Property URIs are
normalised to the **data namespace** (``base_uri`` with a trailing ``/``) so
they match the predicates written by the R2RML generator when syncing data to
the triple store. Class URIs keep their original ``#`` separator because
``rdf:type`` objects in the store use the ontology class URI as-is.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.logging import get_logger

logger = get_logger(__name__)


def _uri_local_name(uri: str) -> str:
    """Local name (fragment or last path segment) of a URI.

    Inlined to avoid importing ``back.core.w3c`` at module load time (that
    package triggers a heavy import chain and a circular import when the
    reasoning layer is imported first).
    """
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rsplit("/", 1)[-1]


def build_uri_map(ontology: Dict[str, Any] | None) -> Dict[str, str]:
    """Return a lowercase class/property name -> URI map for *ontology*."""
    ontology = ontology or {}
    uri_map: Dict[str, str] = {}
    base_uri = ontology.get("base_uri", "")
    sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
    data_ns = base_uri.rstrip("#").rstrip("/") + "/" if base_uri else ""

    for cls in ontology.get("classes", []):
        name = cls.get("name", "") or cls.get("localName", "")
        uri = cls.get("uri", "")
        if not uri and name:
            uri = base_uri + sep + name
        if name:
            uri_map[name.lower()] = uri

    # Datatype properties live on the classes; fold them in so that
    # ``nights(?x, ?n)`` resolves to the data namespace like object properties
    # do. Object properties come last so they win a same-name clash.
    data_props = [
        p
        for cls_def in ontology.get("classes", [])
        for p in cls_def.get("dataProperties", []) or []
    ]
    for prop in data_props + list(ontology.get("properties", [])):
        name = prop.get("name", "") or prop.get("localName", "")
        uri = prop.get("uri", "")
        if data_ns and uri and not uri.startswith(data_ns):
            local = _uri_local_name(uri)
            uri = data_ns + local
        elif not uri and name:
            uri = data_ns + name if data_ns else base_uri + sep + name
        if name:
            uri_map[name.lower()] = uri

    logger.debug(
        "rule uri_map (%d entries, data_ns=%s)",
        len(uri_map),
        data_ns,
    )
    return uri_map
