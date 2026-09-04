"""Pure URI parsing helpers for the OntoBricks MCP server.

Stateless functions that turn RDF URIs into human-readable local names and
attribute labels. Depend only on :mod:`server.constants`.
"""

from __future__ import annotations

import re

from server.constants import RDFS_LABEL


def _local_name(uri: str) -> str:
    """Extract the human-readable local name from a URI.

    ``https://ontobricks.com/ontology/Customer/CUST00094``  →  ``CUST00094``
    ``http://www.w3.org/1999/02/22-rdf-syntax-ns#type``     →  ``type``
    """
    for sep in ("#", "/"):
        idx = uri.rfind(sep)
        if idx >= 0 and idx < len(uri) - 1:
            return uri[idx + 1 :]
    return uri


def _pretty_predicate(uri: str) -> str:
    """Turn a predicate URI into a readable attribute name.

    ``https://ontobricks.com/ontologylastname``  →  ``lastname``
    Handles both ``#``-separated and path-separated URIs, and also bare
    camelCase concatenation (``ontologylastname`` → ``lastname``).
    """
    name = _local_name(uri)
    m = re.match(r"^ontology(.+)$", name, re.IGNORECASE)
    if m:
        name = m.group(1)
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return name.replace("_", " ").strip()


def _is_uri(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _is_label_predicate(pred: str) -> bool:
    ln = _local_name(pred).lower()
    return ln in ("label", "name") or pred == RDFS_LABEL
