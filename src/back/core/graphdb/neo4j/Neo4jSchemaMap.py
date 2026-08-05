"""Per-graph schema registry for the Neo4j property-graph model.

The typed-node model sanitises class/predicate URIs into Neo4j labels and
relationship types (``.../Customer`` → label ``Customer``; ``.../filedBy`` →
reltype ``filedBy``). To reconstruct the *exact* original SPO triples on read
(so the ``GraphDBBackend`` contract is preserved bit-for-bit), we must reverse
those names back to their full URIs.

This registry persists three reverse maps as JSON on a single reserved
``:__GraphSchema`` node per graph, keyed by the graph's marker label:

* ``label_map``   — sanitised label   → class URI      (for rdf:type reconstruction)
* ``reltype_map`` — sanitised reltype → predicate URI  (for object-property edges)
* ``prop_map``    — sanitised prop key → predicate URI  (for datatype properties)

``rdfs:label`` is special-cased (the ``name`` property always maps back to
``RDFS_LABEL``) and never needs storing.

The registry is idempotent: each write *merges* newly-seen names into the
persisted maps, so incremental inserts never lose earlier mappings.
"""

import json
from typing import Any, Dict

from back.core.graphdb.constants import RDFS_LABEL
from back.core.graphdb.neo4j.Neo4jConnection import Neo4jConnection
from back.core.logging import get_logger

logger = get_logger(__name__)

SCHEMA_LABEL = "__GraphSchema"
_NAME_KEY = "name"


class Neo4jSchemaMap:
    """Load/merge/save the URI↔identifier reverse maps for one graph."""

    def __init__(self, connection: Neo4jConnection) -> None:
        self._conn = connection

    # ------------------------------------------------------------------
    #  Persistence
    # ------------------------------------------------------------------

    def load(self, graph_label: str) -> Dict[str, Dict[str, str]]:
        """Return ``{label_map, reltype_map, prop_map}`` for *graph_label*.

        Missing schema node → empty maps (safe for a never-built graph).
        """
        rows = self._conn.run(
            f"MATCH (s:`{SCHEMA_LABEL}` {{graph: $g}}) "
            f"RETURN s.label_map AS label_map, s.reltype_map AS reltype_map, "
            f"s.prop_map AS prop_map",
            g=graph_label,
        )
        if not rows:
            return {"label_map": {}, "reltype_map": {}, "prop_map": {}}
        row = rows[0]
        return {
            "label_map": _loads(row.get("label_map")),
            "reltype_map": _loads(row.get("reltype_map")),
            "prop_map": _loads(row.get("prop_map")),
        }

    def merge_and_save(
        self,
        graph_label: str,
        label_map: Dict[str, str],
        reltype_map: Dict[str, str],
        prop_map: Dict[str, str],
    ) -> None:
        """Merge new mappings into the persisted schema node (idempotent)."""
        existing = self.load(graph_label)
        merged_labels = {**existing["label_map"], **label_map}
        merged_reltypes = {**existing["reltype_map"], **reltype_map}
        merged_props = {**existing["prop_map"], **prop_map}
        self._conn.run(
            f"MERGE (s:`{SCHEMA_LABEL}` {{graph: $g}}) "
            f"SET s.label_map = $lm, s.reltype_map = $rm, s.prop_map = $pm",
            g=graph_label,
            lm=json.dumps(merged_labels),
            rm=json.dumps(merged_reltypes),
            pm=json.dumps(merged_props),
        )
        logger.info(
            "Neo4j schema map for %s: %d labels, %d reltypes, %d props",
            graph_label,
            len(merged_labels),
            len(merged_reltypes),
            len(merged_props),
        )

    def drop(self, graph_label: str) -> None:
        """Remove the schema node for *graph_label* (called on drop_table)."""
        self._conn.run(
            f"MATCH (s:`{SCHEMA_LABEL}` {{graph: $g}}) DELETE s", g=graph_label
        )

    # ------------------------------------------------------------------
    #  Reverse lookups (used by read reconstruction, P2)
    # ------------------------------------------------------------------

    @staticmethod
    def predicate_for_property(prop_map: Dict[str, str], key: str) -> str:
        """Full predicate URI for a sanitised property key.

        ``name`` always reverses to ``rdfs:label``; otherwise consult the map,
        falling back to the key itself if unseen (defensive — should not happen
        for maps written by this backend).
        """
        if key == _NAME_KEY:
            return RDFS_LABEL
        return prop_map.get(key, key)


def _loads(value: Any) -> Dict[str, str]:
    """Parse a JSON string map; tolerate None / already-dict / bad JSON."""
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        return {}
