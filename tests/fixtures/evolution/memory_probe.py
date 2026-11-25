"""In-memory SourceProbe over the supply-chain fixture rows (mirrors supply_chain.sql).

Lets the Evolve coverage, proposal and validation code run offline with the
same data the live fixture seeds. Keep the rows in sync with supply_chain.sql;
``test_memory_probe_matches_gold`` recomputes the gold answers from them.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Set

from back.objects.ontology.evolution import (
    SourceAccessDenied,
    SourceProbe,
    SourceUnavailable,
)

SUPPLIERS = [("S1", "Acme"), ("S2", "Globex"), ("S3", "Initech"), ("S4", "Umbrella")]
PRODUCTS = [
    ("P1", "Widget", "W-001", "S1"),
    ("P2", "Gadget", "G-002", "S2"),
    ("P3", "Sprocket", "S-003", "S1"),
    ("P4", "Gizmo", "Z-004", "S3"),
    ("P5", "Doohickey", "D-005", "S4"),
]
ORDERS = [
    ("O1", "Northwind", "2026-01-05"),
    ("O2", "Contoso", "2026-01-09"),
    ("O3", "Fabrikam", "2026-02-01"),
    ("O4", "Northwind", "2026-02-14"),
    ("O5", "Tailspin", "2026-03-02"),
    ("O6", "Contoso", "2026-03-20"),
]
ORDER_LINES = [
    ("L1", "O1", "P1", "W-001", 2),
    ("L2", "O1", "P2", "G-002", 1),
    ("L3", "O2", "P2", "G-002", 5),
    ("L4", "O3", "P3", "S-003", 3),
    ("L5", "O4", "P4", "Z-004", 1),
    ("L6", "O5", "P1", "W-001", 4),
    ("L7", "O5", "P5", "D-005", 2),
    ("L8", "O6", "P5", "D-005", 6),
]

_COLS = {
    "suppliers": (("supplier_id", "STRING"), ("supplier_name", "STRING")),
    "products": (
        ("product_id", "STRING"),
        ("product_name", "STRING"),
        ("sku", "STRING"),
        ("supplier_id", "STRING"),
    ),
    "orders": (
        ("order_id", "STRING"),
        ("customer_name", "STRING"),
        ("order_date", "DATE"),
    ),
    "order_lines": (
        ("line_id", "STRING"),
        ("order_id", "STRING"),
        ("product_id", "STRING"),
        ("alt_product_ref", "STRING"),
        ("quantity", "INT"),
    ),
    "restricted_costs": (("product_id", "STRING"), ("unit_cost", "DECIMAL(10,2)")),
}
_SHAPE = {
    "suppliers": "suppliers",
    "suppliers_ambiguous": "suppliers",
    "products": "products",
    "products_dup": "products",
    "orders": "orders",
    "order_lines": "order_lines",
    "order_lines_nofk": "order_lines",
    "order_lines_orphan": "order_lines",
    "restricted_costs": "restricted_costs",
}
_FKS = {
    "products": [("supplier_id", "suppliers", "supplier_id")],
    "order_lines": [
        ("order_id", "orders", "order_id"),
        ("product_id", "products", "product_id"),
    ],
}
_SIMPLE = re.compile(r"^\s*SELECT\s+(\w+)\s*,\s*(\w+)\s+FROM\s+([\w.]+)\s*$", re.I)
_JOINED = re.compile(
    r"^\s*SELECT\s+s\.(\w+)\s+AS\s+\w+\s*,\s*t\.(\w+)\s+AS\s+\w+\s+"
    r"FROM\s+([\w.]+)\s+s\s+JOIN\s+([\w.]+)"
    r"\s+t\s+ON\s+s\.(\w+)\s*=\s*t\.(\w+)\s*$",
    re.I,
)


def _rows(name: str) -> List[Dict[str, object]]:
    data = {
        "suppliers": SUPPLIERS,
        "suppliers_ambiguous": SUPPLIERS + [("S5", "Acme")],
        "products": PRODUCTS,
        "products_dup": PRODUCTS + [("P1", "Widget (relabelled)", "W-001", "S2")],
        "orders": ORDERS,
        "order_lines": ORDER_LINES,
        "order_lines_nofk": ORDER_LINES,
        "order_lines_orphan": ORDER_LINES + [("L9", "O6", "P99", "X-999", 1)],
        "restricted_costs": [
            ("P1", 3.10),
            ("P2", 7.25),
            ("P3", 1.40),
            ("P4", 9.99),
            ("P5", 0.55),
        ],
    }[name]
    cols = [c for c, _ in _COLS[_SHAPE[name]]]
    return [dict(zip(cols, r)) for r in data]


GOLD = {
    "acme_orders": ["O1", "O3", "O5"],
    "globex_orders": ["O1", "O2"],
    "umbrella_orders": ["O5", "O6"],
    "initech_orders": ["O4"],
    "acme_products": ["P1", "P3"],
    "globex_products": ["P2"],
    "o1_lines": ["L1", "L2"],
    "o5_lines": ["L6", "L7"],
    "northwind_lines": ["L1", "L2", "L5"],
}


class MemorySourceProbe(SourceProbe):
    """Fixture tables under ``<catalog>.<schema>``; *denied* tables raise access errors."""

    def __init__(
        self, catalog: str = "cat", schema: str = "fix", denied: Iterable[str] = ()
    ):
        self._prefix = f"{catalog}.{schema}."
        self._denied: Set[str] = set(denied)
        self.calls: List[str] = []
        self.versions: Dict[str, int] = {}

    def _name(self, table: str) -> str:
        self.calls.append(table)
        if not table.startswith(self._prefix):
            raise SourceUnavailable("unknown table", detail=table)
        name = table[len(self._prefix) :]
        if name in self._denied:
            raise SourceAccessDenied("denied", detail=table)
        if name not in _SHAPE:
            raise SourceUnavailable("missing table", detail=table)
        return name

    def columns(self, table: str) -> Dict[str, str]:
        return dict(_COLS[_SHAPE[self._name(table)]])

    def foreign_keys(self, table: str) -> List[Dict[str, str]]:
        name = self._name(table)
        if name != _SHAPE[name]:
            return []  # CTAS variants carry no constraints, like the live fixture
        return [
            {"column": c, "ref_table": self._prefix + t, "ref_column": rc}
            for c, t, rc in _FKS.get(name, [])
        ]

    def count_value(self, table: str, column: str, value: str) -> int:
        return sum(1 for r in _rows(self._name(table)) if str(r[column]) == str(value))

    def key_is_unique(self, table: str, column: str) -> bool:
        values = [r[column] for r in _rows(self._name(table)) if r[column] is not None]
        return len(values) == len(set(values))

    def join_profile(self, source, source_column, target, target_column):
        src, tgt = _rows(self._name(source)), _rows(self._name(target))
        keys = [r[target_column] for r in tgt]
        vals = [r[source_column] for r in src if r[source_column] is not None]
        return {
            "source_rows": len(vals),
            "matched": sum(1 for v in vals if v in set(keys)),
            "target_column_unique": len(keys) == len(set(keys)),
        }

    def _query(self, sql: str) -> List[tuple]:
        simple = _SIMPLE.match(sql)
        if simple:
            a, b, table = simple.groups()
            return [(r[a], r[b]) for r in _rows(self._name(table))]
        joined = _JOINED.match(sql)
        if joined:
            a, b, s_table, t_table, sc, tc = joined.groups()
            tgt = _rows(self._name(t_table))
            return [
                (s[a], t[b])
                for s in _rows(self._name(s_table))
                for t in tgt
                if s[sc] == t[tc]
            ]
        raise ValueError(f"MemorySourceProbe cannot run: {sql}")

    def mapping_profile(
        self, sql, source_column, target_column, source_entity, target_entity
    ):
        pairs = self._query(sql)
        src_ids = {
            r[source_entity["id_column"]]
            for r in _rows(self._name(source_entity["table"]))
        }
        tgt_ids = {
            r[target_entity["id_column"]]
            for r in _rows(self._name(target_entity["table"]))
        }
        return {
            "rows": len(pairs),
            "dangling_source": sum(1 for s, _ in pairs if s not in src_ids),
            "dangling_target": sum(1 for _, t in pairs if t not in tgt_ids),
        }

    def table_version(self, table: str) -> int:
        self._name(table)
        return self.versions.get(table, 1)

    def gold_ids(self, table: str, question_id: str) -> List[str]:
        from back.core.errors import NotFoundError

        if question_id not in GOLD:
            raise NotFoundError("unknown question", detail=question_id)
        return sorted(GOLD[question_id])

    def run_pairs(self, sql: str) -> List[tuple]:
        """Rows of a two-column mapping SELECT (test helper)."""
        return self._query(sql)


def table_rows(name: str) -> List[Dict[str, object]]:
    return _rows(name)


def maybe_denied(variant: str) -> Optional[List[str]]:
    return ["suppliers"] if variant == "restricted" else None
