"""Mock Unity Catalog metadata service.

Exposes the methods used by `back.core.databricks.UCMetadataService`:
get_catalogs, get_schemas, get_tables, get_table_columns. The tree is built
fluently via `.with_catalog(...).with_schema(...).with_table(...)`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Column:
    name: str
    type: str
    comment: str = ""


@dataclass
class _Table:
    name: str
    columns: list[_Column] = field(default_factory=list)
    comment: str = ""


@dataclass
class _Schema:
    name: str
    tables: dict[str, _Table] = field(default_factory=dict)


@dataclass
class _Catalog:
    name: str
    schemas: dict[str, _Schema] = field(default_factory=dict)


class MockUCCatalog:
    """Fluent tree builder + read API matching UCMetadataService.

    Example:

        uc = (
            MockUCCatalog()
            .with_catalog("benoit_cayla")
            .with_schema("ontobricks")
            .with_table("customers", [("customer_id", "int"), ("name", "string")])
        )
        assert uc.get_catalogs() == ["benoit_cayla"]
    """

    def __init__(self) -> None:
        self._catalogs: dict[str, _Catalog] = {}
        self._cursor_catalog: str | None = None
        self._cursor_schema: str | None = None

    # --- fluent builder ---

    def with_catalog(self, name: str) -> "MockUCCatalog":
        self._catalogs.setdefault(name, _Catalog(name=name))
        self._cursor_catalog = name
        self._cursor_schema = None
        return self

    def with_schema(self, name: str) -> "MockUCCatalog":
        if self._cursor_catalog is None:
            raise ValueError("call .with_catalog(...) before .with_schema(...)")
        cat = self._catalogs[self._cursor_catalog]
        cat.schemas.setdefault(name, _Schema(name=name))
        self._cursor_schema = name
        return self

    def with_table(
        self,
        name: str,
        columns: list[tuple[str, str]] | list[tuple[str, str, str]],
        comment: str = "",
    ) -> "MockUCCatalog":
        if self._cursor_catalog is None or self._cursor_schema is None:
            raise ValueError("call .with_catalog(...).with_schema(...) before .with_table(...)")
        schema = self._catalogs[self._cursor_catalog].schemas[self._cursor_schema]
        cols = [_Column(name=c[0], type=c[1], comment=(c[2] if len(c) > 2 else "")) for c in columns]
        schema.tables[name] = _Table(name=name, columns=cols, comment=comment)
        return self

    # --- read API matching UCMetadataService ---

    def get_catalogs(self) -> list[str]:
        return list(self._catalogs.keys())

    def get_schemas(self, catalog: str) -> list[str]:
        cat = self._catalogs.get(catalog)
        return list(cat.schemas.keys()) if cat else []

    def get_tables(self, catalog: str, schema: str) -> list[str]:
        cat = self._catalogs.get(catalog)
        if not cat:
            return []
        sch = cat.schemas.get(schema)
        return list(sch.tables.keys()) if sch else []

    def get_table_columns(self, catalog: str, schema: str, table: str) -> list[dict[str, Any]]:
        cat = self._catalogs.get(catalog)
        if not cat:
            return []
        sch = cat.schemas.get(schema)
        if not sch:
            return []
        tab = sch.tables.get(table)
        if not tab:
            return []
        return [{"name": c.name, "type": c.type, "comment": c.comment} for c in tab.columns]
