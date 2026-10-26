"""DDL helpers for Lakebase adjacency tables."""

from __future__ import annotations

import hashlib
from typing import Any

from back.core.graphdb.adjacency import typed_in_select, typed_out_select
from back.core.graphdb.entity_search import entity_search_select
from back.core.graphdb.props import props_select
from back.core.graphdb.lakebase._companion_ddl import view_phy
from back.core.logging import get_logger

logger = get_logger(__name__)

_PG_IDENTIFIER_MAX_BYTES = 63
_HASH_HEX_LEN = 10


def _bounded_identifier(base: str, suffix: str) -> str:
    candidate = f"{base}{suffix}"
    if len(candidate.encode("utf-8")) <= _PG_IDENTIFIER_MAX_BYTES:
        return candidate
    digest = hashlib.sha1(candidate.encode("utf-8")).hexdigest()[:_HASH_HEX_LEN]
    reserved = len(f"_{digest}{suffix}".encode("utf-8"))
    prefix_budget = max(1, _PG_IDENTIFIER_MAX_BYTES - reserved)
    trimmed = base.encode("utf-8")[:prefix_budget].decode("utf-8", errors="ignore")
    return f"{trimmed}_{digest}{suffix}"


def adj_out_phy(graph_name: str) -> str:
    return _bounded_identifier(view_phy(graph_name), "_adj_out")


def adj_in_phy(graph_name: str) -> str:
    return _bounded_identifier(view_phy(graph_name), "_adj_in")


def entity_search_phy(graph_name: str) -> str:
    return _bounded_identifier(view_phy(graph_name), "_entity_search")


def entity_search_asserted_phy(graph_name: str) -> str:
    return _bounded_identifier(_asserted_stem_phy(graph_name), "_entity_search_asserted")


def _asserted_stem_phy(graph_name: str) -> str:
    phy = view_phy(graph_name)
    for suffix in ("_sync", "__app"):
        if phy.endswith(suffix):
            return phy[: -len(suffix)]
    return phy


def props_phy(graph_name: str) -> str:
    return _bounded_identifier(view_phy(graph_name), "_props")


def adjacency_index_name(table_name: str, key: str) -> str:
    suffix = f"_{key}_idx"
    return _bounded_identifier(table_name, suffix)


def ensure_adjacency_tables(cur: Any, adj_out: str, adj_in: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {adj_out} (
            src TEXT NOT NULL,
            predicate TEXT NOT NULL,
            dst TEXT NOT NULL,
            PRIMARY KEY (src, predicate, dst)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(adj_out, 'src')} "
        f"ON {adj_out} (src, predicate)"
    )

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {adj_in} (
            dst TEXT NOT NULL,
            predicate TEXT NOT NULL,
            src TEXT NOT NULL,
            PRIMARY KEY (dst, predicate, src)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(adj_in, 'dst')} "
        f"ON {adj_in} (dst, predicate)"
    )


def ensure_entity_search_table(cur: Any, search: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {search} (
            uri TEXT NOT NULL PRIMARY KEY,
            type_uri TEXT NOT NULL,
            label TEXT NOT NULL,
            uri_lc TEXT NOT NULL,
            label_lc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(search, 'type')} "
        f"ON {search} (type_uri)"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(search, 'label')} "
        f"ON {search} (label_lc text_pattern_ops)"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(search, 'uri')} "
        f"ON {search} (uri_lc text_pattern_ops)"
    )
    _ensure_entity_search_trgm_indexes(cur, search)


def _ensure_entity_search_trgm_indexes(cur: Any, search: str) -> None:
    try:
        cur.execute(_PG_TRGM_ENSURE_REACHABLE)
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
        if cur.fetchone() is None:
            logger.info("pg_trgm is unavailable; skipping GIN contains indexes")
            return
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(search, 'ltrgm')} "
            f"ON {search} USING gin (label_lc gin_trgm_ops)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(search, 'utrgm')} "
            f"ON {search} USING gin (uri_lc gin_trgm_ops)"
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Skipping pg_trgm GIN indexes for %s: %s", search, exc
        )


_PG_TRGM_ENSURE_REACHABLE = """
DO $$
DECLARE ext_schema text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        BEGIN
            EXECUTE 'CREATE EXTENSION pg_trgm WITH SCHEMA public';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE 'CREATE EXTENSION pg_trgm';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END;
    END IF;

    SELECT n.nspname INTO ext_schema
    FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'pg_trgm';

    IF ext_schema IS NOT NULL
       AND NOT (ext_schema = ANY (current_schemas(true))) THEN
        BEGIN
            EXECUTE 'ALTER EXTENSION pg_trgm SET SCHEMA public';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE format(
                    'ALTER EXTENSION pg_trgm SET SCHEMA %I', current_schema()
                );
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END;
    END IF;
END $$
"""


def ensure_props_table(cur: Any, props: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {props} (
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {adjacency_index_name(props, 'subject')} "
        f"ON {props} (subject)"
    )


def rebuild_adjacency_data(cur: Any, union_view: str, adj_out: str, adj_in: str) -> None:
    cur.execute(f"TRUNCATE {adj_out}")
    cur.execute(
        f"INSERT INTO {adj_out} (src, predicate, dst) "
        f"{typed_out_select(union_view)}"
    )
    cur.execute(f"TRUNCATE {adj_in}")
    cur.execute(
        f"INSERT INTO {adj_in} (dst, predicate, src) "
        f"{typed_in_select(union_view)}"
    )


def rebuild_entity_search_data(cur: Any, union_view: str, search: str) -> None:
    cur.execute(f"TRUNCATE {search}")
    cur.execute(
        f"INSERT INTO {search} (uri, type_uri, label, uri_lc, label_lc) "
        f"{entity_search_select(union_view)}"
    )


def rebuild_props_data(cur: Any, union_view: str, props: str) -> None:
    cur.execute(f"TRUNCATE {props}")
    cur.execute(
        f"INSERT INTO {props} (subject, predicate, object) "
        f"{props_select(union_view)}"
    )


def analyze_adjacency_tables(cur: Any, adj_out: str, adj_in: str) -> None:
    cur.execute(f"ANALYZE {adj_out}")
    cur.execute(f"ANALYZE {adj_in}")


def analyze_entity_search_table(cur: Any, search: str) -> None:
    cur.execute(f"ANALYZE {search}")


def analyze_props_table(cur: Any, props: str) -> None:
    cur.execute(f"ANALYZE {props}")
