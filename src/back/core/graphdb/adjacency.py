"""Shared adjacency / typed-neighbor SQL for Lakehouse (Spark) and Lakebase (Postgres)."""

from __future__ import annotations

from typing import Callable, Literal

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

SqlFlavor = Literal["spark", "postgres"]
Escape = Callable[[str], str]

__all__ = [
    "Escape",
    "SqlFlavor",
    "expand_and_fetch_sql",
    "expand_entity_neighbors_sql",
    "seeded_bfs_sql",
    "typed_in_select",
    "typed_out_select",
]


def typed_out_select(spo: str) -> str:
    return (
        f"SELECT DISTINCT t.subject AS src, t.predicate, t.object AS dst "
        f"FROM {spo} t "
        f"INNER JOIN {spo} typed "
        f"ON typed.subject = t.object AND typed.predicate = '{RDF_TYPE}' "
        f"WHERE t.object LIKE 'http%' "
        f"AND t.predicate != '{RDF_TYPE}' "
        f"AND t.predicate != '{RDFS_LABEL}'"
    )


def typed_in_select(spo: str) -> str:
    return (
        f"SELECT DISTINCT t.object AS dst, t.predicate, t.subject AS src "
        f"FROM {spo} t "
        f"INNER JOIN {spo} typed "
        f"ON typed.subject = t.subject AND typed.predicate = '{RDF_TYPE}' "
        f"WHERE t.object LIKE 'http%' "
        f"AND t.predicate != '{RDF_TYPE}' "
        f"AND t.predicate != '{RDFS_LABEL}'"
    )


def expand_entity_neighbors_sql(
    adj_out: str, adj_in: str, uris: list[str], escape: Escape
) -> str:
    if not uris:
        raise ValueError("At least one URI is required")
    in_clause = ", ".join(f"'{escape(u)}'" for u in uris)
    return (
        f"SELECT DISTINCT entity FROM ("
        f"SELECT dst AS entity FROM {adj_out} WHERE src IN ({in_clause}) "
        f"UNION ALL "
        f"SELECT src AS entity FROM {adj_in} WHERE dst IN ({in_clause})"
        f") n"
    )


def _anti_join(flavor: SqlFlavor, level: int) -> str:
    visited = f"visited_{level}"
    if flavor == "spark":
        return (
            f"LEFT ANTI JOIN {visited} "
            f"ON {visited}.entity = candidate.entity"
        )
    return (
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM {visited} v WHERE v.entity = candidate.entity)"
    )


def seeded_bfs_sql(
    *,
    flavor: SqlFlavor,
    adj_out: str,
    adj_in: str,
    seed_sql: str,
    depth: int,
) -> str:
    """Leveled BFS over the adjacency tables, seeded from *seed_sql*.

    *seed_sql* must be a ``SELECT`` returning exactly one column (typically
    :func:`back.core.graphdb.entity_search.entity_search_seed_sql`'s output)
    — the CTE column list below renames it to ``entity`` positionally, the
    same trick :func:`expand_and_fetch_sql` uses for its ``VALUES`` seed.

    Mirrors :func:`expand_and_fetch_sql`'s per-level CTE/anti-join shape but
    returns every discovered ``(entity, min_lvl)`` pair instead of joining a
    payload relation — the contract
    :meth:`~back.core.graphdb.GraphDBBackend.GraphDBBackend.bfs_traversal`
    callers expect. Unbounded by design: MCP find has no entity/triple caps
    today (only output-triple pagination happens after this query), and this
    function must not introduce a new limit that changes result sets.
    """
    depth = max(0, int(depth))
    ctes = [f"level_0(entity) AS ({seed_sql})"]
    for level in range(1, depth + 1):
        previous = f"level_{level - 1}"
        visited_union = " UNION ALL ".join(
            f"SELECT entity FROM level_{prior}" for prior in range(level)
        )
        anti = _anti_join(flavor, level)
        ctes.extend(
            [
                f"visited_{level} AS ({visited_union})",
                (
                    f"level_{level}_candidates AS ("
                    f"SELECT t.dst AS entity FROM {adj_out} t "
                    f"JOIN {previous} frontier ON t.src = frontier.entity "
                    f"UNION ALL "
                    f"SELECT t.src AS entity FROM {adj_in} t "
                    f"JOIN {previous} frontier ON t.dst = frontier.entity)"
                ),
                (
                    f"level_{level} AS ("
                    f"SELECT DISTINCT candidate.entity "
                    f"FROM level_{level}_candidates candidate "
                    f"{anti})"
                ),
            ]
        )
    levels = " UNION ALL ".join(
        f"SELECT entity, {level} AS lvl FROM level_{level}"
        for level in range(depth + 1)
    )
    return (
        "WITH "
        + ", ".join(ctes)
        + " "
        + f"SELECT entity, MIN(lvl) AS min_lvl FROM ({levels}) all_levels "
        + "GROUP BY entity"
    )


def expand_and_fetch_sql(
    *,
    flavor: SqlFlavor,
    adj_out: str,
    adj_in: str,
    spo: str,
    props: str | None = None,
    selected_uris: list[str],
    depth: int,
    max_entities: int,
    max_triples: int,
    escape: Escape,
) -> str:
    if not selected_uris:
        raise ValueError("At least one selected URI is required")
    depth = max(0, int(depth))
    max_entities = max(1, int(max_entities))
    max_triples = max(1, int(max_triples))
    payload_relation = props or spo
    if depth == 0:
        seeds = list(dict.fromkeys(selected_uris))[:max_entities]
        in_clause = ", ".join(f"'{escape(uri)}'" for uri in seeds)
        return (
            f"SELECT subject, predicate, object, "
            f"{len(seeds)} AS _ob_expanded_count "
            f"FROM {payload_relation} "
            f"WHERE subject IN ({in_clause}) "
            f"LIMIT {max_triples + 1}"
        )

    seed_values = ", ".join(
        f"('{escape(uri)}')" for uri in dict.fromkeys(selected_uris)
    )
    ctes = [f"level_0(entity) AS (VALUES {seed_values})"]
    for level in range(1, depth + 1):
        previous = f"level_{level - 1}"
        visited_union = " UNION ALL ".join(
            f"SELECT entity FROM level_{prior}" for prior in range(level)
        )
        anti = _anti_join(flavor, level)
        ctes.extend(
            [
                f"visited_{level} AS ({visited_union})",
                (
                    f"level_{level}_candidates AS ("
                    f"SELECT t.dst AS entity FROM {adj_out} t "
                    f"JOIN {previous} frontier ON t.src = frontier.entity "
                    f"UNION ALL "
                    f"SELECT t.src AS entity FROM {adj_in} t "
                    f"JOIN {previous} frontier ON t.dst = frontier.entity)"
                ),
                (
                    f"level_{level} AS ("
                    f"SELECT DISTINCT candidate.entity "
                    f"FROM level_{level}_candidates candidate "
                    f"{anti} "
                    f"LIMIT {max_entities})"
                ),
            ]
        )
    levels = " UNION ALL ".join(
        f"SELECT entity FROM level_{level}" for level in range(depth + 1)
    )
    ctes.extend(
        [
            (
                "entity_probe AS ("
                f"SELECT DISTINCT entity FROM ({levels}) discovered "
                f"LIMIT {max_entities + 1})"
            ),
            (
                "entities AS ("
                f"SELECT entity FROM entity_probe LIMIT {max_entities})"
            ),
            (
                "entity_stats AS ("
                "SELECT COUNT(*) AS _ob_expanded_count FROM entity_probe)"
            ),
        ]
    )
    hint = "/*+ BROADCAST(entities) */ " if flavor == "spark" else ""
    return (
        "WITH "
        + ", ".join(ctes)
        + " "
        + f"SELECT {hint}triples.subject, triples.predicate, triples.object, "
        + "stats._ob_expanded_count "
        + f"FROM {payload_relation} triples "
        + "JOIN entities ON entities.entity = triples.subject "
        + "CROSS JOIN entity_stats stats "
        + f"LIMIT {max_triples + 1}"
    )
