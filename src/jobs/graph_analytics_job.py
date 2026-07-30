"""Serverless Lakeflow job computing the iterative graph metrics at scale.

The in-app SQL pushdown path
(:mod:`back.core.graph_analysis.PushdownMetrics`) lifts the triple cap for
everything that is a plain aggregation, but PageRank, connected components and
the clustering coefficient need repeated passes over the graph. Those run here,
as a Databricks job, and land per-node scores in a Delta table the app reads
back with a top-N query.

**Why generated SQL rather than the DataFrame API.** Every algorithm below is
emitted as SQL strings and executed with ``spark.sql``. That keeps one
implementation testable: ``tests/units/core/test_graph_analytics_job_sql.py``
runs the exact same statements against SQLite and checks the results against a
NetworkX oracle. A DataFrame implementation could not be verified without a
live Spark cluster. The generated SQL deliberately sticks to portable
constructs — ``DROP TABLE IF EXISTS`` plus ``CREATE TABLE AS`` instead of
``CREATE OR REPLACE TABLE``, and no engine-specific functions beyond ``least``
/ ``greatest``.

Materialising each iteration into a real table (rather than a temp view) is
also what keeps Spark's query plan from growing without bound across
iterations: the write truncates the lineage. The two iterative algorithms
alternate between an ``_a`` and a ``_b`` table so a step never reads and writes
the same relation.

This module imports ``pyspark`` lazily inside :func:`main` so the SQL builders
stay importable in a plain Python test environment.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("ontobricks.jobs.graph_analytics")

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

#: Predicates that never form an entity-entity edge. Mirrors
#: ``back.core.graph_analysis.GraphBuilder._DEFAULT_EXCLUDED_PREDICATES``;
#: duplicated because this file must run standalone on the Spark driver, and
#: the app passes its own list through ``--exclude-predicates`` anyway.
DEFAULT_EXCLUDED_PREDICATES = (
    RDF_TYPE,
    RDFS_LABEL,
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://www.w3.org/2000/01/rdf-schema#seeAlso",
)

DEFAULT_DAMPING = 0.85

#: Power iteration converges at roughly ``damping^k``, so 20 rounds pins
#: absolute scores only to ~4e-2. That is deliberate: the app charts a top-N
#: *ordering*, which is stable well before the values are. Knowledge graphs are
#: often close to bipartite (Customer -> Order -> Product), and those sit at the
#: slow end of that rate, so raise this if you need precise scores rather than
#: a ranking.
DEFAULT_PAGERANK_ITERATIONS = 20

DEFAULT_COMPONENT_ITERATIONS = 50


#: Up to three dot-separated plain identifiers (``catalog.schema.table``).
_IDENTIFIER_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
)


def sql_escape(value: str) -> str:
    """Escape single quotes for a SQL string literal."""
    return (value or "").replace("'", "''")


def validate_identifier(name: str, label: str) -> str:
    """Reject anything that is not a plain (optionally qualified) identifier.

    Table names are interpolated into the generated SQL unquoted, so this is
    both a correctness guard (a keyword or an odd character produces SQL that
    fails deep into a long job) and an injection guard.
    """
    if not _IDENTIFIER_RE.match(name or ""):
        raise ValueError(
            f"{label} must be a plain identifier of the form "
            f"catalog.schema.table, got {name!r}"
        )
    return name


def _in_list(values: List[str]) -> str:
    """Render *values* as a SQL ``IN`` list, never empty."""
    return ", ".join(f"'{sql_escape(v)}'" for v in values) or "''"


@dataclass
class GraphAnalyticsSQL:
    """Builds the SQL for every stage of the analysis.

    Each builder returns an ordered list of statements to execute as-is. The
    caller drives the two iterative algorithms so it can stop on convergence.
    """

    source_table: str
    work_prefix: str
    output_table: str
    excluded_predicates: List[str] = field(
        default_factory=lambda: list(DEFAULT_EXCLUDED_PREDICATES)
    )
    damping: float = DEFAULT_DAMPING

    # -- table names ---------------------------------------------------
    @property
    def edges(self) -> str:
        return f"{self.work_prefix}_edges"

    @property
    def bi(self) -> str:
        """Bidirectional edge list: one row per direction."""
        return f"{self.work_prefix}_bi"

    @property
    def deg(self) -> str:
        return f"{self.work_prefix}_deg"

    @property
    def oriented(self) -> str:
        """Edges oriented by (degree, uri) — an acyclic orientation."""
        return f"{self.work_prefix}_oriented"

    @property
    def triangles(self) -> str:
        return f"{self.work_prefix}_triangles"

    @property
    def triangle_counts(self) -> str:
        return f"{self.work_prefix}_tricount"

    @property
    def summary_table(self) -> str:
        return f"{self.output_table}_summary"

    def pagerank_table(self, slot: str) -> str:
        return f"{self.work_prefix}_pr_{slot}"

    def components_table(self, slot: str) -> str:
        return f"{self.work_prefix}_cc_{slot}"

    def work_tables(self) -> List[str]:
        """Every intermediate table, for cleanup."""
        return [
            self.edges,
            self.bi,
            self.deg,
            self.oriented,
            self.triangles,
            self.triangle_counts,
            self.pagerank_table("a"),
            self.pagerank_table("b"),
            self.components_table("a"),
            self.components_table("b"),
        ]

    # -- helpers -------------------------------------------------------
    @staticmethod
    def _recreate(table: str, select_sql: str) -> List[str]:
        """Drop and rebuild *table*.

        ``CREATE OR REPLACE TABLE`` would be shorter but is not portable, and
        the drop/create pair also truncates the Spark lineage between
        iterations.
        """
        return [
            f"DROP TABLE IF EXISTS {table}",
            f"CREATE TABLE {table} AS\n{select_sql}",
        ]

    # -- stage 1: edge list, degrees -----------------------------------
    def build_edges(self) -> List[str]:
        """Build the deduplicated undirected edge list and per-node degree.

        An edge is canonically ordered by ``least`` / ``greatest`` so the two
        directions of the same relationship collapse to one row. Self-loops
        are dropped: they are not meaningful for centrality and would make the
        degree accounting differ from the rest of the pipeline.
        """
        statements = self._recreate(
            self.edges,
            f"SELECT DISTINCT\n"
            f"  least(subject, object) AS src,\n"
            f"  greatest(subject, object) AS dst\n"
            f"FROM {self.source_table}\n"
            f"WHERE subject <> ''\n"
            f"  AND object <> ''\n"
            f"  AND subject <> object\n"
            f"  AND (object LIKE 'http://%' OR object LIKE 'https://%')\n"
            f"  AND predicate NOT IN ({_in_list(self.excluded_predicates)})",
        )
        statements += self._recreate(
            self.bi,
            f"SELECT src AS u, dst AS v FROM {self.edges}\n"
            f"UNION ALL\n"
            f"SELECT dst AS u, src AS v FROM {self.edges}",
        )
        statements += self._recreate(
            self.deg,
            f"SELECT u AS n, COUNT(*) AS d FROM {self.bi} GROUP BY u",
        )
        return statements

    def node_count_query(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.deg}"

    def edge_count_query(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.edges}"

    # -- stage 2: PageRank --------------------------------------------
    def pagerank_init(self, node_count: int) -> List[str]:
        return self._recreate(
            self.pagerank_table("a"),
            f"SELECT n, {1.0 / max(node_count, 1)} AS rank FROM {self.deg}",
        )

    def pagerank_iteration(
        self, read_slot: str, write_slot: str, node_count: int
    ) -> List[str]:
        """One power-iteration step.

        Every node in the edge set has degree >= 1, so there are no dangling
        nodes and the rank mass is exactly conserved: the teleport term
        contributes ``node_count * (1-d)/node_count == 1-d`` and the
        neighbour term contributes ``d``. That is why no re-normalisation
        step is needed, and why the result matches NetworkX, which also
        normalises to sum 1.
        """
        teleport = (1.0 - self.damping) / max(node_count, 1)
        src = self.pagerank_table(read_slot)
        dst = self.pagerank_table(write_slot)
        return self._recreate(
            dst,
            f"SELECT d.n AS n,\n"
            f"       {teleport} + {self.damping} * COALESCE(c.contrib, 0.0) AS rank\n"
            f"FROM {self.deg} d\n"
            f"LEFT JOIN (\n"
            f"  SELECT b.v AS n, SUM(p.rank / dg.d) AS contrib\n"
            f"  FROM {self.bi} b\n"
            f"  JOIN {src} p ON p.n = b.u\n"
            f"  JOIN {self.deg} dg ON dg.n = b.u\n"
            f"  GROUP BY b.v\n"
            f") c ON c.n = d.n",
        )

    # -- stage 3: connected components ---------------------------------
    def components_init(self) -> List[str]:
        """Label every node with itself, then propagate the minimum."""
        return self._recreate(
            self.components_table("a"),
            f"SELECT n, n AS component_id FROM {self.deg}",
        )

    def components_iteration(self, read_slot: str, write_slot: str) -> List[str]:
        """One min-label propagation step.

        Converges in as many rounds as the graph's diameter, which is small
        for knowledge graphs. Chosen over large-star/small-star (which
        converges in O(log n) rounds) because it is dramatically easier to
        verify, and the caller stops as soon as no label changes.
        """
        src = self.components_table(read_slot)
        dst = self.components_table(write_slot)
        return self._recreate(
            dst,
            f"SELECT c.n AS n,\n"
            f"       least(c.component_id, COALESCE(m.min_nb, c.component_id))"
            f" AS component_id\n"
            f"FROM {src} c\n"
            f"LEFT JOIN (\n"
            f"  SELECT b.u AS n, MIN(nb.component_id) AS min_nb\n"
            f"  FROM {self.bi} b\n"
            f"  JOIN {src} nb ON nb.n = b.v\n"
            f"  GROUP BY b.u\n"
            f") m ON m.n = c.n",
        )

    def components_changed_query(self, read_slot: str, write_slot: str) -> str:
        src = self.components_table(read_slot)
        dst = self.components_table(write_slot)
        return (
            f"SELECT COUNT(*) AS changed\n"
            f"FROM {src} a\n"
            f"JOIN {dst} b ON b.n = a.n\n"
            f"WHERE a.component_id <> b.component_id"
        )

    def component_count_query(self, slot: str) -> str:
        return (
            f"SELECT COUNT(DISTINCT component_id) AS n "
            f"FROM {self.components_table(slot)}"
        )

    # -- stage 4: clustering coefficient -------------------------------
    def clustering(self) -> List[str]:
        """Count triangles per node via a degree-oriented edge list.

        Orienting each edge from the lower to the higher ``(degree, uri)``
        yields an acyclic orientation, so every triangle has exactly one
        vertex with two out-edges and is therefore enumerated exactly once.
        Orienting by degree also caps the work done at high-degree hubs,
        which is what keeps this from blowing up on a skewed graph.
        """
        statements = self._recreate(
            self.oriented,
            f"SELECT\n"
            f"  CASE WHEN du.d < dv.d OR (du.d = dv.d AND e.src < e.dst)\n"
            f"       THEN e.src ELSE e.dst END AS u,\n"
            f"  CASE WHEN du.d < dv.d OR (du.d = dv.d AND e.src < e.dst)\n"
            f"       THEN e.dst ELSE e.src END AS v\n"
            f"FROM {self.edges} e\n"
            f"JOIN {self.deg} du ON du.n = e.src\n"
            f"JOIN {self.deg} dv ON dv.n = e.dst",
        )
        statements += self._recreate(
            self.triangles,
            f"SELECT a.u AS x, a.v AS y, b.v AS z\n"
            f"FROM {self.oriented} a\n"
            f"JOIN {self.oriented} b ON b.u = a.v\n"
            f"JOIN {self.edges} c\n"
            f"  ON c.src = least(a.u, b.v) AND c.dst = greatest(a.u, b.v)",
        )
        statements += self._recreate(
            self.triangle_counts,
            f"SELECT n, COUNT(*) AS t FROM (\n"
            f"  SELECT x AS n FROM {self.triangles}\n"
            f"  UNION ALL\n"
            f"  SELECT y AS n FROM {self.triangles}\n"
            f"  UNION ALL\n"
            f"  SELECT z AS n FROM {self.triangles}\n"
            f") q GROUP BY n",
        )
        return statements

    # -- stage 5: output ------------------------------------------------
    def write_output(
        self, pagerank_slot: str, components_slot: str, node_count: int
    ) -> List[str]:
        """Write one row per node with all four metrics.

        ``degree`` is normalised by ``node_count - 1`` to match
        ``networkx.degree_centrality``; ``degree_raw`` keeps the plain count
        because the per-type rollups need it.
        """
        divisor = float(node_count - 1) if node_count > 1 else 1.0
        pr = self.pagerank_table(pagerank_slot)
        cc = self.components_table(components_slot)
        return self._recreate(
            self.output_table,
            f"SELECT\n"
            f"  d.n AS node_uri,\n"
            f"  d.d AS degree_raw,\n"
            f"  CAST(d.d AS DOUBLE) / {divisor} AS degree,\n"
            f"  p.rank AS pagerank,\n"
            f"  cc.component_id AS component_id,\n"
            f"  CASE WHEN d.d < 2 THEN 0.0\n"
            f"       ELSE 2.0 * COALESCE(t.t, 0) / (CAST(d.d AS DOUBLE) * (d.d - 1))\n"
            f"  END AS clustering\n"
            f"FROM {self.deg} d\n"
            f"JOIN {pr} p ON p.n = d.n\n"
            f"JOIN {cc} cc ON cc.n = d.n\n"
            f"LEFT JOIN {self.triangle_counts} t ON t.n = d.n",
        )

    def write_summary(self, stats: Dict[str, object]) -> List[str]:
        """Write the one-row run summary the app reads for the aggregates."""
        columns = [
            f"{int(stats['node_count'])} AS node_count",
            f"{int(stats['edge_count'])} AS edge_count",
            f"{int(stats['component_count'])} AS component_count",
            f"{'true' if stats['components_converged'] else 'false'}"
            f" AS components_converged",
            f"{int(stats['pagerank_iterations'])} AS pagerank_iterations",
            f"{int(stats['component_iterations'])} AS component_iterations",
            f"'{sql_escape(str(stats['source_table']))}' AS source_table",
        ]
        return self._recreate(
            self.summary_table, "SELECT " + ",\n       ".join(columns)
        )

    def drop_work_tables(self) -> List[str]:
        return [f"DROP TABLE IF EXISTS {t}" for t in self.work_tables()]


def run_analysis(
    execute,
    scalar,
    builder: GraphAnalyticsSQL,
    *,
    pagerank_iterations: int = DEFAULT_PAGERANK_ITERATIONS,
    component_iterations: int = DEFAULT_COMPONENT_ITERATIONS,
    cleanup: bool = True,
) -> Dict[str, object]:
    """Drive the full pipeline.

    Engine-agnostic on purpose: *execute* runs a statement and *scalar* runs a
    query and returns its first column of the first row. The Spark driver and
    the SQLite test harness supply their own pair, so both exercise the same
    orchestration and the same SQL.

    Returns the run summary.
    """
    for stmt in builder.build_edges():
        execute(stmt)

    node_count = int(scalar(builder.node_count_query()) or 0)
    edge_count = int(scalar(builder.edge_count_query()) or 0)
    logger.info("graph: %d nodes, %d edges", node_count, edge_count)

    if node_count == 0:
        stats = {
            "node_count": 0,
            "edge_count": 0,
            "component_count": 0,
            "components_converged": True,
            "pagerank_iterations": 0,
            "component_iterations": 0,
            "source_table": builder.source_table,
        }
        for stmt in builder.write_summary(stats):
            execute(stmt)
        return stats

    # -- PageRank: fixed number of power iterations --------------------
    for stmt in builder.pagerank_init(node_count):
        execute(stmt)
    pr_slot = "a"
    for i in range(max(0, pagerank_iterations)):
        nxt = "b" if pr_slot == "a" else "a"
        for stmt in builder.pagerank_iteration(pr_slot, nxt, node_count):
            execute(stmt)
        pr_slot = nxt
        logger.info("pagerank iteration %d/%d", i + 1, pagerank_iterations)

    # -- Components: propagate until nothing changes -------------------
    for stmt in builder.components_init():
        execute(stmt)
    cc_slot = "a"
    cc_iterations = 0
    converged = False
    for _ in range(max(1, component_iterations)):
        nxt = "b" if cc_slot == "a" else "a"
        for stmt in builder.components_iteration(cc_slot, nxt):
            execute(stmt)
        changed = int(scalar(builder.components_changed_query(cc_slot, nxt)) or 0)
        cc_slot = nxt
        cc_iterations += 1
        logger.info("components iteration %d: %d labels changed", cc_iterations, changed)
        if changed == 0:
            converged = True
            break
    if not converged:
        logger.warning(
            "connected components did not converge in %d iterations — the "
            "component count is reported as unconverged",
            cc_iterations,
        )

    for stmt in builder.clustering():
        execute(stmt)

    component_count = int(scalar(builder.component_count_query(cc_slot)) or 0)

    for stmt in builder.write_output(pr_slot, cc_slot, node_count):
        execute(stmt)

    stats: Dict[str, object] = {
        "node_count": node_count,
        "edge_count": edge_count,
        "component_count": component_count,
        "components_converged": converged,
        "pagerank_iterations": max(0, pagerank_iterations),
        "component_iterations": cc_iterations,
        "source_table": builder.source_table,
    }
    for stmt in builder.write_summary(stats):
        execute(stmt)

    if cleanup:
        for stmt in builder.drop_work_tables():
            execute(stmt)

    return stats


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute iterative graph metrics for an OntoBricks knowledge graph"
    )
    parser.add_argument(
        "--source-table",
        required=True,
        help="Fully-qualified triple table or view (catalog.schema.name)",
    )
    parser.add_argument(
        "--output-table",
        required=True,
        help="Fully-qualified Delta table for the per-node result",
    )
    parser.add_argument(
        "--work-prefix",
        default="",
        help="Prefix for intermediate tables (defaults to <output-table>_work)",
    )
    parser.add_argument(
        "--exclude-predicates",
        default="",
        help="Comma-separated predicate URIs that do not form edges",
    )
    parser.add_argument(
        "--pagerank-iterations", type=int, default=DEFAULT_PAGERANK_ITERATIONS
    )
    parser.add_argument(
        "--component-iterations", type=int, default=DEFAULT_COMPONENT_ITERATIONS
    )
    parser.add_argument("--damping", type=float, default=DEFAULT_DAMPING)
    parser.add_argument(
        "--keep-work-tables",
        action="store_true",
        help="Leave intermediate tables in place for debugging",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Spark entry point. Imports pyspark lazily so the builders stay testable."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    excluded = [
        p.strip() for p in (args.exclude_predicates or "").split(",") if p.strip()
    ] or list(DEFAULT_EXCLUDED_PREDICATES)

    work_prefix = args.work_prefix or f"{args.output_table}_work"
    validate_identifier(args.source_table, "--source-table")
    validate_identifier(args.output_table, "--output-table")
    validate_identifier(work_prefix, "--work-prefix")

    builder = GraphAnalyticsSQL(
        source_table=args.source_table,
        work_prefix=work_prefix,
        output_table=args.output_table,
        excluded_predicates=excluded,
        damping=args.damping,
    )

    def execute(stmt: str) -> None:
        logger.debug("executing: %s", stmt)
        spark.sql(stmt)

    def scalar(query: str):
        row = spark.sql(query).head()
        return row[0] if row is not None else None

    stats = run_analysis(
        execute,
        scalar,
        builder,
        pagerank_iterations=args.pagerank_iterations,
        component_iterations=args.component_iterations,
        cleanup=not args.keep_work_tables,
    )

    # Printed so the run output carries the summary even if the caller only
    # reads the driver log rather than the summary table.
    print("ONTOBRICKS_GRAPH_ANALYTICS_SUMMARY " + json.dumps(stats, default=str))
    logger.info("wrote %s (%s nodes)", args.output_table, stats["node_count"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
