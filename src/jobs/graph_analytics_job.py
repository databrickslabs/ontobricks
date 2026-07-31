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

#: Pivots sampled for the betweenness / closeness estimates (Brandes-Pich).
#: This is the cost driver of the whole job: the BFS table holds one row per
#: (pivot, reached node), so doubling this doubles the largest intermediate.
#: 0 disables both metrics. Set it to at least the node count to compute them
#: exactly — the estimators below are built so that case reduces to the exact
#: definition, which is how they are tested against NetworkX.
DEFAULT_PIVOTS = 64

#: Cap on BFS levels. A runaway guard, not a budget: the loop below stops the
#: moment the frontier empties, so a shallow graph never pays for the headroom
#: and only a graph that genuinely needs more levels feels this number. It is
#: set generously because falling short is expensive — a truncated search makes
#: betweenness and closeness unavailable for the whole run. Sparse knowledge
#: graphs (average degree near 2, long Customer -> Order -> ... chains) reach
#: well past a dozen levels, so do not tune this down on the assumption that
#: knowledge graphs have small diameters.
DEFAULT_MAX_DEPTH = 32


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

    @property
    def pivots(self) -> str:
        return f"{self.work_prefix}_pivots"

    @property
    def bfs(self) -> str:
        """Accumulated ``(pivot, node, dist, sigma)`` from the multi-source BFS."""
        return f"{self.work_prefix}_bfs"

    @property
    def delta_acc(self) -> str:
        """Accumulated Brandes dependencies ``(pivot, node, val)``."""
        return f"{self.work_prefix}_delta"

    @property
    def betweenness_table(self) -> str:
        return f"{self.work_prefix}_bc"

    @property
    def closeness_table(self) -> str:
        return f"{self.work_prefix}_cl"

    def frontier(self, slot: str) -> str:
        return f"{self.work_prefix}_frontier_{slot}"

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
            self.pivots,
            self.bfs,
            self.delta_acc,
            self.betweenness_table,
            self.closeness_table,
            self.frontier("a"),
            self.frontier("b"),
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

    # -- stage 5: betweenness / closeness via pivot sampling ------------
    #
    # Brandes-Pich: run single-source shortest paths from a sample of *pivots*
    # instead of every node, then rescale by ``n / k``. Exact betweenness is
    # O(V*E), which is not viable at the sizes this job exists for.
    #
    # All pivots are expanded in one BFS (a single ``(pivot, node, dist,
    # sigma)`` table advancing one level per iteration) rather than k separate
    # searches, so the loop runs for the graph's diameter, not k * diameter.
    #
    # The estimators in :meth:`write_output` are written so that pivots = all
    # nodes reduces to the exact NetworkX definition. That is not a
    # coincidence — it is what makes the approximation testable.

    def build_pivots(self, pivot_count: int) -> List[str]:
        """Sample *pivot_count* nodes deterministically.

        Ordering by a hash rather than by the URI avoids picking a
        lexicographically clustered sample, which on a real knowledge graph
        would mean sampling one entity type. Deterministic so a re-run gives
        the same estimate rather than a slightly different number each time.

        When ``pivot_count >= node_count`` this returns every node, and the
        metrics become exact.
        """
        return self._recreate(
            self.pivots,
            f"SELECT n FROM {self.deg}\n"
            # md5 rather than a plain hash: present in both Spark and Postgres,
            # so the same statement is valid wherever this runs.
            f"ORDER BY md5(n), n\n"
            f"LIMIT {max(1, int(pivot_count))}",
        )

    def pivot_count_query(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.pivots}"

    def bfs_init(self) -> List[str]:
        """Seed every pivot at distance 0 with one shortest path to itself."""
        seed = (
            f"SELECT n AS pivot, n AS node, 0 AS dist, CAST(1.0 AS DOUBLE) AS sigma\n"
            f"FROM {self.pivots}"
        )
        return self._recreate(self.frontier("a"), seed) + self._recreate(self.bfs, seed)

    def bfs_iteration(self, depth: int, read_slot: str, write_slot: str) -> List[str]:
        """Expand every pivot's frontier by one hop.

        ``sigma`` (the number of shortest paths) is the sum over the
        predecessors one level back, which is exactly the current frontier —
        that is why the anti-join against the accumulated table must happen
        before the new level is appended.
        """
        src = self.frontier(read_slot)
        dst = self.frontier(write_slot)
        statements = self._recreate(
            dst,
            f"SELECT f.pivot AS pivot, b.v AS node, {int(depth)} AS dist,\n"
            f"       SUM(f.sigma) AS sigma\n"
            f"FROM {src} f\n"
            f"JOIN {self.bi} b ON b.u = f.node\n"
            f"LEFT JOIN {self.bfs} seen\n"
            f"  ON seen.pivot = f.pivot AND seen.node = b.v\n"
            f"WHERE seen.node IS NULL\n"
            f"GROUP BY f.pivot, b.v",
        )
        statements.append(
            f"INSERT INTO {self.bfs} SELECT pivot, node, dist, sigma FROM {dst}"
        )
        return statements

    def frontier_count_query(self, slot: str) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.frontier(slot)}"

    def delta_init(self) -> List[str]:
        """An empty dependency table; an absent row means a dependency of 0."""
        return self._recreate(
            self.delta_acc,
            f"SELECT pivot, node, CAST(0.0 AS DOUBLE) AS val\n"
            f"FROM {self.bfs}\n"
            f"WHERE 1 = 0",
        )

    def delta_iteration(self, depth: int) -> List[str]:
        """Push dependencies from level *depth* back onto level ``depth - 1``.

        Brandes' recurrence: each predecessor ``u`` of ``w`` accumulates
        ``(sigma(u)/sigma(w)) * (1 + delta(w))``. Because a node sits at exactly
        one level, it receives contributions in exactly one of these steps, so
        the accumulator gets one row per ``(pivot, node)`` and can be appended
        to rather than re-aggregated.
        """
        staging = f"{self.delta_acc}_stage"
        statements = self._recreate(
            staging,
            f"SELECT cur.pivot AS pivot, pred.node AS node,\n"
            f"       SUM((pred.sigma / cur.sigma) * (1.0 + COALESCE(d.val, 0.0)))"
            f" AS val\n"
            f"FROM {self.bfs} cur\n"
            f"JOIN {self.bi} b ON b.u = cur.node\n"
            f"JOIN {self.bfs} pred\n"
            f"  ON pred.pivot = cur.pivot AND pred.node = b.v\n"
            f"  AND pred.dist = cur.dist - 1\n"
            f"LEFT JOIN {self.delta_acc} d\n"
            f"  ON d.pivot = cur.pivot AND d.node = cur.node\n"
            f"WHERE cur.dist = {int(depth)}\n"
            f"GROUP BY cur.pivot, pred.node",
        )
        statements.append(
            f"INSERT INTO {self.delta_acc} SELECT pivot, node, val FROM {staging}"
        )
        statements.append(f"DROP TABLE IF EXISTS {staging}")
        return statements

    def build_centrality_rollups(self) -> List[str]:
        """Roll the per-pivot results up into one row per node.

        ``pivot <> node`` on both: Brandes excludes the source from its own
        betweenness, and a node's distance to itself is not part of closeness.
        """
        statements = self._recreate(
            self.betweenness_table,
            f"SELECT node, SUM(val) AS raw\n"
            f"FROM {self.delta_acc}\n"
            f"WHERE pivot <> node\n"
            f"GROUP BY node",
        )
        statements += self._recreate(
            self.closeness_table,
            f"SELECT node,\n"
            f"       COUNT(*) AS reached,\n"
            f"       SUM(dist) AS dist_sum\n"
            f"FROM {self.bfs}\n"
            f"WHERE pivot <> node\n"
            f"GROUP BY node",
        )
        return statements

    def max_depth_query(self) -> str:
        return f"SELECT MAX(dist) AS d FROM {self.bfs}"

    # -- stage 6: output ------------------------------------------------
    def node_type_select(self) -> str:
        """One ``rdf:type`` per node.

        ``MIN`` rather than an arbitrary pick: a node typed both ``Person`` and
        ``Agent`` must land in the same profile on every run, or the per-type
        rollups move between runs for no reason.
        """
        return (
            f"SELECT subject AS n, MIN(object) AS type_uri\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''\n"
            f"GROUP BY subject"
        )

    def node_label_select(self) -> str:
        """One ``rdfs:label`` per node, chosen the same way as the type."""
        return (
            f"SELECT subject AS n, MIN(object) AS label\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDFS_LABEL)}' AND object <> ''\n"
            f"GROUP BY subject"
        )

    def write_output(
        self,
        pagerank_slot: str,
        components_slot: str,
        node_count: int,
        *,
        pivot_count: int = 0,
    ) -> List[str]:
        """Write one row per node with every metric.

        ``degree`` is normalised by ``node_count - 1`` to match
        ``networkx.degree_centrality``; ``degree_raw`` keeps the plain count
        because the per-type rollups need it.

        Betweenness and closeness are only present when pivots were sampled.
        Both estimators reduce to the exact NetworkX definition when the pivot
        set is every node:

        * **betweenness** — Brandes' accumulated dependency, rescaled by
          ``1/((n-1)(n-2))`` for an undirected normalised result and by ``n/k``
          to extrapolate from the sample. At ``k == n`` the second factor is 1.
        * **closeness** — ``reached^2 / (k_eff * dist_sum)``, where ``k_eff``
          excludes the node itself when it is a pivot. At ``k == n`` this is
          ``(r/totsp) * (r/(n-1))``, which is NetworkX's ``wf_improved`` form.
        """
        divisor = float(node_count - 1) if node_count > 1 else 1.0
        pr = self.pagerank_table(pagerank_slot)
        cc = self.components_table(components_slot)

        if pivot_count > 0 and node_count > 2:
            scale = (1.0 / ((node_count - 1) * (node_count - 2))) * (
                float(node_count) / float(pivot_count)
            )
            betweenness = (
                f"  COALESCE(bc.raw, 0.0) * {scale} AS betweenness,\n"
                f"  CASE\n"
                f"    WHEN cl.dist_sum IS NULL OR cl.dist_sum <= 0 THEN 0.0\n"
                f"    ELSE (CAST(cl.reached AS DOUBLE) * cl.reached)\n"
                f"         / ((CAST({pivot_count} AS DOUBLE)"
                f" - CASE WHEN pv.n IS NULL THEN 0 ELSE 1 END) * cl.dist_sum)\n"
                f"  END AS closeness,\n"
            )
            pivot_joins = (
                f"\nLEFT JOIN {self.betweenness_table} bc ON bc.node = d.n"
                f"\nLEFT JOIN {self.closeness_table} cl ON cl.node = d.n"
                f"\nLEFT JOIN {self.pivots} pv ON pv.n = d.n"
            )
        else:
            betweenness = "  0.0 AS betweenness,\n  0.0 AS closeness,\n"
            pivot_joins = ""

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
            f"  END AS clustering,\n"
            f"{betweenness}"
            f"  ty.type_uri AS type_uri,\n"
            f"  lb.label AS label\n"
            f"FROM {self.deg} d\n"
            f"JOIN {pr} p ON p.n = d.n\n"
            f"JOIN {cc} cc ON cc.n = d.n\n"
            f"LEFT JOIN {self.triangle_counts} t ON t.n = d.n\n"
            f"LEFT JOIN ({self.node_type_select()}) ty ON ty.n = d.n\n"
            f"LEFT JOIN ({self.node_label_select()}) lb ON lb.n = d.n"
            + pivot_joins,
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
            f"{int(stats.get('pivot_count', 0) or 0)} AS pivot_count",
            # False means the BFS hit the depth cap, so the betweenness and
            # closeness estimates are missing the far tail of the distance
            # distribution. The app downgrades them to "unavailable" rather
            # than publishing a number it cannot stand behind.
            f"{'true' if stats.get('bfs_complete', True) else 'false'}"
            f" AS bfs_complete",
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
    pivots: int = DEFAULT_PIVOTS,
    max_depth: int = DEFAULT_MAX_DEPTH,
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
            "pivot_count": 0,
            "bfs_complete": True,
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

    # -- Betweenness / closeness: BFS from sampled pivots, then Brandes ---
    pivot_count = 0
    bfs_complete = True
    if pivots > 0:
        pivot_count, bfs_complete = _run_pivot_centrality(
            execute, scalar, builder, pivots=pivots, max_depth=max_depth
        )

    for stmt in builder.write_output(
        pr_slot, cc_slot, node_count, pivot_count=pivot_count
    ):
        execute(stmt)

    stats: Dict[str, object] = {
        "node_count": node_count,
        "edge_count": edge_count,
        "component_count": component_count,
        "components_converged": converged,
        "pagerank_iterations": max(0, pagerank_iterations),
        "component_iterations": cc_iterations,
        "pivot_count": pivot_count,
        "bfs_complete": bfs_complete,
        "source_table": builder.source_table,
    }
    for stmt in builder.write_summary(stats):
        execute(stmt)

    if cleanup:
        for stmt in builder.drop_work_tables():
            execute(stmt)

    return stats


def _run_pivot_centrality(
    execute,
    scalar,
    builder: GraphAnalyticsSQL,
    *,
    pivots: int,
    max_depth: int,
) -> tuple:
    """Run the pivot BFS and Brandes accumulation.

    Returns ``(pivot_count, bfs_complete)``. ``bfs_complete`` is False when the
    search hit *max_depth* with a non-empty frontier, which means the distance
    sums are truncated and both estimates are biased.
    """
    for stmt in builder.build_pivots(pivots):
        execute(stmt)
    pivot_count = int(scalar(builder.pivot_count_query()) or 0)
    if pivot_count == 0:
        return 0, True
    logger.info("betweenness/closeness: %d pivots", pivot_count)

    for stmt in builder.bfs_init():
        execute(stmt)

    slot = "a"
    depth = 0
    bfs_complete = True
    for depth in range(1, max(1, max_depth) + 1):
        nxt = "b" if slot == "a" else "a"
        for stmt in builder.bfs_iteration(depth, slot, nxt):
            execute(stmt)
        slot = nxt
        reached = int(scalar(builder.frontier_count_query(slot)) or 0)
        logger.info("bfs level %d: %d (pivot, node) pairs", depth, reached)
        if reached == 0:
            depth -= 1
            break
    else:
        # The loop ran to the cap without the frontier emptying.
        remaining = int(scalar(builder.frontier_count_query(slot)) or 0)
        bfs_complete = remaining == 0
        if not bfs_complete:
            logger.warning(
                "BFS hit the depth cap of %d with %d (pivot, node) pairs still "
                "unexplored — betweenness and closeness will be reported as "
                "unavailable rather than as truncated estimates. Raise "
                "ONTOBRICKS_ANALYTICS_JOB_MAX_DEPTH to let the search finish",
                max_depth,
                remaining,
            )

    max_dist = int(scalar(builder.max_depth_query()) or 0)

    # Brandes accumulates backwards, so the deepest level is processed first
    # and each level's dependency is complete before its predecessors read it.
    for stmt in builder.delta_init():
        execute(stmt)
    for level in range(max_dist, 0, -1):
        for stmt in builder.delta_iteration(level):
            execute(stmt)
        logger.info("dependency accumulation: level %d done", level)

    for stmt in builder.build_centrality_rollups():
        execute(stmt)

    return pivot_count, bfs_complete


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
    parser.add_argument(
        "--pivots",
        type=int,
        default=DEFAULT_PIVOTS,
        help=(
            "Pivots sampled for the betweenness/closeness estimates. 0 skips "
            "both; >= node count computes them exactly. Cost driver of the job."
        ),
    )
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
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
        pivots=args.pivots,
        max_depth=args.max_depth,
        cleanup=not args.keep_work_tables,
    )

    # Printed so the run output carries the summary even if the caller only
    # reads the driver log rather than the summary table.
    print("ONTOBRICKS_GRAPH_ANALYTICS_SUMMARY " + json.dumps(stats, default=str))
    logger.info("wrote %s (%s nodes)", args.output_table, stats["node_count"])
    return 0


def run_cli(argv: Optional[List[str]] = None) -> None:
    """Run :func:`main`, raising ``SystemExit`` only when it reports a failure.

    Databricks serverless executes this file inside an IPython shell, which
    reports *any* raised ``SystemExit`` — code 0 included — as an uncaught
    exception and fails the run. Returning normally on success keeps a
    completed analysis from being recorded as ``RUN_EXECUTION_ERROR``.
    """
    code = main(argv)
    if code:
        raise SystemExit(code)


if __name__ == "__main__":
    run_cli()
