"""Serverless Lakeflow job computing the iterative graph metrics at scale.

PageRank, connected components, and the clustering coefficient need repeated
passes over the graph. Those run here, as a Databricks job, and land per-node
scores in a Delta table the app reads back with a top-N query.

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
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("ontobricks.jobs.graph_analytics")

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

#: Predicates that never form an entity-entity edge. Kept here because this
#: file must run standalone on the Spark driver.
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
    #: Entity types to restrict the analysis to. Empty means the whole graph.
    class_filter: List[str] = field(default_factory=list)
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
    def type_profiles_table(self) -> str:
        return f"{self.output_table}_type_profiles"

    @property
    def type_predicates_table(self) -> str:
        return f"{self.output_table}_type_predicates"

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

    def _class_clause(self, column: str) -> str:
        """``AND <column> IN (…)`` when a class filter is set, else empty."""
        if not self.class_filter:
            return ""
        return f"\n  AND {column} IN ({_in_list(self.class_filter)})"

    def typed_nodes_select(self) -> str:
        """Nodes carrying one of ``class_filter``'s types.

        Only called when a filter is set, so it never widens the graph.
        """
        return (
            f"SELECT DISTINCT subject AS n\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}'\n"
            f"  AND object IN ({_in_list(self.class_filter)})"
        )

    # -- stage 1: edge list, degrees -----------------------------------
    def build_edges(self) -> List[str]:
        """Build the deduplicated undirected edge list and per-node degree.

        An edge is canonically ordered by ``least`` / ``greatest`` so the two
        directions of the same relationship collapse to one row. Self-loops
        are dropped: they are not meaningful for centrality and would make the
        degree accounting differ from the rest of the pipeline.
        """
        keep = ""
        if self.class_filter:
            # Both endpoints must survive, which is exactly an induced
            # subgraph — the same graph the entity-type filter produced when
            # the analysis ran in memory.
            keep = (
                f"  AND subject IN ({self.typed_nodes_select()})\n"
                f"  AND object IN ({self.typed_nodes_select()})\n"
            )

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
            f"  AND predicate NOT IN ({_in_list(self.excluded_predicates)})\n"
            f"{keep}",
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

    def total_node_count_query(self) -> str:
        """Every distinct entity URI in the source, connected or not.

        ``node_count`` counts only nodes that survived edge construction, so a
        domain of fully isolated instances would otherwise report zero nodes and
        look broken rather than flat.

        Read straight from the source rather than from the degree table: this is
        a population total, and a total that moved when the user narrowed the
        run would be indefensible. Neither per-run filter may reach it — not the
        class filter, and not ``excluded_predicates`` either, which the app sets
        per request.

        An entity is anything appearing as a subject, or as a URI object of a
        predicate that can carry one. The predicate test uses the fixed
        metadata list rather than ``self.excluded_predicates`` for exactly that
        reason, and applies to the object side only: a subject is an entity
        whatever it carries, while ``rdf:type``'s object is a class and
        ``rdfs:label``'s is a literal.
        """
        return (
            f"SELECT COUNT(*) AS n FROM (\n"
            f"  SELECT DISTINCT subject AS n FROM {self.source_table}\n"
            f"  WHERE subject <> ''\n"
            f"  UNION\n"
            f"  SELECT DISTINCT object AS n FROM {self.source_table}\n"
            f"  WHERE object <> ''\n"
            f"    AND (object LIKE 'http://%' OR object LIKE 'https://%')\n"
            f"    AND predicate NOT IN "
            f"({_in_list(list(DEFAULT_EXCLUDED_PREDICATES))})\n"
            f") s"
        )

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
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''{self._class_clause('object')}\n"
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

    def type_population_select(self) -> str:
        """Instance count per type in the scored subgraph, isolated included."""
        return (
            f"SELECT object AS type_uri, COUNT(DISTINCT subject) AS instance_count\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''{self._class_clause('object')}\n"
            f"GROUP BY object"
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

    def write_empty_output(self) -> List[str]:
        """An empty per-node output, for a source with no entity-entity edges.

        The read-back then never has to distinguish "no edges" from "the job
        did not get that far", and ``type_profiles`` can join unconditionally.
        """
        return self._recreate(
            self.output_table,
            f"SELECT\n"
            f"  d.n AS node_uri,\n"
            f"  d.d AS degree_raw,\n"
            f"  0.0 AS degree,\n"
            f"  0.0 AS pagerank,\n"
            f"  CAST(NULL AS BIGINT) AS component_id,\n"
            f"  0.0 AS clustering,\n"
            f"  0.0 AS betweenness,\n"
            f"  0.0 AS closeness,\n"
            f"  CAST(NULL AS VARCHAR) AS type_uri,\n"
            f"  CAST(NULL AS VARCHAR) AS label\n"
            f"FROM {self.deg} d\n"
            f"WHERE 1 = 0",
        )

    # -- stage 7: per-type rollups --------------------------------------
    def type_profiles(self) -> List[str]:
        """One row per entity type, joining the population to the scored nodes.

        ``instance_count`` comes from the source so isolated instances are
        counted, which is what makes the "flat dataset" heuristic meaningful:
        a type with 5,000 instances and no relationships must be visible.
        ``connected_count`` and the averages come from the output table, so
        they describe exactly the graph that was scored.
        """
        return self._recreate(
            self.type_profiles_table,
            f"SELECT\n"
            f"  pop.type_uri AS type_uri,\n"
            f"  pop.instance_count AS instance_count,\n"
            f"  COALESCE(m.connected_count, 0) AS connected_count,\n"
            f"  COALESCE(m.degree_sum, 0) AS degree_sum,\n"
            f"  COALESCE(m.avg_clustering, 0.0) AS avg_clustering,\n"
            f"  COALESCE(m.avg_betweenness, 0.0) AS avg_betweenness\n"
            f"FROM ({self.type_population_select()}) pop\n"
            f"LEFT JOIN (\n"
            f"  SELECT type_uri,\n"
            f"         COUNT(*) AS connected_count,\n"
            f"         SUM(degree_raw) AS degree_sum,\n"
            f"         AVG(clustering) AS avg_clustering,\n"
            f"         AVG(betweenness) AS avg_betweenness\n"
            f"  FROM {self.output_table}\n"
            f"  WHERE type_uri IS NOT NULL\n"
            f"  GROUP BY type_uri\n"
            f") m ON m.type_uri = pop.type_uri",
        )

    def type_predicates(self) -> List[str]:
        """Distinct ``(type, predicate)`` pairs over entity-entity edges.

        Rows rather than an array column: ``collect_set`` has no SQLite
        equivalent, and the test harness runs this same SQL.

        The predicate filter mirrors ``build_edges`` exactly, so a predicate
        that forms no edge never shows up as a type's relationship.
        """
        return self._recreate(
            self.type_predicates_table,
            f"SELECT DISTINCT ty.type_uri AS type_uri, s.predicate AS predicate\n"
            f"FROM {self.source_table} s\n"
            f"JOIN ({self.node_type_select()}) ty ON ty.n = s.subject\n"
            f"WHERE s.subject <> ''\n"
            f"  AND s.object <> ''\n"
            f"  AND s.subject <> s.object\n"
            f"  AND (s.object LIKE 'http://%' OR s.object LIKE 'https://%')\n"
            f"  AND s.predicate NOT IN ({_in_list(self.excluded_predicates)})",
        )

    def write_summary(self, stats: Dict[str, object]) -> List[str]:
        """Write the one-row run summary the app reads for the aggregates."""
        columns = [
            f"{int(stats['node_count'])} AS node_count",
            f"{int(stats.get('total_node_count', 0) or 0)} AS total_node_count",
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


def _drop_work_tables(execute, builder: GraphAnalyticsSQL) -> None:
    """Drop every intermediate, best-effort and statement by statement.

    Called from a ``finally``, so it must never raise: an exception here would
    replace whatever made the run fail with a misleading "could not drop
    table" and discard the stack the operator actually needs. A single
    undroppable table must also not strand the other sixteen.
    """
    for stmt in builder.drop_work_tables():
        try:
            execute(stmt)
        except Exception:  # noqa: BLE001 — cleanup is never worth failing over
            logger.warning("could not drop work table: %s", stmt, exc_info=True)


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
    """Drive the full pipeline, always dropping the intermediates afterwards.

    Engine-agnostic on purpose: *execute* runs a statement and *scalar* runs a
    query and returns its first column of the first row. The Spark driver and
    the SQLite test harness supply their own pair, so both exercise the same
    orchestration and the same SQL.

    Cleanup lives here rather than at the end of the stages so it also covers
    the paths that do not reach the end: a stage that raises, and the early
    return for a graph with no edges. The seventeen work tables are named
    after the output table, so a stranded set is both a storage cost and the
    first thing an operator finds when looking for the results of the run that
    just failed. ``cleanup=False`` (``--keep-work-tables``) still wins, since
    its whole purpose is to leave the evidence in place after a failure.

    Returns the run summary.
    """
    try:
        return _run_analysis_stages(
            execute,
            scalar,
            builder,
            pagerank_iterations=pagerank_iterations,
            component_iterations=component_iterations,
            pivots=pivots,
            max_depth=max_depth,
        )
    finally:
        if cleanup:
            _drop_work_tables(execute, builder)


def _run_analysis_stages(
    execute,
    scalar,
    builder: GraphAnalyticsSQL,
    *,
    pagerank_iterations: int,
    component_iterations: int,
    pivots: int,
    max_depth: int,
) -> Dict[str, object]:
    """The pipeline itself. Wrapped by :func:`run_analysis`, which cleans up."""
    for stmt in builder.build_edges():
        execute(stmt)

    node_count = int(scalar(builder.node_count_query()) or 0)
    edge_count = int(scalar(builder.edge_count_query()) or 0)
    total_node_count = int(scalar(builder.total_node_count_query()) or 0)
    logger.info("graph: %d nodes, %d edges", node_count, edge_count)

    if node_count == 0:
        stats = {
            "node_count": 0,
            "total_node_count": total_node_count,
            "edge_count": 0,
            "component_count": 0,
            "components_converged": True,
            "pagerank_iterations": 0,
            "component_iterations": 0,
            "pivot_count": 0,
            "bfs_complete": True,
            "source_table": builder.source_table,
        }
        for stmt in builder.write_empty_output():
            execute(stmt)
        for stmt in builder.type_profiles() + builder.type_predicates():
            execute(stmt)
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

    for stmt in builder.type_profiles() + builder.type_predicates():
        execute(stmt)

    stats: Dict[str, object] = {
        "node_count": node_count,
        "total_node_count": total_node_count,
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


# ---------------------------------------------------------------------------
# Community detection (``--task communities``)
# ---------------------------------------------------------------------------

COMMUNITY_ALGORITHM = "label_propagation"

#: Hard cap on label-propagation rounds. Two rounds make one full parity
#: cycle, so this allows 15 complete passes over every node.
DEFAULT_LP_ITERATIONS = 30

#: A round counts as quiet when at most this fraction of nodes changed label.
#: Two quiet rounds in a row (one full parity cycle) stop the loop. The exact
#: fixed point is not worth chasing on a 100M-node graph: the last few
#: thousand flips do not move the partition the UI shows.
DEFAULT_LP_MIN_CHANGE_RATIO = 0.001

#: Bounds of the preview graph the Community page draws.
PREVIEW_COMMUNITIES = 20
PREVIEW_MAX_NODES = 1_500
PREVIEW_MAX_EDGES = 5_000

#: First hex digit of ``md5(node)`` for the half of the nodes updated on even
#: rounds; the other half updates on odd rounds.
_EVEN_HALF = ", ".join(f"'{c}'" for c in "01234567")


@dataclass
class CommunitySQL(GraphAnalyticsSQL):
    """SQL for semi-synchronous label propagation and its outputs.

    Reuses the edge list, the bidirectional table and the degree table of
    :class:`GraphAnalyticsSQL`, so both tasks agree on what an edge is.

    Each round updates only the half of the nodes selected by the parity of
    ``md5(node)``. Fully synchronous propagation flips forever between two
    labellings on bipartite graphs, which is the usual shape of a knowledge
    graph (Customer -> Order -> Product); updating one half at a time breaks
    that two-cycle. A node's own label counts as one vote, and ties go to the
    smallest label, so a re-run on the same data yields the same partition.
    """

    @property
    def sizes_table(self) -> str:
        return f"{self.output_table}_sizes"

    @property
    def preview_nodes_table(self) -> str:
        return f"{self.output_table}_preview_nodes"

    @property
    def preview_edges_table(self) -> str:
        return f"{self.output_table}_preview_edges"

    def lp_table(self, slot: str) -> str:
        return f"{self.work_prefix}_lp_{slot}"

    def work_tables(self) -> List[str]:
        return [
            self.edges,
            self.bi,
            self.deg,
            self.lp_table("a"),
            self.lp_table("b"),
        ]

    def lp_init(self) -> List[str]:
        return self._recreate(
            self.lp_table("a"), f"SELECT n, n AS label FROM {self.deg}"
        )

    def lp_iteration(self, read_slot: str, write_slot: str, parity: int) -> List[str]:
        """One round: the selected half adopts its neighbourhood's majority label."""
        src = self.lp_table(read_slot)
        dst = self.lp_table(write_slot)
        half = "IN" if parity % 2 == 0 else "NOT IN"
        return self._recreate(
            dst,
            f"SELECT cur.n AS n,\n"
            f"       CASE WHEN substr(md5(cur.n), 1, 1) {half} ({_EVEN_HALF})\n"
            f"            THEN best.label ELSE cur.label END AS label\n"
            f"FROM {src} cur\n"
            f"JOIN (\n"
            f"  SELECT n, label FROM (\n"
            f"    SELECT n, label,\n"
            f"           ROW_NUMBER() OVER (PARTITION BY n ORDER BY c DESC, label) AS rk\n"
            f"    FROM (\n"
            f"      SELECT n, label, SUM(c) AS c FROM (\n"
            f"        SELECT b.u AS n, nb.label AS label, 1 AS c\n"
            f"        FROM {self.bi} b\n"
            f"        JOIN {src} nb ON nb.n = b.v\n"
            f"        UNION ALL\n"
            f"        SELECT n, label, 1 AS c FROM {src}\n"
            f"      ) v\n"
            f"      GROUP BY n, label\n"
            f"    ) agg\n"
            f"  ) ranked\n"
            f"  WHERE rk = 1\n"
            f") best ON best.n = cur.n",
        )

    def lp_changed_query(self, read_slot: str, write_slot: str) -> str:
        return (
            f"SELECT COUNT(*) AS changed\n"
            f"FROM {self.lp_table(read_slot)} a\n"
            f"JOIN {self.lp_table(write_slot)} b ON b.n = a.n\n"
            f"WHERE a.label <> b.label"
        )

    def write_sizes(self, slot: str) -> List[str]:
        """One row per community, ranked by size desc then seed URI.

        ``community_rank`` is the cluster id everywhere downstream; ranking
        by size makes cluster 0 the largest, matching the in-memory ordering
        this replaced.
        """
        return self._recreate(
            self.sizes_table,
            f"SELECT s.community_rank AS community_rank,\n"
            f"       s.size AS size,\n"
            f"       s.seed_uri AS seed_uri,\n"
            f"       lb.label AS seed_label\n"
            f"FROM (\n"
            f"  SELECT label AS seed_uri, size,\n"
            f"         ROW_NUMBER() OVER (ORDER BY size DESC, label) - 1 AS community_rank\n"
            f"  FROM (\n"
            f"    SELECT label, COUNT(*) AS size FROM {self.lp_table(slot)} GROUP BY label\n"
            f"  ) c\n"
            f") s\n"
            f"LEFT JOIN ({self.node_label_select()}) lb ON lb.n = s.seed_uri",
        )

    def write_community_output(self, slot: str) -> List[str]:
        return self._recreate(
            self.output_table,
            f"SELECT l.n AS node_uri,\n"
            f"       s.community_rank AS community_rank,\n"
            f"       d.d AS degree_raw,\n"
            f"       ty.type_uri AS type_uri,\n"
            f"       lb.label AS label\n"
            f"FROM {self.lp_table(slot)} l\n"
            f"JOIN {self.sizes_table} s ON s.seed_uri = l.label\n"
            f"JOIN {self.deg} d ON d.n = l.n\n"
            f"LEFT JOIN ({self.node_type_select()}) ty ON ty.n = l.n\n"
            f"LEFT JOIN ({self.node_label_select()}) lb ON lb.n = l.n",
        )

    def community_count_query(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.sizes_table}"

    def intra_edges_query(self) -> str:
        """Edges whose two endpoints share a community (``sum L_c``)."""
        return (
            f"SELECT COUNT(*) AS n\n"
            f"FROM {self.edges} e\n"
            f"JOIN {self.output_table} a ON a.node_uri = e.src\n"
            f"JOIN {self.output_table} b ON b.node_uri = e.dst\n"
            f"WHERE a.community_rank = b.community_rank"
        )

    def degree_square_sum_query(self) -> str:
        """``sum D_c^2`` over communities, in DOUBLE to stay clear of overflow."""
        return (
            f"SELECT SUM(CAST(ds AS DOUBLE) * ds) AS n FROM (\n"
            f"  SELECT SUM(degree_raw) AS ds FROM {self.output_table}\n"
            f"  GROUP BY community_rank\n"
            f") q"
        )

    def write_preview(self) -> List[str]:
        """Highest-degree members of the largest communities, plus their edges."""
        per_community = -(-PREVIEW_MAX_NODES // PREVIEW_COMMUNITIES)
        statements = self._recreate(
            self.preview_nodes_table,
            f"SELECT node_uri, community_rank, label, type_uri FROM (\n"
            f"  SELECT node_uri, community_rank, label, type_uri,\n"
            f"         ROW_NUMBER() OVER (\n"
            f"           PARTITION BY community_rank ORDER BY degree_raw DESC, node_uri\n"
            f"         ) AS rk\n"
            f"  FROM {self.output_table}\n"
            f"  WHERE community_rank < {PREVIEW_COMMUNITIES}\n"
            f") q\n"
            f"WHERE rk <= {per_community}",
        )
        statements += self._recreate(
            self.preview_edges_table,
            f"SELECT e.src AS src, e.dst AS dst\n"
            f"FROM {self.edges} e\n"
            f"JOIN {self.preview_nodes_table} a ON a.node_uri = e.src\n"
            f"JOIN {self.preview_nodes_table} b ON b.node_uri = e.dst\n"
            f"ORDER BY e.src, e.dst\n"
            f"LIMIT {PREVIEW_MAX_EDGES}",
        )
        return statements

    def write_community_summary(self, stats: Dict[str, object]) -> List[str]:
        columns = [
            f"{int(stats['node_count'])} AS node_count",
            f"{int(stats['edge_count'])} AS edge_count",
            f"{int(stats['community_count'])} AS community_count",
            f"{float(stats['modularity'])} AS modularity",
            f"{int(stats['iterations'])} AS iterations",
            f"{'true' if stats['converged'] else 'false'} AS converged",
            f"'{sql_escape(str(stats['algorithm']))}' AS algorithm",
            f"'{sql_escape(','.join(self.class_filter))}' AS class_filter",
            f"'{sql_escape(str(stats['source_table']))}' AS source_table",
            f"'{sql_escape(str(stats.get('computed_at', '') or ''))}' AS computed_at",
            f"{int(stats.get('elapsed_ms', 0) or 0)} AS elapsed_ms",
        ]
        return self._recreate(
            self.summary_table, "SELECT " + ",\n       ".join(columns)
        )


def modularity_from_sums(intra_edges: float, degree_square_sum: float, edge_count: int) -> float:
    """Newman modularity ``sum(L_c)/m - sum(D_c^2)/(4 m^2)`` (resolution 1)."""
    if edge_count <= 0:
        return 0.0
    m = float(edge_count)
    return float(intra_edges) / m - float(degree_square_sum) / (4.0 * m * m)


# ---------------------------------------------------------------------------
# Pattern detection (``--task patterns``)
# ---------------------------------------------------------------------------

PATTERN_ALGORITHM = "egocentric_signature"

DEFAULT_PATTERN_HOPS = 3
DEFAULT_PATTERN_BINS = 8
DEFAULT_PATTERN_MIN_SUPPORT = 3
DEFAULT_PATTERN_FRONTIER = 50_000
DEFAULT_PATTERN_REFINE_TOP = 0
CATEGORICAL_TOP_N = 20

PREVIEW_PATTERNS = 20
# Preview caps reuse the community constants when available; keep local aliases
# so this block stays readable next to the community preview helpers.
PATTERN_PREVIEW_MAX_NODES = PREVIEW_MAX_NODES
PATTERN_PREVIEW_MAX_EDGES = PREVIEW_MAX_EDGES


def _portable_is_numeric_sql(column: str) -> str:
    """True when *column* is a plain decimal: optional leading ``-``, one ``.``.

    Spark serverless runs in ANSI mode, where ``CAST('2026-11-13' AS DOUBLE)``
    or ``CAST('1.2.3' AS DOUBLE)`` raises ``CAST_INVALID_INPUT`` instead of
    returning NULL. Matching only the *alphabet* (digits, ``.``, ``-``) is not
    enough, so this also requires at most one ``.``, a ``-`` only in first
    position, and at least one digit.

    Built from ``replace`` / ``substr`` / ``length`` so the same SQL runs on
    Spark and SQLite (no ``RLIKE`` / ``GLOB``).
    """
    stripped = column
    for ch in "0123456789":
        stripped = f"replace({stripped}, '{ch}', '')"
    stripped = f"replace(replace({stripped}, '.', ''), '-', '')"
    tail = f"substr({column}, 2)"
    return (
        f"({column} <> '' AND {column} <> '-' AND {column} <> '.' "
        f"AND {column} <> '-.' AND {stripped} = '' "
        f"AND length({column}) - length(replace({column}, '.', '')) <= 1 "
        f"AND replace({tail}, '-', '') = {tail})"
    )


@dataclass
class PatternSQL(GraphAnalyticsSQL):
    """SQL for egocentric neighbourhood signatures and optional motif refine.

    ``class_filter`` selects **anchor** nodes only; hop expansion walks the
    full directed edge set so neighbours of other types still appear in the
    signature. ``via`` is the full predicate URI (the UI local-names it).
    """

    hops: int = DEFAULT_PATTERN_HOPS
    bins: int = DEFAULT_PATTERN_BINS
    min_support: int = DEFAULT_PATTERN_MIN_SUPPORT
    frontier_cap: int = DEFAULT_PATTERN_FRONTIER
    refine_top: int = DEFAULT_PATTERN_REFINE_TOP
    categorical_top_n: int = CATEGORICAL_TOP_N

    @property
    def directed(self) -> str:
        return f"{self.work_prefix}_dir"

    @property
    def literals(self) -> str:
        return f"{self.work_prefix}_lit"

    @property
    def anchors(self) -> str:
        return f"{self.work_prefix}_anchors"

    @property
    def bins_table(self) -> str:
        return f"{self.work_prefix}_bins"

    @property
    def tokens(self) -> str:
        return f"{self.work_prefix}_tokens"

    @property
    def signatures(self) -> str:
        return f"{self.work_prefix}_sigs"

    @property
    def refine_map(self) -> str:
        return f"{self.work_prefix}_refine"

    @property
    def shapes_table(self) -> str:
        return f"{self.output_table}_shapes"

    @property
    def preview_nodes_table(self) -> str:
        return f"{self.output_table}_preview_nodes"

    @property
    def preview_edges_table(self) -> str:
        return f"{self.output_table}_preview_edges"

    def frontier(self, hop: int) -> str:
        return f"{self.work_prefix}_frontier_{int(hop)}"

    def work_tables(self) -> List[str]:
        tables = [
            self.directed,
            self.literals,
            self.anchors,
            self.bins_table,
            self.tokens,
            self.signatures,
            self.refine_map,
        ]
        for h in range(max(0, int(self.hops)) + 1):
            tables.append(self.frontier(h))
        return tables

    def node_type_select_all(self) -> str:
        """One ``rdf:type`` per node, ignoring the anchor ``class_filter``."""
        return (
            f"SELECT subject AS n, MIN(object) AS type_uri\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''\n"
            f"GROUP BY subject"
        )

    def build_directed_edges(self) -> List[str]:
        """Directed URI edges both ways, keeping the predicate as ``via``."""
        src = self.source_table
        excl = _in_list(self.excluded_predicates)
        uri = "(object LIKE 'http://%' OR object LIKE 'https://%')"
        return self._recreate(
            self.directed,
            f"SELECT subject AS src, object AS dst, predicate AS via, "
            f"'out' AS direction\n"
            f"FROM {src}\n"
            f"WHERE subject <> '' AND object <> '' AND subject <> object\n"
            f"  AND {uri}\n"
            f"  AND predicate NOT IN ({excl})\n"
            f"UNION ALL\n"
            f"SELECT object AS src, subject AS dst, predicate AS via, "
            f"'in' AS direction\n"
            f"FROM {src}\n"
            f"WHERE subject <> '' AND object <> '' AND subject <> object\n"
            f"  AND {uri}\n"
            f"  AND predicate NOT IN ({excl})",
        )

    def build_literals(self) -> List[str]:
        src = self.source_table
        excl = _in_list(self.excluded_predicates)
        return self._recreate(
            self.literals,
            f"SELECT subject AS n, predicate AS prop, object AS value\n"
            f"FROM {src}\n"
            f"WHERE subject <> '' AND object <> ''\n"
            f"  AND object NOT LIKE 'http://%'\n"
            f"  AND object NOT LIKE 'https://%'\n"
            f"  AND predicate NOT IN ({excl})",
        )

    def build_anchors(self) -> List[str]:
        return self._recreate(
            self.anchors,
            f"SELECT a.n AS n, a.type_uri AS type_uri, lb.label AS label\n"
            f"FROM ({self.typed_nodes_select()}) t\n"
            f"JOIN ({self.node_type_select()}) a ON a.n = t.n\n"
            f"LEFT JOIN ({self.node_label_select()}) lb ON lb.n = t.n",
        )

    def build_bins(self) -> List[str]:
        """Per-(node, property) bin labels for anchors and hop neighbours."""
        is_num = _portable_is_numeric_sql("value")
        top_n = max(1, int(self.categorical_top_n))
        bins = max(1, int(self.bins))
        return self._recreate(
            self.bins_table,
            f"WITH typed AS (\n"
            f"  SELECT n, prop, value,\n"
            f"         CASE WHEN {is_num} THEN 1 ELSE 0 END AS is_num\n"
            f"  FROM {self.literals}\n"
            f"),\n"
            f"prop_kind AS (\n"
            f"  SELECT prop,\n"
            f"         CASE WHEN MIN(is_num) = 1 THEN 'num' ELSE 'cat' END AS pkind\n"
            f"  FROM typed GROUP BY prop\n"
            f"),\n"
            f"numeric AS (\n"
            f"  SELECT t.n AS n, t.prop AS prop,\n"
            f"         'q' || CAST(\n"
            f"           CASE\n"
            f"             WHEN vr.n_vals <= 1 THEN 0\n"
            f"             WHEN CAST(((vr.vrk - 1) * {bins}) / vr.n_vals AS INT) "
            f">= {bins} THEN {bins - 1}\n"
            f"             ELSE CAST(((vr.vrk - 1) * {bins}) / vr.n_vals AS INT)\n"
            f"           END AS VARCHAR(16)\n"
            f"         ) AS bin_label\n"
            f"  FROM typed t\n"
            f"  JOIN prop_kind pk ON pk.prop = t.prop AND pk.pkind = 'num'\n"
            f"  JOIN (\n"
            f"    SELECT prop, value,\n"
            f"           ROW_NUMBER() OVER (\n"
            f"             PARTITION BY prop ORDER BY CAST(value AS DOUBLE), value\n"
            f"           ) AS vrk,\n"
            f"           COUNT(*) OVER (PARTITION BY prop) AS n_vals\n"
            f"    FROM (\n"
            f"      SELECT DISTINCT d.prop AS prop, d.value AS value\n"
            f"      FROM typed d\n"
            f"      JOIN prop_kind k ON k.prop = d.prop AND k.pkind = 'num'\n"
            f"    ) dv\n"
            f"  ) vr ON vr.prop = t.prop AND vr.value = t.value\n"
            f"),\n"
            f"categorical AS (\n"
            f"  SELECT t.n AS n, t.prop AS prop,\n"
            f"         CASE WHEN fr.freq_rank <= {top_n} THEN t.value\n"
            f"              ELSE '__other' END AS bin_label\n"
            f"  FROM typed t\n"
            f"  JOIN prop_kind pk ON pk.prop = t.prop AND pk.pkind = 'cat'\n"
            f"  JOIN (\n"
            f"    SELECT prop, value,\n"
            f"           ROW_NUMBER() OVER (\n"
            f"             PARTITION BY prop ORDER BY cnt DESC, value\n"
            f"           ) AS freq_rank\n"
            f"    FROM (\n"
            f"      SELECT prop, value, COUNT(*) AS cnt\n"
            f"      FROM typed GROUP BY prop, value\n"
            f"    ) freq\n"
            f"  ) fr ON fr.prop = t.prop AND fr.value = t.value\n"
            f")\n"
            f"SELECT n, prop, bin_label FROM numeric\n"
            f"UNION ALL\n"
            f"SELECT n, prop, bin_label FROM categorical",
        )

    def build_frontier_0(self) -> List[str]:
        return self._recreate(
            self.frontier(0),
            f"SELECT n AS anchor, n AS node FROM {self.anchors}",
        )

    def build_hop(self, hop: int) -> List[str]:
        """Expand one hop and emit structural + neighbour-attribute tokens."""
        prev = self.frontier(hop - 1)
        cur = self.frontier(hop)
        cap = max(1, int(self.frontier_cap))
        hop_lit = str(int(hop))
        # Ranked expansion, then frontier + tokens for survivors.
        expanded = f"{self.work_prefix}_exp_{int(hop)}"
        statements = self._recreate(
            expanded,
            f"SELECT anchor, node, via, direction FROM (\n"
            f"  SELECT f.anchor AS anchor, e.dst AS node, e.via AS via,\n"
            f"         e.direction AS direction,\n"
            f"         ROW_NUMBER() OVER (\n"
            f"           PARTITION BY f.anchor ORDER BY e.via, e.dst\n"
            f"         ) AS rk\n"
            f"  FROM {prev} f\n"
            f"  JOIN {self.directed} e ON e.src = f.node\n"
            f") q\n"
            f"WHERE rk <= {cap}",
        )
        statements += self._recreate(
            cur,
            f"SELECT DISTINCT anchor, node FROM {expanded}",
        )
        # Structural tokens (kind = 's').
        statements.append(
            f"INSERT INTO {self.tokens} (anchor, token, kind)\n"
            f"SELECT e.anchor,\n"
            f"       '{hop_lit}|' || e.via || '|' || coalesce(ty.type_uri, '') "
            f"|| '|' || e.direction,\n"
            f"       's'\n"
            f"FROM {expanded} e\n"
            f"LEFT JOIN ({self.node_type_select_all()}) ty ON ty.n = e.node"
        )
        # Neighbour attribute tokens (kind = 'na').
        statements.append(
            f"INSERT INTO {self.tokens} (anchor, token, kind)\n"
            f"SELECT e.anchor,\n"
            f"       '{hop_lit}|' || e.via || '|' || coalesce(ty.type_uri, '') "
            f"|| '|' || e.direction || '|' || b.prop || '|' || b.bin_label,\n"
            f"       'na'\n"
            f"FROM {expanded} e\n"
            f"JOIN {self.bins_table} b ON b.n = e.node\n"
            f"LEFT JOIN ({self.node_type_select_all()}) ty ON ty.n = e.node"
        )
        statements.append(f"DROP TABLE IF EXISTS {expanded}")
        return statements

    def init_tokens(self) -> List[str]:
        """Create the tokens table and seed hop-0 attribute tokens."""
        statements = [
            f"DROP TABLE IF EXISTS {self.tokens}",
            f"CREATE TABLE {self.tokens} (anchor VARCHAR(1024), token VARCHAR(4096), "
            f"kind VARCHAR(8))",
        ]
        statements.append(
            f"INSERT INTO {self.tokens} (anchor, token, kind)\n"
            f"SELECT a.n,\n"
            f"       '0||' || coalesce(a.type_uri, '') || '|self|' || b.prop "
            f"|| '|' || b.bin_label,\n"
            f"       'a0'\n"
            f"FROM {self.anchors} a\n"
            f"JOIN {self.bins_table} b ON b.n = a.n"
        )
        # Anchors with no attributes still need a type token so empty
        # neighbourhoods are distinguishable by type.
        statements.append(
            f"INSERT INTO {self.tokens} (anchor, token, kind)\n"
            f"SELECT a.n,\n"
            f"       '0||' || coalesce(a.type_uri, '') || '|self||',\n"
            f"       'a0'\n"
            f"FROM {self.anchors} a\n"
            f"WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM {self.bins_table} b WHERE b.n = a.n\n"
            f")"
        )
        return statements

    def build_signatures(self) -> List[str]:
        """Hash each anchor's full token multiset into ``pattern_id``."""
        return self._recreate(
            self.signatures,
            f"SELECT anchor AS anchor_uri,\n"
            f"       md5(concat_ws('|', sort_array(collect_list(token)))) "
            f"AS pattern_id\n"
            f"FROM {self.tokens}\n"
            f"GROUP BY anchor",
        )

    def write_pattern_output(self) -> List[str]:
        """Anchors whose pattern meets ``min_support``."""
        support = max(1, int(self.min_support))
        return self._recreate(
            self.output_table,
            f"SELECT s.anchor_uri AS anchor_uri,\n"
            f"       s.pattern_id AS pattern_id,\n"
            f"       a.type_uri AS type_uri,\n"
            f"       a.label AS label\n"
            f"FROM {self.signatures} s\n"
            f"JOIN {self.anchors} a ON a.n = s.anchor_uri\n"
            f"JOIN (\n"
            f"  SELECT pattern_id FROM {self.signatures}\n"
            f"  GROUP BY pattern_id HAVING COUNT(*) >= {support}\n"
            f") ok ON ok.pattern_id = s.pattern_id",
        )

    def write_shapes(self, *, refined: bool) -> List[str]:
        refined_i = 1 if refined else 0
        return self._recreate(
            self.shapes_table,
            f"SELECT p.pattern_id AS pattern_id,\n"
            f"       p.support AS support,\n"
            f"       coalesce(h.hop_count, 0) AS hop_count,\n"
            f"       coalesce(sh.shape_json, '[]') AS shape_json,\n"
            f"       p.seed_label AS seed_label,\n"
            f"       {refined_i} AS refined,\n"
            f"       coalesce(nd.near_duplicate_count, 1) AS near_duplicate_count\n"
            f"FROM (\n"
            f"  SELECT pattern_id, COUNT(*) AS support,\n"
            f"         MIN(label) AS seed_label\n"
            f"  FROM {self.output_table}\n"
            f"  GROUP BY pattern_id\n"
            f") p\n"
            f"LEFT JOIN (\n"
            f"  SELECT o.pattern_id AS pattern_id,\n"
            f"         MAX(CAST(substr(t.token, 1, instr(t.token || '|', '|') - 1) "
            f"AS INT)) AS hop_count\n"
            f"  FROM {self.output_table} o\n"
            f"  JOIN {self.tokens} t ON t.anchor = o.anchor_uri\n"
            f"  GROUP BY o.pattern_id\n"
            f") h ON h.pattern_id = p.pattern_id\n"
            f"LEFT JOIN (\n"
            f"  SELECT pattern_id,\n"
            f"         '[' || concat_ws(',', sort_array(collect_list(qtok))) "
            f"|| ']' AS shape_json\n"
            f"  FROM (\n"
            f"    SELECT DISTINCT o.pattern_id AS pattern_id,\n"
            f"           '\"' || t.token || '\"' AS qtok\n"
            f"    FROM {self.output_table} o\n"
            f"    JOIN {self.tokens} t ON t.anchor = o.anchor_uri\n"
            f"  ) d\n"
            f"  GROUP BY pattern_id\n"
            f") sh ON sh.pattern_id = p.pattern_id\n"
            f"LEFT JOIN (\n"
            f"  SELECT new_id AS pattern_id, COUNT(*) AS near_duplicate_count\n"
            f"  FROM {self.refine_map}\n"
            f"  GROUP BY new_id\n"
            f") nd ON nd.pattern_id = p.pattern_id",
        )

    def write_pattern_preview(self) -> List[str]:
        per = -(-PATTERN_PREVIEW_MAX_NODES // PREVIEW_PATTERNS)
        statements = self._recreate(
            self.preview_nodes_table,
            f"SELECT node_uri, pattern_id, label, type_uri FROM (\n"
            f"  SELECT o.anchor_uri AS node_uri, o.pattern_id AS pattern_id,\n"
            f"         o.label AS label, o.type_uri AS type_uri,\n"
            f"         ROW_NUMBER() OVER (\n"
            f"           PARTITION BY o.pattern_id ORDER BY o.anchor_uri\n"
            f"         ) AS rk,\n"
            f"         dense_rank() OVER (ORDER BY s.support DESC, o.pattern_id) "
            f"AS prank\n"
            f"  FROM {self.output_table} o\n"
            f"  JOIN (\n"
            f"    SELECT pattern_id, COUNT(*) AS support\n"
            f"    FROM {self.output_table} GROUP BY pattern_id\n"
            f"  ) s ON s.pattern_id = o.pattern_id\n"
            f") q\n"
            f"WHERE prank <= {PREVIEW_PATTERNS} AND rk <= {per}",
        )
        statements += self._recreate(
            self.preview_edges_table,
            f"SELECT e.src AS src, e.dst AS dst\n"
            f"FROM {self.directed} e\n"
            f"JOIN {self.preview_nodes_table} a ON a.node_uri = e.src\n"
            f"JOIN {self.preview_nodes_table} b ON b.node_uri = e.dst\n"
            f"WHERE e.direction = 'out'\n"
            f"ORDER BY e.src, e.dst\n"
            f"LIMIT {PATTERN_PREVIEW_MAX_EDGES}",
        )
        return statements

    def write_pattern_summary(self, stats: Dict[str, object]) -> List[str]:
        columns = [
            f"'{sql_escape(str(stats.get('anchor_type', '') or ''))}' AS anchor_type",
            f"{int(stats['anchor_count'])} AS anchor_count",
            f"{int(stats['pattern_count'])} AS pattern_count",
            f"{int(stats['hop_count'])} AS hop_count",
            f"{int(stats['bins'])} AS bins",
            f"{int(stats['refined'])} AS refined",
            f"{int(stats['min_support'])} AS min_support",
            f"'{sql_escape(str(stats['algorithm']))}' AS algorithm",
            f"'{sql_escape(','.join(self.class_filter))}' AS class_filter",
            f"'{sql_escape(str(stats['source_table']))}' AS source_table",
            f"'{sql_escape(str(stats.get('computed_at', '') or ''))}' AS computed_at",
            f"{int(stats.get('elapsed_ms', 0) or 0)} AS elapsed_ms",
        ]
        return self._recreate(
            self.summary_table, "SELECT " + ",\n       ".join(columns)
        )

    def empty_refine_map(self) -> List[str]:
        return self._recreate(
            self.refine_map,
            "SELECT CAST(NULL AS VARCHAR(64)) AS old_id, "
            "CAST(NULL AS VARCHAR(64)) AS new_id WHERE 1 = 0",
        )

    def build_refine_map(self) -> List[str]:
        """Map exact pattern_ids in the top-N to multiplicity-stripped ids."""
        top = max(0, int(self.refine_top))
        # Stripped tokens: hop-0 attributes + distinct structural keys only.
        stripped = f"{self.work_prefix}_stripped"
        statements = self._recreate(
            stripped,
            f"SELECT DISTINCT o.anchor_uri AS anchor, t.token AS token\n"
            f"FROM {self.output_table} o\n"
            f"JOIN {self.tokens} t ON t.anchor = o.anchor_uri\n"
            f"WHERE t.kind IN ('a0', 's')",
        )
        refined_sigs = f"{self.work_prefix}_refined_sigs"
        statements += self._recreate(
            refined_sigs,
            f"SELECT anchor AS anchor_uri,\n"
            f"       md5(concat_ws('|', sort_array(collect_list(token)))) "
            f"AS refined_id\n"
            f"FROM {stripped}\n"
            f"GROUP BY anchor",
        )
        # Only remap anchors whose exact pattern is among the top-N by support.
        statements += self._recreate(
            self.refine_map,
            f"SELECT DISTINCT o.pattern_id AS old_id, r.refined_id AS new_id\n"
            f"FROM {self.output_table} o\n"
            f"JOIN {refined_sigs} r ON r.anchor_uri = o.anchor_uri\n"
            f"JOIN (\n"
            f"  SELECT pattern_id FROM (\n"
            f"    SELECT pattern_id, COUNT(*) AS support,\n"
            f"           ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, pattern_id) "
            f"AS rk\n"
            f"    FROM {self.output_table}\n"
            f"    GROUP BY pattern_id\n"
            f"  ) ranked WHERE rk <= {top}\n"
            f") topn ON topn.pattern_id = o.pattern_id",
        )
        statements.append(f"DROP TABLE IF EXISTS {stripped}")
        statements.append(f"DROP TABLE IF EXISTS {refined_sigs}")
        return statements

    def apply_refine_map(self) -> List[str]:
        """Rewrite ``pattern_id`` for anchors covered by the refine map.

        Writes through a temp table because ``_recreate`` drops the target
        before the ``SELECT``, so the output cannot be read from itself.
        """
        tmp = f"{self.work_prefix}_out_refined"
        statements = self._recreate(
            tmp,
            f"SELECT o.anchor_uri AS anchor_uri,\n"
            f"       coalesce(m.new_id, o.pattern_id) AS pattern_id,\n"
            f"       o.type_uri AS type_uri,\n"
            f"       o.label AS label\n"
            f"FROM {self.output_table} o\n"
            f"LEFT JOIN {self.refine_map} m ON m.old_id = o.pattern_id",
        )
        statements += self._recreate(self.output_table, f"SELECT * FROM {tmp}")
        statements.append(f"DROP TABLE IF EXISTS {tmp}")
        return statements

    def pattern_count_query(self) -> str:
        return f"SELECT COUNT(DISTINCT pattern_id) AS n FROM {self.output_table}"

    def anchor_count_query(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.anchors}"

    def output_anchor_count_query(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.output_table}"


def run_patterns(
    execute,
    scalar,
    builder: PatternSQL,
    *,
    computed_at: str = "",
    cleanup: bool = True,
) -> Dict[str, object]:
    """Run egocentric signature discovery and write pattern outputs."""
    try:
        return _run_pattern_stages(execute, scalar, builder, computed_at=computed_at)
    finally:
        if cleanup:
            _drop_work_tables(execute, builder)


def _run_pattern_stages(
    execute,
    scalar,
    builder: PatternSQL,
    *,
    computed_at: str,
) -> Dict[str, object]:
    if not builder.class_filter:
        raise ValueError("class_filter is required for pattern detection")

    t0 = time.time()
    for stmt in builder.build_directed_edges():
        execute(stmt)
    for stmt in builder.build_literals():
        execute(stmt)
    for stmt in builder.build_anchors():
        execute(stmt)
    for stmt in builder.build_bins():
        execute(stmt)
    for stmt in builder.init_tokens():
        execute(stmt)
    for stmt in builder.build_frontier_0():
        execute(stmt)

    hops = max(0, int(builder.hops))
    for h in range(1, hops + 1):
        for stmt in builder.build_hop(h):
            execute(stmt)
        logger.info("pattern hop %d expanded", h)

    for stmt in builder.build_signatures():
        execute(stmt)
    for stmt in builder.write_pattern_output():
        execute(stmt)

    refined = 0
    if int(builder.refine_top) > 0:
        for stmt in builder.build_refine_map():
            execute(stmt)
        for stmt in builder.apply_refine_map():
            execute(stmt)
        refined = 1
    else:
        for stmt in builder.empty_refine_map():
            execute(stmt)

    for stmt in builder.write_shapes(refined=bool(refined)):
        execute(stmt)
    for stmt in builder.write_pattern_preview():
        execute(stmt)

    pattern_count = int(scalar(builder.pattern_count_query()) or 0)
    # Anchors that survived min_support (and refine), not the raw class count.
    anchor_count = int(scalar(builder.output_anchor_count_query()) or 0)
    raw_anchor_count = int(scalar(builder.anchor_count_query()) or 0)
    if raw_anchor_count and not pattern_count:
        # Keep population visible even when nothing met min_support.
        anchor_count = 0

    stats: Dict[str, object] = {
        "anchor_type": builder.class_filter[0] if builder.class_filter else "",
        "anchor_count": raw_anchor_count if pattern_count else raw_anchor_count,
        "pattern_count": pattern_count,
        "hop_count": hops,
        "bins": int(builder.bins),
        "refined": refined,
        "min_support": int(builder.min_support),
        "algorithm": PATTERN_ALGORITHM,
        "source_table": builder.source_table,
        "computed_at": computed_at,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }
    # When patterns exist, report how many anchors were kept in the output.
    if pattern_count:
        stats["anchor_count"] = int(scalar(builder.output_anchor_count_query()) or 0)
    for stmt in builder.write_pattern_summary(stats):
        execute(stmt)
    return stats


def run_communities(
    execute,
    scalar,
    builder: CommunitySQL,
    *,
    lp_iterations: int = DEFAULT_LP_ITERATIONS,
    min_change_ratio: float = DEFAULT_LP_MIN_CHANGE_RATIO,
    computed_at: str = "",
    cleanup: bool = True,
) -> Dict[str, object]:
    """Run label propagation and write the community outputs.

    Same engine-agnostic contract as :func:`run_analysis`: *execute* runs a
    statement, *scalar* returns the first column of the first row. Work tables
    are dropped on the way out, failure included, unless *cleanup* is False.
    """
    try:
        return _run_community_stages(
            execute,
            scalar,
            builder,
            lp_iterations=lp_iterations,
            min_change_ratio=min_change_ratio,
            computed_at=computed_at,
        )
    finally:
        if cleanup:
            _drop_work_tables(execute, builder)


def _run_community_stages(
    execute,
    scalar,
    builder: CommunitySQL,
    *,
    lp_iterations: int,
    min_change_ratio: float,
    computed_at: str,
) -> Dict[str, object]:
    t0 = time.time()
    for stmt in builder.build_edges():
        execute(stmt)
    node_count = int(scalar(builder.node_count_query()) or 0)
    edge_count = int(scalar(builder.edge_count_query()) or 0)
    logger.info("communities: %d nodes, %d edges", node_count, edge_count)

    for stmt in builder.lp_init():
        execute(stmt)
    slot = "a"
    iterations = 0
    converged = node_count == 0
    quiet_rounds = 0
    min_change = max(0, int(node_count * max(0.0, min_change_ratio)))
    for i in range(max(1, lp_iterations) if node_count else 0):
        nxt = "b" if slot == "a" else "a"
        for stmt in builder.lp_iteration(slot, nxt, parity=i):
            execute(stmt)
        changed = int(scalar(builder.lp_changed_query(slot, nxt)) or 0)
        slot = nxt
        iterations += 1
        logger.info("label propagation round %d: %d labels changed", iterations, changed)
        quiet_rounds = quiet_rounds + 1 if changed <= min_change else 0
        if quiet_rounds >= 2:
            converged = True
            break
    if not converged:
        logger.warning(
            "label propagation did not settle in %d rounds — the partition is "
            "reported as unconverged",
            iterations,
        )

    for stmt in builder.write_sizes(slot) + builder.write_community_output(slot):
        execute(stmt)
    community_count = int(scalar(builder.community_count_query()) or 0)
    modularity = modularity_from_sums(
        float(scalar(builder.intra_edges_query()) or 0),
        float(scalar(builder.degree_square_sum_query()) or 0),
        edge_count,
    )
    for stmt in builder.write_preview():
        execute(stmt)

    stats: Dict[str, object] = {
        "node_count": node_count,
        "edge_count": edge_count,
        "community_count": community_count,
        "modularity": round(modularity, 6),
        "iterations": iterations,
        "converged": converged,
        "algorithm": COMMUNITY_ALGORITHM,
        "source_table": builder.source_table,
        "computed_at": computed_at,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }
    for stmt in builder.write_community_summary(stats):
        execute(stmt)
    return stats


def merge_excluded_predicates(raw: Optional[str]) -> List[str]:
    """Combine the caller's ``--exclude-predicates`` with the defaults.

    The caller's list *adds to* the defaults rather than replacing them. A user
    excluding one noisy predicate must not thereby promote ``rdf:type`` and
    ``rdfs:label`` into entity-entity edges, which would connect every instance
    to its class node and silently inflate every degree and PageRank in the run.
    """
    requested = [p.strip() for p in (raw or "").split(",") if p.strip()]
    return list(DEFAULT_EXCLUDED_PREDICATES) + [
        p for p in requested if p not in DEFAULT_EXCLUDED_PREDICATES
    ]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute iterative graph metrics for an OntoBricks knowledge graph"
    )
    parser.add_argument(
        "--task",
        choices=("metrics", "communities", "patterns"),
        default="metrics",
        help=(
            "metrics: centrality and structure; communities: label propagation; "
            "patterns: egocentric neighbourhood signatures"
        ),
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
        "--class-filter",
        default="",
        help=(
            "Comma-separated entity type URIs. When set, only nodes carrying "
            "one of these rdf:type values are scored, and only edges whose "
            "both endpoints survive."
        ),
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
        "--lp-iterations", type=int, default=DEFAULT_LP_ITERATIONS,
        help="Cap on label-propagation rounds (communities task)",
    )
    parser.add_argument(
        "--pattern-hops", type=int, default=DEFAULT_PATTERN_HOPS,
        help="Neighbourhood depth for pattern signatures",
    )
    parser.add_argument(
        "--pattern-bins", type=int, default=DEFAULT_PATTERN_BINS,
        help="Quantile buckets for numeric attribute bins",
    )
    parser.add_argument(
        "--pattern-min-support", type=int, default=DEFAULT_PATTERN_MIN_SUPPORT,
        help="Drop patterns with fewer anchors than this",
    )
    parser.add_argument(
        "--pattern-frontier", type=int, default=DEFAULT_PATTERN_FRONTIER,
        help="Max neighbours kept per anchor per hop",
    )
    parser.add_argument(
        "--refine-top", type=int, default=DEFAULT_PATTERN_REFINE_TOP,
        help="Collapse multiplicity near-duplicates among the top-N patterns (0=off)",
    )
    parser.add_argument(
        "--computed-at", default="",
        help="Run timestamp recorded in the community/pattern summary",
    )
    parser.add_argument(
        "--keep-work-tables",
        action="store_true",
        help="Leave intermediate tables in place for debugging",
    )
    return parser.parse_args(argv)


def _run_selected_task(args, execute, scalar, *, excluded, class_filter, work_prefix) -> int:
    """Dispatch ``--task`` to patterns, communities, or graph metrics."""
    if args.task == "patterns":
        builder = PatternSQL(
            source_table=args.source_table,
            work_prefix=work_prefix,
            output_table=args.output_table,
            excluded_predicates=excluded,
            class_filter=class_filter,
            hops=args.pattern_hops,
            bins=args.pattern_bins,
            min_support=args.pattern_min_support,
            frontier_cap=args.pattern_frontier,
            refine_top=args.refine_top,
        )
        stats = run_patterns(
            execute,
            scalar,
            builder,
            computed_at=args.computed_at,
            cleanup=not args.keep_work_tables,
        )
        print("ONTOBRICKS_PATTERN_SUMMARY " + json.dumps(stats, default=str))
        logger.info(
            "wrote %s (%s patterns)", args.output_table, stats["pattern_count"]
        )
        return 0

    if args.task == "communities":
        builder = CommunitySQL(
            source_table=args.source_table,
            work_prefix=work_prefix,
            output_table=args.output_table,
            excluded_predicates=excluded,
            damping=args.damping,
            class_filter=class_filter,
        )
        stats = run_communities(
            execute,
            scalar,
            builder,
            lp_iterations=args.lp_iterations,
            computed_at=args.computed_at,
            cleanup=not args.keep_work_tables,
        )
        print("ONTOBRICKS_COMMUNITY_SUMMARY " + json.dumps(stats, default=str))
        logger.info(
            "wrote %s (%s communities)", args.output_table, stats["community_count"]
        )
        return 0

    builder = GraphAnalyticsSQL(
        source_table=args.source_table,
        work_prefix=work_prefix,
        output_table=args.output_table,
        excluded_predicates=excluded,
        damping=args.damping,
        class_filter=class_filter,
    )
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


def main(argv: Optional[List[str]] = None) -> int:
    """Spark entry point. Imports pyspark lazily so the builders stay testable."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    excluded = merge_excluded_predicates(args.exclude_predicates)
    class_filter = [
        v.strip() for v in (args.class_filter or "").split(",") if v.strip()
    ]
    work_prefix = args.work_prefix or f"{args.output_table}_work"
    validate_identifier(args.source_table, "--source-table")
    validate_identifier(args.output_table, "--output-table")
    validate_identifier(work_prefix, "--work-prefix")

    if class_filter:
        logger.info("class filter: %d type(s) — %s", len(class_filter), ", ".join(class_filter))

    def execute(stmt: str) -> None:
        logger.debug("executing: %s", stmt)
        spark.sql(stmt)

    def scalar(query: str):
        row = spark.sql(query).head()
        return row[0] if row is not None else None

    return _run_selected_task(
        args, execute, scalar,
        excluded=excluded, class_filter=class_filter, work_prefix=work_prefix,
    )


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
