"""CohortBuilder — backend-agnostic cohort discovery engine.

The engine pulls all triples through ``store.query_triples(graph_name)``
(the only call that works on every backend, SQL and Cypher alike), then
applies the user-authored :class:`CohortRule` in pure Python:

  1. List class members (subjects with ``rdf:type = class_uri``).
  2. Fetch attribute values for the properties referenced by compatibility
     rows.
  3. Apply node filters (``value_equals`` / ``value_in`` / ``value_range``).
  4. Build candidate edges per :class:`CohortLink` (members sharing a
     bridging entity reachable through ``via``); combine via union (``any``)
     or intersection (``all``).
  5. Apply edge filters (``same_value``).
  6. Run NetworkX ``connected_components`` (group_type=connected) or
     ``find_cliques`` (group_type=strict).
  7. Filter by ``min_size``, sort by size desc, hash the sorted-member list
     into a content-hash URI.

Materialisation:
  * ``materialize_to_graph`` — delete old cohort triples for the rule
    (URI-prefix-based), then ``insert_triples`` the new payload.
  * ``materialize_to_uc`` — ``CREATE TABLE IF NOT EXISTS``, ``DELETE`` the
    rule's partition, then chunked ``INSERT``.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import networkx as nx

from back.core.helpers import extract_local_name, sql_escape
from back.core.logging import get_logger
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.graph_analysis.models import (
    CohortCompat,
    CohortGroup,
    CohortHop,
    CohortLink,
    CohortResult,
    CohortRule,
    CohortStats,
    CohortUCTarget,
)
from back.core.graph_analysis.CohortVocabulary import CohortVocabulary

logger = get_logger(__name__)


_DDL_CREATE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS {fq} (
    rule_id            STRING    NOT NULL,
    rule_label         STRING    NOT NULL,
    cohort_id          STRING    NOT NULL,
    cohort_uri         STRING    NOT NULL,
    cohort_idx         INT       NOT NULL,
    cohort_size        INT       NOT NULL,
    member_uri         STRING    NOT NULL,
    member_id          STRING,
    member_label       STRING,
    domain_name        STRING    NOT NULL,
    domain_version     STRING    NOT NULL,
    materialised_at    TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (rule_id)
TBLPROPERTIES (
    'delta.appendOnly'      = 'false',
    'ontobricks.created_by' = 'cohort_discovery'
)
"""


class CohortBuilder:
    """Backend-agnostic cohort discovery engine.

    Operates on a triplestore/graph-DB backend (anything that exposes
    ``query_triples(graph_name)`` and ``insert_triples(graph_name, triples)``)
    plus a domain ``base_uri`` used to mint cohort URIs.
    """

    def __init__(
        self, store: Any, graph_name: str, base_uri: str = ""
    ) -> None:
        self._store = store
        self._graph_name = graph_name
        self._base_uri = base_uri or ""
        self._cache: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # URI namespace normalisation
    # ------------------------------------------------------------------
    #
    # Codebase convention: the ontology stores property URIs with the
    # base_uri's separator (typically ``…#name``), but data triples are
    # loaded with the *data namespace* — ``base_uri.rstrip("#") + "/"`` —
    # so live predicates look like ``…/name``. SPARQL, SWRL, SHACL and
    # Aggregate engines all rewrite property URIs through this
    # translation before querying triples (see
    # :meth:`SPARQLRuleEngine._build_uri_map`); the cohort engine has to
    # do the same or otherwise lookups by raw ontology URI silently
    # return zero matches on R2RML-loaded data.
    #
    # Class URIs are *usually* preserved in ontology form by R2RML
    # (``rr:class`` keeps the URI verbatim — see
    # :class:`R2RMLGenerator`), but some loader paths and W3C
    # round-trips emit ``rdf:type`` triples with class URIs in the
    # data namespace. To stay robust we always compare ``rdf:type``
    # objects against *both* forms via :meth:`_class_uri_variants`.

    def _data_namespace(self) -> str:
        if not self._base_uri:
            return ""
        return self._base_uri.rstrip("#").rstrip("/") + "/"

    def _to_data_uri(self, uri: str) -> str:
        """Rewrite *uri* from ontology form to data form.

        - Already-data URIs are returned unchanged.
        - URIs starting with ``base_uri`` get their ``#`` separator
          rewritten to ``/``.
        - Anything else (foreign namespaces, URNs, blank nodes) is
          returned unchanged.
        """
        if not uri or not self._base_uri:
            return uri
        data_ns = self._data_namespace()
        if not data_ns or uri.startswith(data_ns):
            return uri
        base_no_sep = self._base_uri.rstrip("#").rstrip("/")
        if uri.startswith(base_no_sep + "#"):
            return data_ns + uri[len(base_no_sep) + 1:]
        if uri.startswith(self._base_uri):
            return data_ns + uri[len(self._base_uri):]
        return uri

    def _to_ontology_uri(self, uri: str) -> str:
        """Inverse of :meth:`_to_data_uri` — rewrite a data-namespace URI
        back to ontology form (``base_uri`` + ``#`` + local).

        - Already-ontology URIs (those that don't start with the data
          namespace) are returned unchanged.
        - The ontology separator follows the configured ``base_uri``
          (``#`` if it ends with ``#``, ``/`` otherwise).
        - URIs in foreign namespaces are returned unchanged.
        """
        if not uri or not self._base_uri:
            return uri
        data_ns = self._data_namespace()
        if not data_ns or not uri.startswith(data_ns):
            return uri
        local = uri[len(data_ns):]
        base_no_sep = self._base_uri.rstrip("#").rstrip("/")
        sep = "#" if self._base_uri.endswith("#") else "/"
        return base_no_sep + sep + local

    def _class_uri_variants(self, class_uri: str) -> Set[str]:
        """Every URI form a class might be stored under in ``rdf:type``
        triples — the rule's own form, the data-namespace form, and
        the ontology-namespace form. Used to defend against R2RML /
        W3C-import drift between the rule (typically ``…#Person``)
        and the data (``…/Person`` or ``…#Person`` depending on the
        loader).
        """
        if not class_uri:
            return set()
        variants = {class_uri}
        if self._base_uri:
            variants.add(self._to_data_uri(class_uri))
            variants.add(self._to_ontology_uri(class_uri))
        return {v for v in variants if v}

    def _normalized_links(self, links: List[CohortLink]) -> List[CohortLink]:
        """Return a copy of *links* with every hop's ``via`` and
        ``where[*].property`` rewritten to the data namespace.
        ``target_class`` is left alone — class URIs use the ontology
        form on both sides.
        """
        if not self._base_uri:
            return list(links)
        out: List[CohortLink] = []
        for lk in links:
            new_path: List[CohortHop] = []
            for h in lk.hops():
                new_where: List[CohortCompat] = []
                for w in h.where or []:
                    new_w = CohortCompat(
                        type=w.type,
                        property=self._to_data_uri(w.property),
                        value=w.value,
                        values=w.values,
                        min=w.min,
                        max=w.max,
                        allow_missing=w.allow_missing,
                    )
                    new_where.append(new_w)
                new_path.append(
                    CohortHop(
                        via=self._to_data_uri(h.via),
                        target_class=h.target_class,
                        where=new_where,
                    )
                )
            out.append(CohortLink(path=new_path))
        return out

    def _normalized_compat(
        self, compatibility: List[CohortCompat]
    ) -> List[CohortCompat]:
        if not self._base_uri:
            return list(compatibility)
        return [
            CohortCompat(
                type=c.type,
                property=self._to_data_uri(c.property),
                value=c.value,
                values=c.values,
                min=c.min,
                max=c.max,
                allow_missing=c.allow_missing,
            )
            for c in compatibility
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, rule: CohortRule) -> CohortResult:
        """Run the full pipeline on *rule* and return cohorts + stats."""
        errors = rule.validate()
        if errors:
            raise ValueError("Invalid cohort rule: " + "; ".join(errors))

        t0 = time.time()
        triples = self._load_triples(rule.max_triples)

        members = self._members_of_class(triples, rule.class_uri)
        norm_compat = self._normalized_compat(rule.compatibility)
        norm_links = self._normalized_links(rule.links)
        attrs = self._fetch_attrs_for_props(
            triples, members, [c.property for c in norm_compat if c.property]
        )

        survivors = self._apply_node_filters(members, attrs, norm_compat)
        edges = self._build_edges_from_links(
            triples, survivors, norm_links, rule.links_combine
        )
        edges = self._apply_edge_filters(edges, attrs, norm_compat)

        cohorts = self._run_grouping(survivors, edges, rule)
        ranked = self._rank_and_hash(cohorts, rule)

        elapsed_ms = int((time.time() - t0) * 1000)
        grouped = sum(c.size for c in ranked)

        stats = CohortStats(
            rule_id=rule.id,
            class_member_count=len(members),
            survivor_count=len(survivors),
            edge_count=len(edges),
            cohort_count=len(ranked),
            grouped_member_count=grouped,
            elapsed_ms=elapsed_ms,
        )
        logger.info(
            "CohortBuilder[%s]: members=%d survivors=%d edges=%d cohorts=%d in %dms",
            rule.id,
            len(members),
            len(survivors),
            len(edges),
            len(ranked),
            elapsed_ms,
        )
        return CohortResult(rule_id=rule.id, cohorts=ranked, stats=stats)

    # ---- live preview helpers (Stage 1–3 only) ----------------------

    def count_class_members(self, class_uri: str) -> int:
        """Stage 1 helper: count instances of *class_uri*."""
        triples = self._load_triples(max_triples=None, allow_overflow=True)
        return len(self._members_of_class(triples, class_uri))

    def count_link_edges(
        self,
        class_uri: str,
        links: List[CohortLink],
        combine: str = "any",
    ) -> int:
        """Stage 3b helper: count candidate edges produced by *links*."""
        triples = self._load_triples(max_triples=None, allow_overflow=True)
        members = self._members_of_class(triples, class_uri)
        edges = self._build_edges_from_links(
            triples, members, self._normalized_links(links), combine
        )
        return len(edges)

    def trace_paths(
        self,
        class_uri: str,
        links: List[CohortLink],
        compatibility: Optional[List[CohortCompat]] = None,
    ) -> Dict[str, Any]:
        """Diagnose where a multi-hop rule loses candidates.

        Walks every link's path with per-hop counters so users can see
        exactly which hop empties the frontier. Honours the rule's
        compatibility (Stage 3a) so the starting frontier matches the
        real preview.

        Returned shape::

            {
              "class_uri": "...",
              "class_member_count": 6,
              "survivor_count": 6,
              "links": [
                {
                  "link_idx": 0,
                  "starting_frontier": 6,
                  "terminal_frontier": 0,
                  "edge_count": 0,
                  "hops": [
                    {
                      "hop_idx": 0,
                      "via": "...",
                      "target_class": "...",
                      "in_frontier": 6,
                      "neighbours_raw": 12,
                      "dropped_type": 0,
                      "dropped_where": 0,
                      "out_frontier": 12,
                      "where_count": 0
                    },
                    ...
                  ]
                },
                ...
              ]
            }

        Counters meaning:
          * ``in_frontier`` — distinct nodes at hop entry
          * ``neighbours_raw`` — count of (node → predicate) edges
            traversed, before any filter
          * ``dropped_type`` — neighbours rejected by ``target_class``
            type filter (after the lenient fallback)
          * ``dropped_where`` — neighbours rejected by hop-level
            ``where`` filters
          * ``out_frontier`` — distinct surviving neighbours
        """
        triples = self._load_triples(max_triples=None, allow_overflow=True)
        members = self._members_of_class(triples, class_uri)
        compat = self._normalized_compat(list(compatibility or []))
        if compat:
            attrs = self._fetch_attrs_for_props(
                triples, members, [c.property for c in compat if c.property]
            )
            survivors = self._apply_node_filters(members, attrs, compat)
        else:
            survivors = set(members)

        norm_links = self._normalized_links(links)
        out_index = self._outgoing_edge_index(triples)
        type_index = self._type_index_for_links(triples, norm_links)
        hop_attrs = self._hop_where_attr_index(triples, norm_links)

        link_traces: List[Dict[str, Any]] = []
        for idx, lk in enumerate(norm_links):
            hops = lk.hops()
            trace = self._trace_path(
                out_index, type_index, hop_attrs, survivors, hops
            )
            terminal = trace[-1]["out_frontier"] if trace else len(survivors)
            edge_count = self._count_edges_from_terminal_frontier(
                out_index, type_index, hop_attrs, survivors, hops
            )
            link_traces.append(
                {
                    "link_idx": idx,
                    "starting_frontier": len(survivors),
                    "terminal_frontier": terminal,
                    "edge_count": edge_count,
                    "hops": trace,
                }
            )
        return {
            "class_uri": class_uri,
            "class_member_count": len(members),
            "survivor_count": len(survivors),
            "links": link_traces,
        }

    def count_matching_nodes(
        self,
        class_uri: str,
        compatibility: List[CohortCompat],
    ) -> Tuple[int, int]:
        """Stage 3a helper: return ``(matching, total)`` member counts."""
        triples = self._load_triples(max_triples=None, allow_overflow=True)
        members = self._members_of_class(triples, class_uri)
        norm_compat = self._normalized_compat(compatibility)
        attrs = self._fetch_attrs_for_props(
            triples, members, [c.property for c in norm_compat if c.property]
        )
        survivors = self._apply_node_filters(members, attrs, norm_compat)
        return len(survivors), len(members)

    def sample_property_values(
        self,
        class_uri: str,
        property_uri: str,
        limit: int = 20,
    ) -> List[Any]:
        """Return up to *limit* distinct values of *property_uri* for instances
        of *class_uri* present in the graph."""
        triples = self._load_triples(max_triples=None, allow_overflow=True)
        members = self._members_of_class(triples, class_uri)
        property_uri = self._to_data_uri(property_uri)
        if not members or not property_uri:
            return []
        seen: List[Any] = []
        seen_set: Set[Any] = set()
        for t in triples:
            if t.get("predicate") != property_uri:
                continue
            if t.get("subject") not in members:
                continue
            obj = t.get("object")
            if obj is None or obj in seen_set:
                continue
            seen.append(obj)
            seen_set.add(obj)
            if len(seen) >= limit:
                break
        return seen

    def explain_membership(
        self, rule: CohortRule, target: str
    ) -> Dict[str, Any]:
        """Return a per-stage breakdown of why *target* did or did not end
        up in a cohort.  Used by the "Why?" / "Why not?" UI.
        """
        triples = self._load_triples(rule.max_triples)
        members = self._members_of_class(triples, rule.class_uri)
        in_class = target in members
        if not in_class:
            return {
                "uri": target,
                "in_class": False,
                "reason": self._explain_missing_member(triples, rule, target),
            }

        norm_compat = self._normalized_compat(rule.compatibility)
        norm_links = self._normalized_links(rule.links)
        attrs = self._fetch_attrs_for_props(
            triples, members, [c.property for c in norm_compat if c.property]
        )
        survivors = self._apply_node_filters(members, attrs, norm_compat)
        compatible = target in survivors

        failing_constraints: List[Dict[str, Any]] = []
        if not compatible:
            for cc in norm_compat:
                if cc.type == "same_value":
                    continue
                v = attrs.get(cc.property, {}).get(target)
                if not _node_filter_passes(cc, v):
                    failing_constraints.append(
                        {
                            "type": cc.type,
                            "property": cc.property,
                            "actual": v,
                            "expected": _expected_repr(cc),
                        }
                    )

        edges = (
            self._build_edges_from_links(triples, survivors, norm_links, rule.links_combine)
            if compatible
            else set()
        )
        edges = self._apply_edge_filters(edges, attrs, norm_compat)

        cohorts: List[CohortGroup] = []
        if compatible:
            groups = self._run_grouping(survivors, edges, rule)
            cohorts = self._rank_and_hash(groups, rule)

        in_cohort: Optional[Dict[str, Any]] = None
        for c in cohorts:
            if target in c.members:
                in_cohort = {"cohort_id": c.id, "size": c.size, "idx": c.idx}
                break

        return {
            "uri": target,
            "in_class": True,
            "compatible": compatible,
            "failing_constraints": failing_constraints,
            "in_cohort": in_cohort,
        }

    def _explain_missing_member(
        self,
        triples: List[Dict[str, str]],
        rule: CohortRule,
        target: str,
    ) -> str:
        """Build a diagnostic ``reason`` string when *target* is not in
        the rule's class membership set.

        Tells the user *why* the lookup failed by inspecting what the
        graph actually carries for the target subject:

        - the target has rdf:type triples but to a different class →
          report what it *is* typed as
        - the target appears as a subject but has no rdf:type triple →
          report the missing-type case (cohort matching needs an
          explicit type)
        - the target is nowhere in the graph → report the URI was not
          seen at all
        """
        actual_types: Set[str] = set()
        seen_as_subject = False
        for t in triples:
            if t.get("subject") != target:
                continue
            seen_as_subject = True
            if t.get("predicate") == RDF_TYPE:
                obj = t.get("object")
                if obj:
                    actual_types.add(obj)
        class_uri = rule.class_uri
        if actual_types:
            types_str = ", ".join(f"'{t}'" for t in sorted(actual_types))
            return (
                f"'{target}' exists in the graph but is typed as {types_str}, "
                f"not as '{class_uri}'. Either pick the matching class in "
                f"step 2 or fix the rdf:type triples in the registry."
            )
        if seen_as_subject:
            return (
                f"'{target}' exists in the graph but has no rdf:type triple. "
                f"Cohort matching needs an explicit rdf:type to "
                f"'{class_uri}' — check your R2RML mapping or the loader."
            )
        return (
            f"'{target}' was not found as the subject of any triple — "
            f"the URI is unknown to the graph. Double-check the URI you "
            f"pasted (case, encoding, trailing slash) and confirm the "
            f"member appears in a Preview run for rule '{class_uri}'."
        )

    # ---- materialisation --------------------------------------------

    def materialize_to_graph(self, rule: CohortRule, result: CohortResult) -> int:
        """Delete prior cohort triples for *rule*, insert fresh ones.

        Returns the number of triples inserted.
        """
        if self._store is None:
            return 0
        prefix = CohortVocabulary.cohort_prefix(self._base_uri, rule.id)
        in_cohort = CohortVocabulary.in_cohort(self._base_uri, rule.id)

        # Delete by URI prefix — wipes both cohort entity triples (subject
        # under prefix) and membership triples (predicate=inCohort<RuleId>,
        # object under prefix).  Implementation lives on the backend.
        delete_fn = getattr(self._store, "delete_cohort_triples", None)
        if callable(delete_fn):
            try:
                delete_fn(self._graph_name, prefix, in_cohort)
            except Exception as exc:
                logger.warning(
                    "CohortBuilder: delete_cohort_triples failed for %s: %s",
                    rule.id,
                    exc,
                )

        triples = self._build_cohort_triples(rule, result)
        if not triples:
            return 0
        return int(self._store.insert_triples(self._graph_name, triples))

    def materialize_to_uc(
        self,
        rule: CohortRule,
        result: CohortResult,
        client: Any,
        target: CohortUCTarget,
        domain_name: str = "",
        domain_version: str = "",
        member_labels: Optional[Dict[str, str]] = None,
    ) -> int:
        """Write cohort rows to a Unity Catalog Delta table (idempotent).

        Returns the number of rows inserted.
        """
        if not target or not target.table_name:
            return 0
        fq = target.fq_name()

        client.execute_statement(_DDL_CREATE_IF_NOT_EXISTS.format(fq=fq))
        rid_safe = sql_escape(rule.id)
        client.execute_statement(
            f"DELETE FROM {fq} WHERE rule_id = '{rid_safe}'"
        )

        rows = self._build_uc_rows(
            rule,
            result,
            domain_name=domain_name,
            domain_version=domain_version,
            member_labels=member_labels or {},
        )
        if not rows:
            return 0
        return _insert_rows_chunked(client, fq, rows, chunk_size=5_000)

    # ------------------------------------------------------------------
    # Stage 1 — list members
    # ------------------------------------------------------------------

    def _members_of_class(
        self, triples: List[Dict[str, str]], class_uri: str
    ) -> Set[str]:
        if not class_uri:
            return set()
        variants = self._class_uri_variants(class_uri)
        return {
            t["subject"]
            for t in triples
            if t.get("predicate") == RDF_TYPE and t.get("object") in variants
        }

    # ------------------------------------------------------------------
    # Stage 2 — fetch attribute values
    # ------------------------------------------------------------------

    def _fetch_attrs_for_props(
        self,
        triples: List[Dict[str, str]],
        members: Set[str],
        props: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        if not props:
            return {}
        prop_set = set(props)
        attrs: Dict[str, Dict[str, Any]] = {p: {} for p in prop_set}
        for t in triples:
            pred = t.get("predicate")
            if pred not in prop_set:
                continue
            subj = t.get("subject")
            if subj in members and subj not in attrs[pred]:
                attrs[pred][subj] = t.get("object")
        return attrs

    # ------------------------------------------------------------------
    # Stage 3a — node filters
    # ------------------------------------------------------------------

    def _apply_node_filters(
        self,
        members: Set[str],
        attrs: Dict[str, Dict[str, Any]],
        compatibility: List[CohortCompat],
    ) -> Set[str]:
        node_constraints = [c for c in compatibility if c.type != "same_value"]
        if not node_constraints:
            return set(members)
        survivors: Set[str] = set()
        for m in members:
            ok = True
            for cc in node_constraints:
                v = attrs.get(cc.property, {}).get(m)
                if v is None:
                    if cc.allow_missing:
                        continue
                    ok = False
                    break
                if not _node_filter_passes(cc, v):
                    ok = False
                    break
            if ok:
                survivors.add(m)
        return survivors

    # ------------------------------------------------------------------
    # Stage 3b — build edges
    # ------------------------------------------------------------------

    def _build_edges_from_links(
        self,
        triples: List[Dict[str, str]],
        members: Set[str],
        links: List[CohortLink],
        combine: str,
    ) -> Set[Tuple[str, str]]:
        if not members:
            return set()
        if not links:
            # No togetherness predicate: every pair is linked.
            sorted_members = sorted(members)
            return {
                (a, b)
                for i, a in enumerate(sorted_members)
                for b in sorted_members[i + 1 :]
            }

        out_index = self._outgoing_edge_index(triples)
        type_index = self._type_index_for_links(triples, links)
        hop_attrs = self._hop_where_attr_index(triples, links)

        per_link_edges: List[Set[Tuple[str, str]]] = []
        for lk in links:
            terminals = self._walk_path(
                out_index, type_index, hop_attrs, members, lk.hops()
            )
            edges: Set[Tuple[str, str]] = set()
            for src_set in terminals.values():
                if len(src_set) < 2:
                    continue
                uniq = sorted(src_set)
                for i, a in enumerate(uniq):
                    for b in uniq[i + 1 :]:
                        edges.add((a, b))
            per_link_edges.append(edges)

        if not per_link_edges:
            return set()
        if combine == "all":
            edges = per_link_edges[0]
            for s in per_link_edges[1:]:
                edges = edges & s
            return edges
        edges = set()
        for s in per_link_edges:
            edges |= s
        return edges

    @staticmethod
    def _outgoing_edge_index(
        triples: List[Dict[str, str]],
    ) -> Dict[Tuple[str, str], List[str]]:
        """Index ``(subject, predicate) -> [object, …]`` for path walking."""
        idx: Dict[Tuple[str, str], List[str]] = {}
        for t in triples:
            subj = t.get("subject")
            pred = t.get("predicate")
            if subj is None or pred is None or pred == RDF_TYPE:
                continue
            obj = t.get("object")
            if obj is None:
                continue
            idx.setdefault((subj, pred), []).append(obj)
        return idx

    def _type_index_for_links(
        self,
        triples: List[Dict[str, str]],
        links: List[CohortLink],
    ) -> Dict[str, Set[str]]:
        """Index ``hop.target_class -> {subject URIs typed as that class}``.

        Robust to URI-form drift: each ``rdf:type`` object is matched
        against every variant (ontology + data form) of every wanted
        class — see :meth:`_class_uri_variants`.
        """
        wanted: Set[str] = set()
        for lk in links:
            for h in lk.hops():
                if h.target_class:
                    wanted.add(h.target_class)
        if not wanted:
            return {}
        # variant_to_canonical: every URI form (ontology + data) ->
        # the canonical wanted class URI it represents. A single triple
        # can therefore route into multiple buckets if two wanted
        # classes share a variant (rare but legal).
        variant_to_canonical: Dict[str, Set[str]] = {}
        for cls in wanted:
            for v in self._class_uri_variants(cls):
                variant_to_canonical.setdefault(v, set()).add(cls)
        index: Dict[str, Set[str]] = {c: set() for c in wanted}
        for t in triples:
            if t.get("predicate") != RDF_TYPE:
                continue
            obj = t.get("object")
            canonicals = variant_to_canonical.get(obj or "")
            if not canonicals:
                continue
            subj = t.get("subject", "")
            for c in canonicals:
                index[c].add(subj)
        return index

    @staticmethod
    def _hop_where_attr_index(
        triples: List[Dict[str, str]],
        links: List[CohortLink],
    ) -> Dict[str, Dict[str, Any]]:
        """Index ``predicate -> {subject: value}`` for hop ``where`` props.

        Unlike :meth:`_fetch_attrs_for_props`, this does **not** restrict to
        the rule's source-class members — hop targets can be instances of
        any class along the path.
        """
        wanted: Set[str] = set()
        for lk in links:
            for h in lk.hops():
                for w in h.where:
                    if w.property:
                        wanted.add(w.property)
        if not wanted:
            return {}
        attrs: Dict[str, Dict[str, Any]] = {p: {} for p in wanted}
        for t in triples:
            pred = t.get("predicate")
            if pred not in wanted:
                continue
            subj = t.get("subject", "")
            if not subj or subj in attrs[pred]:
                continue
            attrs[pred][subj] = t.get("object")
        return attrs

    @staticmethod
    def _trace_path(
        out_index: Dict[Tuple[str, str], List[str]],
        type_index: Dict[str, Set[str]],
        hop_attrs: Dict[str, Dict[str, Any]],
        members: Set[str],
        hops: List["CohortHop"],
    ) -> List[Dict[str, Any]]:
        """Instrumented twin of :meth:`_walk_path`.

        Per hop, records the inbound frontier size, the count of raw
        outbound neighbours seen, how many were dropped by the
        ``target_class`` type filter, how many by the ``where`` filter,
        and the resulting outbound frontier size. Used by
        :meth:`trace_paths` to power the Preview-tab diagnostic.
        """
        if not hops:
            return []
        out: List[Dict[str, Any]] = []
        frontier: Dict[str, Set[str]] = {m: {m} for m in members}
        for hop_idx, h in enumerate(hops):
            allowed: Optional[Set[str]] = None
            if h.target_class:
                bucket = type_index.get(h.target_class) or None
                allowed = bucket if bucket else None
            in_frontier = len(frontier)
            neighbours_raw = 0
            dropped_type = 0
            dropped_where = 0
            next_frontier: Dict[str, Set[str]] = {}
            for node, srcs in frontier.items():
                neighbours = out_index.get((node, h.via))
                if not neighbours:
                    continue
                for nbr in neighbours:
                    neighbours_raw += 1
                    if allowed is not None and nbr not in allowed:
                        dropped_type += 1
                        continue
                    if h.where and not _hop_where_passes(h.where, nbr, hop_attrs):
                        dropped_where += 1
                        continue
                    cell = next_frontier.get(nbr)
                    if cell is None:
                        next_frontier[nbr] = set(srcs)
                    else:
                        cell.update(srcs)
            out.append(
                {
                    "hop_idx": hop_idx,
                    "via": h.via,
                    "target_class": h.target_class,
                    "where_count": len(h.where or []),
                    "in_frontier": in_frontier,
                    "neighbours_raw": neighbours_raw,
                    "dropped_type": dropped_type,
                    "dropped_where": dropped_where,
                    "out_frontier": len(next_frontier),
                }
            )
            frontier = next_frontier
            if not frontier:
                # Pad remaining hops with all-zeros so the UI still has
                # one row per declared hop (makes "where did it die"
                # visually obvious).
                for k in range(hop_idx + 1, len(hops)):
                    rh = hops[k]
                    out.append(
                        {
                            "hop_idx": k,
                            "via": rh.via,
                            "target_class": rh.target_class,
                            "where_count": len(rh.where or []),
                            "in_frontier": 0,
                            "neighbours_raw": 0,
                            "dropped_type": 0,
                            "dropped_where": 0,
                            "out_frontier": 0,
                        }
                    )
                break
        return out

    @staticmethod
    def _count_edges_from_terminal_frontier(
        out_index: Dict[Tuple[str, str], List[str]],
        type_index: Dict[str, Set[str]],
        hop_attrs: Dict[str, Dict[str, Any]],
        members: Set[str],
        hops: List["CohortHop"],
    ) -> int:
        """Replay the (non-instrumented) walk to count emitted edges
        for one link. Cheap — O(triples)."""
        if not hops:
            return 0
        terminals = CohortBuilder._walk_path(
            out_index, type_index, hop_attrs, members, hops
        )
        edges: Set[Tuple[str, str]] = set()
        for srcs in terminals.values():
            if len(srcs) < 2:
                continue
            uniq = sorted(srcs)
            for i, a in enumerate(uniq):
                for b in uniq[i + 1 :]:
                    edges.add((a, b))
        return len(edges)

    @staticmethod
    def _walk_path(
        out_index: Dict[Tuple[str, str], List[str]],
        type_index: Dict[str, Set[str]],
        hop_attrs: Dict[str, Dict[str, Any]],
        members: Set[str],
        hops: List["CohortHop"],
    ) -> Dict[str, Set[str]]:
        """Walk *hops* starting from *members*; return ``terminal -> {members}``.

        Each hop traverses its ``via`` predicate, optionally restricts the
        next node to instances of ``target_class``, and optionally applies
        per-hop ``where`` filters to the neighbour's attribute values. The
        final frontier maps the shared-entity URI back to the set of
        source members that can reach it — that's exactly the
        togetherness relation.

        **Lenient type filtering.** When *target_class* is set but the
        graph has no ``rdf:type`` triples for it, the engine treats the
        filter as a no-op rather than blocking every neighbour. This
        matches the pre-multi-hop ``... or None`` semantics and keeps
        the engine usable on graphs that don't materialise types
        (e.g. data loaded from CSV, R2RML output without explicit
        type triples, or W3C-imported graphs that rely on rdfs:domain
        for typing).

        **Per-hop where.** Hop-level ``where`` filters are applied to the
        neighbour (the hop's *target* node), not the source — which lets
        users say *"… → ComplianceType where complianceTypeId =
        'Individual'"* and have the constraint scoped to the terminal
        node it actually describes, instead of bleeding into the rule's
        source-class compatibility (which would drop every source).
        """
        if not hops:
            return {}
        # frontier maps current_node_uri -> {source members able to reach it}
        frontier: Dict[str, Set[str]] = {m: {m} for m in members}
        for h in hops:
            allowed: Optional[Set[str]] = None
            if h.target_class:
                # An empty type set means "no instances of this class were
                # observed" — fall back to no filter, matching legacy
                # behaviour. A populated set narrows neighbours to it.
                bucket = type_index.get(h.target_class) or None
                allowed = bucket if bucket else None
            next_frontier: Dict[str, Set[str]] = {}
            for node, srcs in frontier.items():
                neighbours = out_index.get((node, h.via))
                if not neighbours:
                    continue
                for nbr in neighbours:
                    if allowed is not None and nbr not in allowed:
                        continue
                    if h.where and not _hop_where_passes(h.where, nbr, hop_attrs):
                        continue
                    cell = next_frontier.get(nbr)
                    if cell is None:
                        next_frontier[nbr] = set(srcs)
                    else:
                        cell.update(srcs)
            frontier = next_frontier
            if not frontier:
                break
        return frontier

    # ------------------------------------------------------------------
    # Stage 4 — edge filters
    # ------------------------------------------------------------------

    def _apply_edge_filters(
        self,
        edges: Set[Tuple[str, str]],
        attrs: Dict[str, Dict[str, Any]],
        compatibility: List[CohortCompat],
    ) -> Set[Tuple[str, str]]:
        same_value_props = [c for c in compatibility if c.type == "same_value"]
        if not same_value_props or not edges:
            return edges
        out: Set[Tuple[str, str]] = set()
        for a, b in edges:
            ok = True
            for cc in same_value_props:
                v1 = attrs.get(cc.property, {}).get(a)
                v2 = attrs.get(cc.property, {}).get(b)
                if v1 is None or v2 is None:
                    if cc.allow_missing:
                        continue
                    ok = False
                    break
                if v1 != v2:
                    ok = False
                    break
            if ok:
                out.add((a, b))
        return out

    # ------------------------------------------------------------------
    # Stage 5 — NetworkX
    # ------------------------------------------------------------------

    def _run_grouping(
        self,
        nodes: Set[str],
        edges: Set[Tuple[str, str]],
        rule: CohortRule,
    ) -> List[List[str]]:
        if not nodes:
            return []
        g = nx.Graph()
        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        if rule.group_type == "strict":
            return [sorted(c) for c in nx.find_cliques(g) if len(c) >= 2]
        return [sorted(c) for c in nx.connected_components(g)]

    # ------------------------------------------------------------------
    # Stage 6 — rank + hash
    # ------------------------------------------------------------------

    def _rank_and_hash(
        self, groups: List[List[str]], rule: CohortRule
    ) -> List[CohortGroup]:
        ranked = sorted(groups, key=lambda g: (-len(g), g[0] if g else ""))
        out: List[CohortGroup] = []
        for idx, members in enumerate(ranked, start=1):
            if len(members) < rule.min_size:
                continue
            sorted_members = sorted(members)
            joined = "|".join(sorted_members).encode("utf-8")
            content_hash = hashlib.sha256(joined).hexdigest()[:8]
            uri = CohortVocabulary.cohort(self._base_uri, rule.id, content_hash)
            out.append(
                CohortGroup(
                    id=uri,
                    idx=idx,
                    size=len(sorted_members),
                    members=sorted_members,
                )
            )
        return out

    # ------------------------------------------------------------------
    # Triple / row builders
    # ------------------------------------------------------------------

    def _build_cohort_triples(
        self, rule: CohortRule, result: CohortResult
    ) -> List[Dict[str, str]]:
        if not result.cohorts:
            return []
        klass = CohortVocabulary.cohort_class(self._base_uri)
        from_rule = CohortVocabulary.from_rule(self._base_uri)
        size_pred = CohortVocabulary.cohort_size(self._base_uri)
        in_cohort = CohortVocabulary.in_cohort(self._base_uri, rule.id)

        triples: List[Dict[str, str]] = []
        for c in result.cohorts:
            triples.append(
                {"subject": c.id, "predicate": RDF_TYPE, "object": klass}
            )
            triples.append(
                {
                    "subject": c.id,
                    "predicate": RDFS_LABEL,
                    "object": f"{rule.label} — cohort {c.idx}",
                }
            )
            triples.append(
                {"subject": c.id, "predicate": from_rule, "object": rule.id}
            )
            triples.append(
                {"subject": c.id, "predicate": size_pred, "object": str(c.size)}
            )
            for m in c.members:
                triples.append(
                    {"subject": m, "predicate": in_cohort, "object": c.id}
                )
        return triples

    def _build_uc_rows(
        self,
        rule: CohortRule,
        result: CohortResult,
        domain_name: str,
        domain_version: str,
        member_labels: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        if not result.cohorts:
            return []
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rows: List[Dict[str, Any]] = []
        for c in result.cohorts:
            cohort_short = c.id.rstrip("/").split("/")[-1] or c.id
            for m in c.members:
                rows.append(
                    {
                        "rule_id": rule.id,
                        "rule_label": rule.label,
                        "cohort_id": cohort_short,
                        "cohort_uri": c.id,
                        "cohort_idx": int(c.idx),
                        "cohort_size": int(c.size),
                        "member_uri": m,
                        "member_id": extract_local_name(m),
                        "member_label": member_labels.get(m, ""),
                        "domain_name": domain_name,
                        "domain_version": domain_version,
                        "materialised_at": now_iso,
                    }
                )
        return rows

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_triples(
        self,
        max_triples: Optional[int],
        allow_overflow: bool = False,
    ) -> List[Dict[str, str]]:
        cached = self._cache.get("triples")
        if cached is None:
            cached = self._store.query_triples(self._graph_name) if self._store else []
            self._cache["triples"] = cached
        if max_triples is not None and len(cached) > max_triples:
            if allow_overflow:
                logger.debug(
                    "CohortBuilder: triple count %d exceeds soft cap %d "
                    "but allow_overflow=True",
                    len(cached),
                    max_triples,
                )
                return cached
            raise ValueError(
                f"Triple count ({len(cached)}) exceeds max_triples "
                f"({max_triples}). Pick a more specific class or add "
                f"a filter constraint."
            )
        return cached


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _hop_where_passes(
    where: List[CohortCompat],
    node: str,
    hop_attrs: Dict[str, Dict[str, Any]],
) -> bool:
    """Return ``True`` when *node* satisfies all hop-level filters.

    Mirrors :func:`_node_filter_passes` semantics (``allow_missing``,
    ``value_equals`` / ``value_in`` / ``value_range``); ``same_value``
    is meaningless at hop scope (it's a pairwise edge constraint) and
    is silently ignored — already rejected at validation time.
    """
    for w in where:
        if w.type == "same_value":
            continue
        v = hop_attrs.get(w.property, {}).get(node)
        if v is None:
            if w.allow_missing:
                continue
            return False
        if not _node_filter_passes(w, v):
            return False
    return True


def _node_filter_passes(cc: CohortCompat, value: Any) -> bool:
    """Return ``True`` when *value* satisfies a single node filter."""
    if cc.type == "value_equals":
        return value == cc.value
    if cc.type == "value_in":
        return value in (cc.values or [])
    if cc.type == "value_range":
        try:
            num = float(value)
        except (TypeError, ValueError):
            return False
        if cc.min is not None and num < float(cc.min):
            return False
        if cc.max is not None and num > float(cc.max):
            return False
        return True
    return True


def _expected_repr(cc: CohortCompat) -> Any:
    if cc.type == "value_equals":
        return cc.value
    if cc.type == "value_in":
        return cc.values
    if cc.type == "value_range":
        return {"min": cc.min, "max": cc.max}
    return None


def _insert_rows_chunked(
    client: Any,
    fq: str,
    rows: List[Dict[str, Any]],
    chunk_size: int = 5_000,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Insert rows into a Delta table in chunks via ``execute_statement``.

    Mirrors the existing Delta materialisation pattern in
    ``ReasoningService.materialize_to_delta``.
    """
    cols = [
        "rule_id",
        "rule_label",
        "cohort_id",
        "cohort_uri",
        "cohort_idx",
        "cohort_size",
        "member_uri",
        "member_id",
        "member_label",
        "domain_name",
        "domain_version",
        "materialised_at",
    ]
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        values = []
        for r in chunk:
            values.append(
                "("
                + ", ".join(
                    [
                        f"'{sql_escape(str(r.get('rule_id', '')))}'",
                        f"'{sql_escape(str(r.get('rule_label', '')))}'",
                        f"'{sql_escape(str(r.get('cohort_id', '')))}'",
                        f"'{sql_escape(str(r.get('cohort_uri', '')))}'",
                        str(int(r.get("cohort_idx", 0))),
                        str(int(r.get("cohort_size", 0))),
                        f"'{sql_escape(str(r.get('member_uri', '')))}'",
                        f"'{sql_escape(str(r.get('member_id', '')))}'",
                        f"'{sql_escape(str(r.get('member_label', '')))}'",
                        f"'{sql_escape(str(r.get('domain_name', '')))}'",
                        f"'{sql_escape(str(r.get('domain_version', '')))}'",
                        f"TIMESTAMP '{sql_escape(str(r.get('materialised_at', '')))}'",
                    ]
                )
                + ")"
            )
        sql = (
            f"INSERT INTO {fq} ({', '.join(cols)}) VALUES\n"
            + ",\n".join(values)
        )
        client.execute_statement(sql)
        total += len(chunk)
        if on_progress:
            on_progress(total, len(rows))
    return total
