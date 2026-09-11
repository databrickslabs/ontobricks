"""MCP tool registration for the OntoBricks server.

``register_tools(mcp, session)`` binds all 13 ``@mcp.tool`` handlers to a
:class:`~server.session.MCPServerSession`. The handlers are thin: they enforce
the per-domain policy gate, issue the HTTP call through
:mod:`server.http_client` (late-bound for monkeypatching) and hand the JSON to
the :mod:`server.formatting` renderers.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import httpx
from fastmcp import Context, FastMCP

from server import http_client as _http
from server.constants import (
    API_V1_DOMAIN_CLASSES,
    API_V1_DOMAIN_DESIGN_STATUS,
    API_V1_DOMAIN_ONTOLOGY,
    API_V1_DOMAIN_VERSIONS,
    API_V1_DOMAINS,
    API_V1_DT_NODE_ACTION,
    API_V1_DT_NODE_CONTEXT,
    API_V1_DT_NODE_VIRTUAL_ATTRIBUTES,
    API_V1_DT_STATS,
    API_V1_DT_STATUS,
    API_V1_DT_TRIPLES_FIND,
    MAX_DEPTH,
)
from server.formatting import (
    _format_find_response,
    _format_graphql_response,
    _format_node_action_response,
    _format_node_context_response,
    _format_virtual_attributes_response,
)
from server.session import MCPServerSession
from server.uri_helpers import _local_name, _pretty_predicate

logger = logging.getLogger(__name__)


def register_tools(mcp: FastMCP, session: MCPServerSession) -> None:
    """Register every OntoBricks MCP tool on *mcp*, bound to *session*."""

    # ── Tools — Domain selection ──────────────────────────────────────

    @mcp.tool()
    async def list_domains() -> str:
        """List all domains (knowledge graphs) available in the registry.

        Returns each domain's name and description so you can choose
        the right one for the user's question.

        Always call this first before any other tool.
        """
        logger.info("Tool list_domains called")
        await session.ensure_registry()
        params = session.registry_params()
        logger.info(
            "list_domains → calling %s%s with override params=%s",
            session.base,
            API_V1_DOMAINS,
            params,
        )

        async with session.client() as client:
            data = await _http._get(client, API_V1_DOMAINS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not retrieve domains.")

        domains = data.get("domains", [])
        if not domains:
            return "No domains found in the registry."

        session.domain_policy.clear()
        session.domain_has_graph.clear()
        for d in domains:
            if d.get("name"):
                session.domain_policy[d["name"]] = d.get("mcp_policy") or {}
                session.domain_has_graph[d["name"]] = bool(d.get("has_graph", True))

        lines: list[str] = []
        lines.append(f"Available Domains ({len(domains)})")
        lines.append("=" * 40)
        for d in domains:
            name = d.get("name", "")
            desc = d.get("description", "")
            tag = "" if d.get("has_graph", True) else "  (ontology-only)"
            lines.append(f"  • {name}{tag}")
            if desc:
                lines.append(f"    {desc}")
        lines.append("")

        current = session.selected_domain_name
        if current:
            lines.append(f"Currently selected: {current}")
        else:
            lines.append("No domain selected yet — call select_domain(<name>) next.")

        return "\n".join(lines)

    @mcp.tool()
    async def list_domain_versions(domain_name: str) -> str:
        """List registry versions for a domain (latest first).

        Uses ``GET /api/v1/domain/versions``. Call after ``list_domains``
        to see which versions exist before selecting or building.

        Args:
            domain_name: Exact domain name as returned by ``list_domains``.
        """
        await session.ensure_registry()
        params = session.registry_params()
        params["domain_name"] = domain_name

        async with session.client() as client:
            data = await _http._get(client, API_V1_DOMAIN_VERSIONS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not list versions.")

        versions = data.get("versions", [])
        latest = data.get("latest_version", "")
        if not versions:
            return f"No versions returned for '{domain_name}'."

        lines = [
            f"Versions — {domain_name}",
            "=" * 40,
            f"Latest: {latest}",
            "",
        ]
        for v in versions:
            ver = v.get("version", "")
            tag = " (latest)" if v.get("is_latest") else ""
            lines.append(f"  • {ver}{tag}")
        return "\n".join(lines)

    @mcp.tool()
    async def get_design_status(domain_name: Optional[str] = None) -> str:
        """Design pipeline readiness: ontology, metadata, assignment, build_ready.

        Uses ``GET /api/v1/domain/design-status``. If ``domain_name`` is
        omitted, uses the currently selected domain (after ``select_domain``).

        Args:
            domain_name: Registry domain name, or omit to use selected domain.
        """
        await session.ensure_registry()
        name = domain_name or session.selected_domain_name
        if not name:
            return (
                "Provide domain_name or call select_domain first "
                "to set the active domain."
            )
        params = session.registry_params()
        params["domain_name"] = name

        async with session.client() as client:
            data = await _http._get(client, API_V1_DOMAIN_DESIGN_STATUS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not load design status.")

        lines = [f"Design status — {name}", "=" * 40]

        ont = data.get("ontology") or {}
        if ont:
            lines.append(
                f"Ontology:  ready={ont.get('ready', False)} "
                f"has_owl={ont.get('has_owl', False)} "
                f"classes={ont.get('class_count', 0)} "
                f"props={ont.get('property_count', 0)}"
            )

        meta = data.get("metadata") or {}
        if meta:
            lines.append(
                f"Metadata:  tables={meta.get('table_count', 0)} "
                f"ready={meta.get('ready', False)}"
            )

        asn = data.get("assignment") or {}
        if asn:
            lines.append(
                f"Assignment: progress={asn.get('progress_percent', 0)}% "
                f"status={asn.get('status', 'n/a')} "
                f"has_r2rml={asn.get('has_r2rml', False)}"
            )

        lines.append(f"build_ready: {data.get('build_ready', False)}")
        if data.get("message"):
            lines.append(f"Note: {data['message']}")

        return "\n".join(lines)

    @mcp.tool()
    async def select_domain(domain_name: str, ctx: Context) -> str:
        """Select a domain (knowledge graph) to work with.

        After calling ``list_domains`` to see what is available, call
        this tool with the exact domain name. All subsequent calls to
        ``list_entity_types``, ``describe_entity``, and ``get_status``
        will operate on this domain's Knowledge Graph.

        Each domain publishes its own set of tools, so the tool list is
        recomputed here: tools the domain does not expose disappear for this
        session, and previously hidden ones come back.

        Args:
            domain_name: Exact domain name as shown by ``list_domains``.
        """
        await session.ensure_registry()
        await session.ensure_domain_policies()

        params = session.registry_params()
        params["domain_name"] = domain_name

        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_STATUS, params=params)
            if not data.get("success") and data.get("message"):
                return f"Error selecting domain: {data['message']}"
            session.selected_domain_name = domain_name
            # Ontology labels are resolved lazily via ``label_or_local``
            # (local-name fallback). A dedicated read-only label endpoint
            # can repopulate ``ontology_labels`` here in the future; the
            # previous eager POST hit the legacy UC handler (wrong
            # contract) and only added a wasted round trip.
            session.ontology_labels.clear()
            session.class_actions.clear()
            # Fetch class Actions for the selected domain — reuse the same client
            try:
                cls_data = await _http._get(
                    client,
                    API_V1_DOMAIN_CLASSES,
                    params={**session.registry_params(), "domain_name": domain_name},
                )
                for cls in cls_data.get("classes", []):
                    uri = cls.get("uri", "")
                    if uri:
                        session.class_actions[uri] = {
                            "name": cls.get("name", ""),
                            "dataset": cls.get("dataset") or None,
                            "bridges": cls.get("bridges") or [],
                            "actions": cls.get("actions") or [],
                            "virtualAttributes": cls.get("virtualAttributes") or [],
                        }
                logger.info(
                    "select_domain: loaded class Actions for %d classes",
                    len(session.class_actions),
                )
            except Exception as exc:
                logger.warning("select_domain: could not load class Actions: %s", exc)

        # Recompute the session tool set for the new domain. reset_visibility
        # first, otherwise rules accumulate and a tool hidden by a previously
        # selected domain would stay hidden here. An ontology-only domain (no
        # built graph) additionally hides every GRAPH_TOOLS entry, leaving
        # describe_ontology as its sole domain tool.
        hidden = session.hidden_for(domain_name)
        try:
            await ctx.reset_visibility()
            if hidden:
                await ctx.disable_components(names=hidden, components={"tool"})
        except Exception as exc:  # noqa: BLE001
            # Visibility is a presentation concern; ensure_tool_allowed still
            # refuses the calls, so a failure here must not break selection.
            logger.warning("select_domain: could not apply tool visibility: %s", exc)

        has_data = data.get("has_data", False)
        count = data.get("count", 0)
        graph_name = data.get("graph_name", "N/A")
        view_table = data.get("view_table", "N/A")

        lines = [
            f"Domain '{domain_name}' selected.",
            f"View:  {view_table}",
            f"Graph: {graph_name}",
            f"Data:  {'Yes' if has_data else 'No'} ({count:,} triples)",
            "",
        ]
        if hidden:
            lines.append(
                "Not available for this domain: " + ", ".join(sorted(hidden))
            )
            lines.append("")
        if not session.has_graph(domain_name):
            lines.append(
                "This domain is published with an ontology only (no graph). "
                "Use describe_ontology to read its structure."
            )
        else:
            lines.append(
                "You can now use list_entity_types and describe_entity."
            )
        return "\n".join(lines)

    # ── Tools — Ontology structure ────────────────────────────────────

    @mcp.tool()
    async def describe_ontology(domain_name: Optional[str] = None) -> str:
        """Describe the selected domain's ontology structure.

        Returns a structured summary (classes and their attachments) plus the
        raw OWL/Turtle document, which carries the full class, attribute and
        relationship (domain/range) detail. This is the ground truth for the
        ontology *schema* — it does not touch the graph store, so it works even
        for an ontology-only domain that has never been built. For such a
        domain this is the only tool available.

        Args:
            domain_name: Registry domain name, or omit to use the selected one.
        """
        name = domain_name or session.selected_domain_name
        if not name:
            return (
                "No domain selected. Call list_domains first, then "
                "select_domain to choose one (or pass domain_name)."
            )

        await session.ensure_registry()
        await session.ensure_domain_policies()
        # describe_ontology is never gated by has_graph (it is the fallback for
        # ontology-only domains), but a domain can still hide it via policy.
        if "describe_ontology" in session.disabled_tools(
            session.domain_policy.get(name, {})
        ):
            return (
                f"The tool 'describe_ontology' is not available for domain "
                f"'{name}' — its MCP policy does not expose it."
            )
        params = {**session.registry_params(), "domain_name": name}

        async with session.client() as client:
            owl_data = await _http._get(client, API_V1_DOMAIN_ONTOLOGY, params=params)
            classes_data = await _http._get(
                client, API_V1_DOMAIN_CLASSES, params=params
            )

        if not owl_data.get("success"):
            return owl_data.get("message", "Could not load the ontology.")

        base_uri = owl_data.get("base_uri") or "N/A"
        class_count = owl_data.get("class_count", 0)
        property_count = owl_data.get("property_count", 0)
        owl_content = owl_data.get("content", "")

        lines: list[str] = [
            f"Ontology — {name}",
            "=" * 50,
            f"Base URI:   {base_uri}",
            f"Classes:    {class_count}",
            f"Properties: {property_count}",
            "",
        ]

        classes = classes_data.get("classes", []) if isinstance(classes_data, dict) else []
        if classes:
            lines.append("Classes")
            lines.append("-" * 50)
            for cls in classes:
                cls_name = cls.get("name") or _local_name(cls.get("uri", ""))
                tags: list[str] = []
                if cls.get("dataset"):
                    tags.append("dataset")
                if cls.get("bridges"):
                    tags.append(f"{len(cls['bridges'])} bridge(s)")
                if cls.get("actions"):
                    tags.append(f"{len(cls['actions'])} action(s)")
                if cls.get("virtualAttributes"):
                    tags.append(f"{len(cls['virtualAttributes'])} virtual attr")
                suffix = f"  [{', '.join(tags)}]" if tags else ""
                lines.append(f"  • {cls_name}{suffix}")
            lines.append("")

        if owl_content:
            lines.append("OWL (Turtle)")
            lines.append("-" * 50)
            lines.append(owl_content)
        else:
            lines.append("(OWL document unavailable.)")

        return "\n".join(lines)

    # ── Tools — Knowledge graph queries ───────────────────────────────

    @mcp.tool()
    async def list_entity_types() -> str:
        """List all entity types available in the selected domain's knowledge graph.

        Returns a readable summary of every entity type (rdf:type) present
        in the triple store together with instance counts, plus overall
        statistics (total triples, distinct subjects, etc.).

        When a type has a linked external dataset in the ontology, also
        includes the dataset full name and Description.

        A domain must be selected first via ``select_domain``.
        """
        blocked = session.require_domain("list_entity_types")
        if blocked:
            return blocked

        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_STATS, params=session.domain_params())

        if not data.get("success"):
            return data.get("message", "Could not retrieve statistics.")

        lines: list[str] = []
        lines.append(f"Graph Viewer — {session.selected_domain_name}")
        lines.append("=" * 40)
        inferred = data.get("inferred_triples", 0)
        lines.append(f"Total triples:       {data.get('total_triples', 0):,}")
        lines.append(f"Distinct entities:   {data.get('distinct_subjects', 0):,}")
        lines.append(f"Distinct predicates: {data.get('distinct_predicates', 0):,}")
        lines.append(f"Labels:              {data.get('label_count', 0):,}")
        lines.append(f"Type assertions:     {data.get('type_assertion_count', 0):,}")
        lines.append(f"Relationships:       {data.get('relationship_count', 0):,}")
        if inferred > 0:
            lines.append(
                f"Inferred triples:    {inferred:,}  "
                f"[reasoning output — ONLY visible via describe_entity, "
                f"NOT via query_graphql]"
            )
        lines.append("")

        entity_types = data.get("entity_types", [])
        if entity_types:
            lines.append("Entity Types")
            lines.append("-" * 40)
            for et in entity_types:
                uri = et.get("uri", "")
                count = et.get("count", 0)
                name = session.label_or_local(uri)
                lines.append(f"  • {name}  ({count:,} instances)")
                lines.append(f"    URI: {uri}")
                actions = session.class_actions.get(uri) or {}
                dataset = actions.get("dataset") or {}
                if dataset.get("fullName"):
                    lines.append(f"    Dataset: {dataset['fullName']}")
                    desc = (dataset.get("description") or "").strip()
                    if desc:
                        lines.append(f"    Description: {desc}")
                for fn_action in actions.get("actions") or []:
                    fn_desc = (fn_action.get("description") or "").strip()
                    suffix = f" — {fn_desc}" if fn_desc else ""
                    lines.append(f"    Action: {fn_action.get('fullName', '')}{suffix}")
            lines.append("")

        top_predicates = data.get("top_predicates", [])
        if top_predicates:
            lines.append("Predicates (attributes & relationships)")
            lines.append("-" * 40)
            for tp in top_predicates:
                uri = tp.get("uri", "")
                count = tp.get("count", 0)
                name = session.label_or_local(uri) or _pretty_predicate(uri)
                lines.append(f"  • {name}  ({count:,} usages)")

        return "\n".join(lines)

    @mcp.tool()
    async def describe_entity(
        search: Optional[str] = None,
        entity_type: Optional[str] = None,
        depth: int = MAX_DEPTH,
    ) -> str:
        """Search for an entity and return a full-text description.

        Queries the RAW TRIPLE STORE (union of synced data AND
        inferred/materialised triples added by reasoning). This is the
        GROUND TRUTH tool — it returns ALL triples regardless of the
        ontology schema, including relationships added by inference that
        are not declared as ontology predicates.

        Finds entities matching the search text and/or type in the
        selected domain's knowledge graph, then traverses their
        relationships hop-by-hop and returns a human-readable description
        including:
          - Entity identity (name, type, URI)
          - All attributes (e.g. email, phone, city …)
          - All relationships to other entities, including inferred ones
          - Related entities discovered at each traversal depth
          - Linked external dataset name, key column, and description when
            configured on the ontology class

        Use this as the PRIMARY tool for any question about a specific
        entity. Do NOT rely on ``query_graphql`` alone — it may miss
        inferred/materialised relationships.

        A domain must be selected first via ``select_domain``.
        At least one of ``search`` or ``entity_type`` must be provided.

        Args:
            search: Text to search for in entity names / labels / URIs.
                Example: ``"Jacob Martinez"``, ``"CUST00094"``.
            entity_type: Entity type to filter by (local name,
                case-insensitive). Example: ``"Customer"``, ``"Order"``.
            depth: How many hops to traverse (1 = direct neighbors only,
                default 1, max 10).

        Returns:
            A full-text description of the matching entities, their
            attributes, and their relationships, organized hop by hop.
        """
        blocked = session.require_domain("describe_entity")
        if blocked:
            return blocked
        if not search and not entity_type:
            return "Please provide at least a search term or an entity type."

        params = session.domain_params(
            {
                "depth": min(max(depth, 1), 10),
                # Keep the LLM payload tight: 100 triples is plenty to
                # describe an entity + its immediate neighbours, and cuts
                # both backend fetch size and token cost. The backend still
                # reports ``total`` so the model can page for more.
                "limit": 100,
                "offset": 0,
            }
        )
        if search:
            params["search"] = search
        if entity_type:
            params["entity_type"] = entity_type

        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_TRIPLES_FIND, params=params)

        return _format_find_response(
            data,
            session.label_or_local,
            class_actions=session.class_actions,
            context_policy=session.active_context_policy(),
        )

    @mcp.tool()
    async def get_status() -> str:
        """Check whether the selected domain's knowledge graph is ready.

        Returns view name, graph name, whether data exists, and row count.
        Call this if other tools report errors to diagnose configuration issues.

        A domain must be selected first via ``select_domain``.
        """
        blocked = session.require_domain("get_status")
        if blocked:
            return blocked

        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_STATUS, params=session.domain_params())
        status = data.get("reason") or "OK"
        has_data = data.get("has_data", False)
        count = data.get("count", 0)
        graph_name = data.get("graph_name", "N/A")
        view_table = data.get("view_table", "N/A")
        return (
            f"Domain: {session.selected_domain_name}\n"
            f"View:    {view_table}\n"
            f"Graph:   {graph_name}\n"
            f"Status:  {status}\n"
            f"Data:    {'Yes' if has_data else 'No'} ({count:,} triples)"
        )

    # ── Tools — GraphQL queries ─────────────────────────────────────

    @mcp.tool()
    async def get_graphql_schema() -> str:
        """Get the GraphQL schema (SDL) for the selected domain.

        Returns the auto-generated schema in Schema Definition Language
        format, showing all available types, fields, and relationships.
        Use this to understand what data you can query before calling
        ``query_graphql``.

        A domain must be selected first via ``select_domain``.
        """
        blocked = session.require_domain("get_graphql_schema")
        if blocked:
            return blocked

        domain_name = session.selected_domain_name
        try:
            async with session.client() as client:
                resp = await client.get(
                    f"/graphql/{domain_name}/schema",
                    params=session.registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "GraphQL schema request failed: %s %s", exc.response.status_code, exc
            )
            body_text = exc.response.text[:500]
            return f"Could not retrieve GraphQL schema ({exc.response.status_code}): {body_text}"
        except Exception as exc:
            logger.warning("GraphQL schema request error: %s", exc)
            return f"Error fetching GraphQL schema: {exc}"

        sdl = data.get("sdl", "")
        if not sdl:
            return "GraphQL schema is empty — the domain may have no ontology classes."

        lines: list[str] = []
        lines.append(f"GraphQL Schema — {domain_name}")
        lines.append("=" * 50)
        lines.append("")
        lines.append(sdl)
        lines.append("")
        lines.append("Use query_graphql to execute queries against this schema.")
        return "\n".join(lines)

    @mcp.tool()
    async def query_graphql(
        query: str,
        variables: Optional[str] = None,
    ) -> str:
        """Execute a GraphQL query against the selected domain's knowledge graph.

        Reads the graph store through the ONTOLOGY SCHEMA layer.
        WARNING: only predicates declared in the ontology appear as
        GraphQL fields. Inferred/materialised triples whose predicate is
        NOT in the ontology schema are silently invisible here.
        Use ``describe_entity`` when you need to see ALL relationships
        including inferred ones.

        The schema is auto-generated from the domain's ontology.
        Call ``get_graphql_schema`` first to discover available types
        and fields.

        This tool is ideal for:
          - Bulk typed look-ups where the schema covers the data you need
          - Fetching specific fields (no over-fetching)
          - Nested relationship traversal in a single request
          - Filtering and pagination (``limit``, ``offset``, ``search``)

        A domain must be selected first via ``select_domain``.

        Args:
            query: A valid GraphQL query string.
                Example: ``{ allCustomer(limit: 5) { id label email } }``
            variables: Optional JSON string of query variables.
                Example: ``{"limit": 10}``

        Returns:
            The query result as formatted text, or an error message.
        """
        blocked = session.require_domain("query_graphql")
        if blocked:
            return blocked

        domain_name = session.selected_domain_name

        body: dict = {"query": query}
        if variables:
            try:
                body["variables"] = json.loads(variables)
            except json.JSONDecodeError:
                return "Invalid JSON in 'variables' parameter."

        try:
            async with session.client() as client:
                resp = await client.post(
                    f"/graphql/{domain_name}",
                    json=body,
                    params=session.registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("GraphQL query failed: %s %s", exc.response.status_code, exc)
            body_text = exc.response.text[:500]
            return f"GraphQL query failed ({exc.response.status_code}): {body_text}"
        except Exception as exc:
            logger.warning("GraphQL query error: %s", exc)
            return f"Error executing GraphQL query: {exc}"

        return _format_graphql_response(data, domain_name)

    @mcp.tool()
    async def get_entity_context(
        entity_uri: str,
        fetch_dataset_rows: bool = False,
        dataset_row_limit: int = 5,
        follow_bridges: bool = False,
        compute_virtual_attributes: bool = False,
    ) -> str:
        """Return complete context for an entity node: linked dataset rows,
        cross-domain bridge entities and/or computed virtual attributes.

        Requires a domain to be selected first via select_domain.
        The class must have dataset / bridges configured in the ontology.
        Use the entity URI from describe_entity output.

        When a dataset is linked, the response includes its full name, key
        column, and ontology-authored Description (from the class dataset
        ``description`` field).

        Bridges include the target domain's description so an interrogating
        agent can decide whether to hop. To actually run queries in the
        target domain, call ``select_domain(<target_domain>)`` and then
        ``describe_entity`` / GraphQL there — ``follow_bridges=True`` only
        peeks at the target graph and does NOT switch the selected domain.
        Bridges whose target is not exposed on MCP are omitted from the
        response.

        Virtual attributes are class attributes that are not stored in the
        graph: a Unity Catalog function computes them on demand. They are
        always *listed* so you know what is available, but their values cost a
        warehouse round-trip. When the user asks for a virtual attribute's
        value, call ``compute_virtual_attributes(entity_uri)`` — not this tool
        alone. The ``compute_virtual_attributes=True`` flag here remains for
        callers that want declarations and values in one response.

        Args:
            entity_uri: Full URI of the entity (e.g. from describe_entity).
            fetch_dataset_rows: If true, query the linked UC table/view for rows.
            dataset_row_limit: Max rows to return (1–20, default 5).
            follow_bridges: If true, peek at bridge target domains (read-only —
                does NOT change the selected domain). Prefer ``select_domain``
                for a real hop.
            compute_virtual_attributes: If true, run the class's virtual
                attribute functions and return their values.
        """
        blocked = session.require_domain("get_entity_context")
        if blocked:
            return blocked

        # Refuse the argument rather than silently returning nothing, so the
        # model learns the element is off instead of retrying the same call.
        if fetch_dataset_rows:
            blocked = session.ensure_context_allowed("dataset", "Datasets")
            if blocked:
                return blocked
        if follow_bridges:
            blocked = session.ensure_context_allowed("bridges", "Bridges")
            if blocked:
                return blocked
        if compute_virtual_attributes:
            blocked = session.ensure_context_allowed(
                "virtual_attributes", "Virtual attributes"
            )
            if blocked:
                return blocked

        params = session.domain_params(
            {
                "entity_uri": entity_uri,
                "fetch_dataset_rows": str(fetch_dataset_rows).lower(),
                "dataset_row_limit": min(max(dataset_row_limit, 1), 20),
                "follow_bridges": str(follow_bridges).lower(),
                "compute_virtual_attributes": str(
                    compute_virtual_attributes
                ).lower(),
            }
        )

        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_NODE_CONTEXT, params=params)

        return _format_node_context_response(data, session.active_context_policy())

    @mcp.tool()
    async def compute_virtual_attributes(
        entity_uri: str,
        function: Optional[str] = None,
    ) -> str:
        """Compute one or all virtual attributes declared on an entity's class.

        Requires a domain to be selected first via select_domain. Use this
        tool whenever the user asks about the **value** of a virtual attribute
        — risk score, live standing, distance, and so on. Those values are not
        stored in the knowledge graph; only a Unity Catalog function can
        produce them.

        Discover what is available with ``describe_entity`` or
        ``get_entity_context`` (declarations only). Then call this tool to run
        the bound UC function and read the result. Only functions declared on
        the entity's ontology class can be invoked.

        The function receives exactly one argument server-side: the entity's
        local ID, derived from *entity_uri*. Omit *function* to compute every
        group on the class; pass a fully qualified name
        (``catalog.schema.function``) to compute one group only.

        Args:
            entity_uri: Full URI of the entity (e.g. from describe_entity).
            function: Optional fully qualified UC function name. When omitted,
                every virtual attribute group declared on the class is computed.
        """
        blocked = session.require_domain("compute_virtual_attributes")
        if blocked:
            return blocked
        blocked = session.ensure_context_allowed(
            "virtual_attributes", "Virtual attributes"
        )
        if blocked:
            return blocked

        params = session.domain_params({"entity_uri": entity_uri})
        if function:
            params["function"] = function

        try:
            async with session.client() as client:
                data = await _http._get(
                    client, API_V1_DT_NODE_VIRTUAL_ATTRIBUTES, params=params
                )
        except httpx.HTTPStatusError as exc:
            try:
                err_body = exc.response.json()
            except Exception:
                err_body = {}
            return (
                err_body.get("message")
                or err_body.get("error")
                or (
                    f"Could not compute virtual attributes "
                    f"(HTTP {exc.response.status_code})."
                )
            )

        return _format_virtual_attributes_response(data, session.active_context_policy())

    @mcp.tool()
    async def invoke_entity_action(entity_uri: str, action: str) -> str:
        """Run a Unity Catalog function action configured on an entity's class.

        Requires a domain to be selected first via select_domain. Discover the
        available actions with get_entity_context or describe_entity — only
        functions declared on the entity's ontology class can be invoked.

        The function is called with exactly one argument: the entity's local ID,
        derived server-side from *entity_uri*.

        Args:
            entity_uri: Full URI of the entity (e.g. from describe_entity).
            action: Fully qualified function name (catalog.schema.function).
        """
        blocked = session.require_domain("invoke_entity_action")
        if blocked:
            return blocked
        # Disabling the actions element also stops invocation, even when the
        # tool itself is still exposed.
        blocked = session.ensure_context_allowed("actions", "Actions")
        if blocked:
            return blocked

        body: dict = {"entity_uri": entity_uri, "action_full_name": action}
        body.update(session.registry_params())
        body["domain_name"] = session.selected_domain_name

        try:
            async with session.client() as client:
                data = await _http._post(client, API_V1_DT_NODE_ACTION, json=body)
        except httpx.HTTPStatusError as exc:
            try:
                err_body = exc.response.json()
            except Exception:
                err_body = {}
            return (
                err_body.get("message")
                or err_body.get("error")
                or f"Could not invoke the action (HTTP {exc.response.status_code})."
            )

        return _format_node_action_response(data)
