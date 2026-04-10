"""Translate SWRL rules to Cypher for LadybugDB graph-model backends.

Uses typed node/relationship tables from the graph schema.
"""
from typing import Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.SWRLParser import SWRLParser
from back.core.reasoning.SWRLBuiltinRegistry import SWRLBuiltinRegistry
from back.core.reasoning.constants import RDF_TYPE

logger = get_logger(__name__)


class SWRLCypherTranslator:
    """Build Cypher queries from SWRL rules for LadybugDB."""

    def __init__(self, graph_schema=None) -> None:
        self._schema = graph_schema

    @staticmethod
    def _resolve_arg_cypher(token: str, var_subject_alias: Dict[str, str]) -> str:
        """Resolve a SWRL argument to a Cypher expression."""
        if not SWRLBuiltinRegistry.is_literal(token):
            stripped = token.lstrip("?")
            ref = var_subject_alias.get(token) or var_subject_alias.get(stripped)
            if ref:
                return ref
            return stripped
        return SWRLBuiltinRegistry.literal_cypher(token)

    @staticmethod
    def _build_cypher_builtin_filters(
        builtin_atoms: List[Dict], var_alias: Dict[str, str],
    ) -> List[str]:
        """Translate built-in atoms to Cypher WHERE fragments."""
        filters: List[str] = []
        for atom in builtin_atoms:
            bi = SWRLBuiltinRegistry.get(atom["name"])
            if bi is None:
                continue
            resolved = [
                SWRLCypherTranslator._resolve_arg_cypher(a, var_alias)
                for a in atom["args"]
            ]
            if bi.category == "comparison" and bi.arity == 2:
                filters.append(bi.cypher_template.format(*resolved[:2]))
            elif bi.category == "string" and bi.arity == 2:
                filters.append(bi.cypher_template.format(*resolved[:2]))
            elif bi.category == "math" and bi.arity == 3:
                expr = bi.cypher_template.format(*resolved[:2])
                result_ref = SWRLCypherTranslator._resolve_arg_cypher(
                    atom["args"][2], var_alias
                )
                filters.append(f"{result_ref} = ({expr})")
            elif bi.category == "date" and bi.arity == 2:
                filters.append(bi.cypher_template.format(*resolved[:2]))
            elif bi.arity <= len(resolved):
                filters.append(bi.cypher_template.format(*resolved[:bi.arity]))
        return filters

    def _resolve_node_table(self, class_name: str, base_uri: str,
                            uri_map: Optional[Dict] = None) -> str:
        """Map a SWRL class name to a graph-schema node table name."""
        if self._schema is None:
            from back.core.triplestore.ladybugdb.GraphSchema import GraphSchema
            return GraphSchema.safe_identifier(class_name)

        uri = SWRLParser.resolve_uri(class_name, base_uri, uri_map)
        tbl = self._schema.class_uri_to_table.get(uri)
        if tbl:
            return tbl
        from back.core.triplestore.ladybugdb.GraphSchema import GraphSchema
        safe = GraphSchema.safe_identifier(class_name)
        if safe in self._schema.node_tables:
            return safe
        return safe

    def _resolve_rel_table(self, prop_name: str, base_uri: str,
                           uri_map: Optional[Dict] = None) -> str:
        """Map a SWRL property name to a graph-schema relationship table."""
        if self._schema is None:
            from back.core.triplestore.ladybugdb.GraphSchema import GraphSchema
            return GraphSchema.safe_identifier(prop_name)

        uri = SWRLParser.resolve_uri(prop_name, base_uri, uri_map)
        tbl = self._schema.property_uri_to_table.get(uri)
        if tbl:
            return tbl
        from back.core.triplestore.ladybugdb.GraphSchema import GraphSchema
        safe = GraphSchema.safe_identifier(prop_name)
        if safe in self._schema.rel_tables:
            return safe
        return safe

    def build_violation_query(self, params: Dict) -> Optional[str]:
        """Build Cypher that returns subjects violating a SWRL rule."""
        antecedent = params.get("antecedent", "")
        consequent = params.get("consequent", "")
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [a for a in ante_atoms
                       if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")]
        prop_atoms = [a for a in ante_atoms
                      if a["arity"] == 2 and not a.get("builtin") and not a.get("negated")]
        builtin_atoms = [a for a in ante_atoms if a.get("builtin") and not a.get("negated")]
        negated_atoms = [a for a in ante_atoms if a.get("negated")]
        if not class_atoms:
            return None

        var_tables: Dict[str, str] = {}
        for a in class_atoms:
            var = a["args"][0]
            var_tables[var] = self._resolve_node_table(a["name"], base_uri, uri_map)

        violation_var = SWRLParser.determine_violation_subject(cons_atoms, class_atoms)
        if not violation_var or violation_var not in var_tables:
            return None

        connected = SWRLParser.find_connected_vars(violation_var, prop_atoms)

        match_parts: List[str] = []
        var_cypher_map: Dict[str, str] = {}
        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                continue
            cypher = self._var_name(var)
            var_cypher_map[var] = f"{cypher}.uri"
            match_parts.append(f"MATCH ({cypher}:{var_tables[var]})")

        for prop in prop_atoms:
            subj_var = prop["args"][0]
            obj_var = prop["args"][1]
            if subj_var not in connected or obj_var not in connected:
                continue
            rel_table = self._resolve_rel_table(prop["name"], base_uri, uri_map)
            s_cypher = self._var_name(subj_var)
            o_cypher = self._var_name(obj_var)
            s_label = f":{var_tables[subj_var]}" if subj_var in var_tables else ""
            o_label = f":{var_tables[obj_var]}" if obj_var in var_tables else ""
            if obj_var not in var_cypher_map:
                var_cypher_map[obj_var] = f"{o_cypher}.uri"
            match_parts.append(
                f"MATCH ({s_cypher}{s_label})-[:{rel_table}]->({o_cypher}{o_label})"
            )

        disconnected_tables: Dict[str, str] = {}
        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected and var in var_tables:
                disconnected_tables[var] = var_tables[var]

        where_clauses: List[str] = []

        builtin_filters = SWRLCypherTranslator._build_cypher_builtin_filters(
            builtin_atoms, var_cypher_map
        )
        where_clauses.extend(builtin_filters)

        for neg in negated_atoms:
            if neg.get("builtin"):
                continue
            if neg["arity"] == 2:
                var_s = neg["args"][0]
                var_o = neg["args"][1]
                rel_table = self._resolve_rel_table(neg["name"], base_uri, uri_map)
                vs = self._var_name(var_s)
                if var_o in var_tables:
                    o_label = f":{var_tables[var_o]}"
                    where_clauses.append(
                        f"NOT EXISTS {{ MATCH ({vs})-[:{rel_table}]->(:{o_label.lstrip(':')}) }}"
                    )
                else:
                    where_clauses.append(
                        f"NOT EXISTS {{ MATCH ({vs})-[:{rel_table}]->() }}"
                    )
            elif neg["arity"] == 1:
                var = neg["args"][0]
                c_table = self._resolve_node_table(neg["name"], base_uri, uri_map)
                v = self._var_name(var)
                where_clauses.append(
                    f"NOT EXISTS {{ MATCH ({v})-[:rdf_type]->(:{{uri: '{c_table}'}}) }}"
                )

        for atom in cons_atoms:
            if atom["arity"] == 1:
                var = atom["args"][0]
                c_table = self._resolve_node_table(atom["name"], base_uri, uri_map)
                v = self._var_name(var)
                where_clauses.append(
                    f"NOT EXISTS {{ MATCH ({v})-[:rdf_type]->(:{{uri: '{c_table}'}}) }}"
                )
            elif atom["arity"] == 2:
                var_s = atom["args"][0]
                var_o = atom["args"][1]
                rel_table = self._resolve_rel_table(atom["name"], base_uri, uri_map)
                vs = self._var_name(var_s)
                if var_o in connected:
                    vo = self._var_name(var_o)
                    o_label = f":{var_tables[var_o]}" if var_o in var_tables else ""
                    where_clauses.append(
                        f"NOT EXISTS {{ MATCH ({vs})-[:{rel_table}]->({vo}_c{o_label}) }}"
                    )
                elif var_o in disconnected_tables:
                    o_table = disconnected_tables[var_o]
                    where_clauses.append(
                        f"NOT EXISTS {{ MATCH ({vs})-[:{rel_table}]->(:{o_table}) }}"
                    )
                else:
                    where_clauses.append(
                        f"NOT EXISTS {{ MATCH ({vs})-[:{rel_table}]->() }}"
                    )

        if not where_clauses:
            return None

        viol_cypher = self._var_name(violation_var)
        cypher_lines = match_parts.copy()
        cypher_lines.append("WHERE " + " OR ".join(where_clauses))
        cypher_lines.append(f"RETURN DISTINCT {viol_cypher}.uri AS s")

        query = "\n".join(cypher_lines)
        logger.debug("SWRL Cypher [%s -> %s]:\n%s", antecedent, consequent, query)
        return query

    def build_materialization_query(self, params: Dict) -> Optional[str]:
        """Build Cypher CREATE for inferred consequent triples."""
        antecedent = params.get("antecedent", "")
        consequent = params.get("consequent", "")
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [a for a in ante_atoms
                       if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")]
        prop_atoms = [a for a in ante_atoms
                      if a["arity"] == 2 and not a.get("builtin") and not a.get("negated")]
        if not class_atoms:
            return None

        var_tables: Dict[str, str] = {}
        for a in class_atoms:
            var_tables[a["args"][0]] = self._resolve_node_table(a["name"], base_uri, uri_map)

        primary_var = class_atoms[0]["args"][0]
        connected = SWRLParser.find_connected_vars(primary_var, prop_atoms)

        match_parts: List[str] = []
        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                continue
            cypher = self._var_name(var)
            match_parts.append(f"MATCH ({cypher}:{var_tables[var]})")

        for prop in prop_atoms:
            subj_var = prop["args"][0]
            obj_var = prop["args"][1]
            if subj_var not in connected or obj_var not in connected:
                continue
            rel_table = self._resolve_rel_table(prop["name"], base_uri, uri_map)
            s_cypher = self._var_name(subj_var)
            o_cypher = self._var_name(obj_var)
            s_label = f":{var_tables[subj_var]}" if subj_var in var_tables else ""
            o_label = f":{var_tables[obj_var]}" if obj_var in var_tables else ""
            match_parts.append(
                f"MATCH ({s_cypher}{s_label})-[:{rel_table}]->({o_cypher}{o_label})"
            )

        creates: List[str] = []
        for atom in cons_atoms:
            if atom["arity"] == 2:
                var_s = atom["args"][0]
                var_o = atom["args"][1]
                if var_s not in connected or var_o not in connected:
                    logger.warning(
                        "SWRL materialisation: skipping consequent '%s' — "
                        "variables %s/%s not connected (cartesian product)",
                        atom["name"], var_s, var_o,
                    )
                    continue
                rel_table = self._resolve_rel_table(atom["name"], base_uri, uri_map)
                vs = self._var_name(var_s)
                vo = self._var_name(var_o)
                creates.append(f"CREATE ({vs})-[:{rel_table}]->({vo})")

        if not creates:
            return None

        cypher_lines = match_parts + creates
        query = "\n".join(cypher_lines)
        logger.debug("SWRL Cypher materialisation:\n%s", query)
        return query

    def build_inference_query(self, params: Dict) -> Optional[str]:
        """Build Cypher RETURN for inferred consequent triples."""
        antecedent = params.get("antecedent", "")
        consequent = params.get("consequent", "")
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [a for a in ante_atoms
                       if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")]
        prop_atoms = [a for a in ante_atoms
                      if a["arity"] == 2 and not a.get("builtin") and not a.get("negated")]
        if not class_atoms:
            return None

        var_tables: Dict[str, str] = {}
        for a in class_atoms:
            var_tables[a["args"][0]] = self._resolve_node_table(a["name"], base_uri, uri_map)

        primary_var = class_atoms[0]["args"][0]
        connected = SWRLParser.find_connected_vars(primary_var, prop_atoms)

        match_parts: List[str] = []
        var_cypher_map: Dict[str, str] = {}
        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                continue
            cypher = self._var_name(var)
            var_cypher_map[var] = cypher
            match_parts.append(f"MATCH ({cypher}:{var_tables[var]})")

        for prop in prop_atoms:
            subj_var = prop["args"][0]
            obj_var = prop["args"][1]
            if subj_var not in connected or obj_var not in connected:
                continue
            rel_table = self._resolve_rel_table(prop["name"], base_uri, uri_map)
            s_cypher = self._var_name(subj_var)
            o_cypher = self._var_name(obj_var)
            s_label = f":{var_tables[subj_var]}" if subj_var in var_tables else ""
            o_label = f":{var_tables[obj_var]}" if obj_var in var_tables else ""
            if obj_var not in var_cypher_map:
                var_cypher_map[obj_var] = o_cypher
            match_parts.append(
                f"MATCH ({s_cypher}{s_label})-[:{rel_table}]->({o_cypher}{o_label})"
            )

        return_blocks: List[tuple] = []
        for atom in cons_atoms:
            if atom["arity"] == 1:
                var = atom["args"][0]
                if var not in connected:
                    logger.warning(
                        "SWRL inference: skipping consequent class atom for "
                        "disconnected variable %s (would cause cartesian product)",
                        var,
                    )
                    continue
                cls_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                v = self._var_name(var)
                ret = (
                    f"{v}.uri AS subject, '{RDF_TYPE}' AS predicate, '{cls_uri}' AS object"
                )
                not_exists = (
                    f"NOT EXISTS {{ MATCH ({v})-[:rdf_type]->(:{{uri: '{cls_uri}'}}) }}"
                )
                return_blocks.append((ret, not_exists))
            elif atom["arity"] == 2:
                var_s = atom["args"][0]
                var_o = atom["args"][1]
                if var_s not in connected or var_o not in connected:
                    logger.warning(
                        "SWRL inference: skipping consequent property atom '%s' — "
                        "variables %s/%s are not connected (would cause cartesian product)",
                        atom["name"], var_s, var_o,
                    )
                    continue
                prop_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                rel_table = self._resolve_rel_table(atom["name"], base_uri, uri_map)
                vs = self._var_name(var_s)
                vo = self._var_name(var_o)
                ret = f"{vs}.uri AS subject, '{prop_uri}' AS predicate, {vo}.uri AS object"
                o_label = f":{var_tables[var_o]}" if var_o in var_tables else ""
                not_exists = (
                    f"NOT EXISTS {{ MATCH ({vs})-[:{rel_table}]->({vo}_chk{o_label}) "
                    f"WHERE {vo}_chk.uri = {vo}.uri }}"
                )
                return_blocks.append((ret, not_exists))

        if not return_blocks:
            return None

        blocks: List[str] = []
        for ret, not_exists in return_blocks:
            block_lines = match_parts.copy()
            block_lines.append(f"WHERE {not_exists}")
            block_lines.append(f"RETURN DISTINCT {ret}")
            blocks.append("\n".join(block_lines))

        query = "\nUNION ALL\n".join(blocks)
        logger.debug("SWRL Cypher inference:\n%s", query)
        return query

    @staticmethod
    def _var_name(swrl_var: str) -> str:
        """Convert a SWRL variable ``?x`` to a Cypher variable ``x``."""
        return swrl_var.lstrip("?")
