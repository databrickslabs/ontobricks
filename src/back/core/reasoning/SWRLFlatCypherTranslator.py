"""Translate SWRL rules to Cypher for LadybugDB flat-model backends.

The flat model stores every triple as a node in a single table with
``subject``, ``predicate``, ``object`` columns.
"""
from typing import Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.constants import RDF_TYPE
from back.core.reasoning.SWRLParser import SWRLParser
from back.core.reasoning.SWRLCypherTranslator import SWRLCypherTranslator

logger = get_logger(__name__)


class SWRLFlatCypherTranslator:
    """Build Cypher queries from SWRL rules for LadybugDB flat-model stores.

    The flat model stores every triple as a node in a single table::

        Triple(id, subject, predicate, object)

    Each SWRL class atom ``ClassName(?x)`` becomes a MATCH on a Triple
    node whose ``predicate = rdf:type`` and ``object = <class URI>``.
    Each property atom ``propName(?x, ?y)`` becomes a MATCH on a Triple
    node whose ``predicate = <prop URI>`` with variable binding via
    ``subject``/``object``.
    """

    def __init__(self, node_table: str = "Triple") -> None:
        self._table = node_table

    def build_violation_query(self, params: Dict) -> Optional[str]:
        """Return Cypher that finds subjects violating a SWRL rule."""
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

        violation_var = SWRLParser.determine_violation_subject(cons_atoms, class_atoms)
        if not violation_var:
            return None
        if not any(ca["args"][0] == violation_var for ca in class_atoms):
            return None

        connected = SWRLParser.find_connected_vars(violation_var, prop_atoms)

        tbl = self._table
        match_parts: List[str] = []
        where_parts: List[str] = []
        alias_idx = 0
        var_subject_alias: Dict[str, str] = {}

        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                continue
            cls_uri = SWRLParser.resolve_uri(ca["name"], base_uri, uri_map)
            alias = f"ca{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.predicate = '{RDF_TYPE}'")
            where_parts.append(f"{alias}.object = '{self._esc(cls_uri)}'")
            var_subject_alias[var] = f"{alias}.subject"

        for pa in prop_atoms:
            subj_var = pa["args"][0]
            obj_var = pa["args"][1]
            if subj_var not in connected or obj_var not in connected:
                continue
            prop_uri = SWRLParser.resolve_uri(pa["name"], base_uri, uri_map)
            alias = f"pa{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.predicate = '{self._esc(prop_uri)}'")
            if subj_var in var_subject_alias:
                where_parts.append(f"{alias}.subject = {var_subject_alias[subj_var]}")
            if obj_var not in var_subject_alias:
                var_subject_alias[obj_var] = f"{alias}.object"
            else:
                where_parts.append(f"{alias}.object = {var_subject_alias[obj_var]}")

        builtin_filters = SWRLCypherTranslator._build_cypher_builtin_filters(
            builtin_atoms, var_subject_alias
        )
        where_parts.extend(builtin_filters)

        for neg in negated_atoms:
            if neg.get("builtin"):
                continue
            prop_uri = SWRLParser.resolve_uri(neg["name"], base_uri, uri_map)
            if neg["arity"] == 2:
                var_s = neg["args"][0]
                ref_s = var_subject_alias.get(var_s, f"'{var_s}'")
                na = f"neg{alias_idx}"
                alias_idx += 1
                where_parts.append(
                    f"NOT EXISTS {{ MATCH ({na}:{tbl}) "
                    f"WHERE {na}.predicate = '{self._esc(prop_uri)}' "
                    f"AND {na}.subject = {ref_s} }}"
                )
            elif neg["arity"] == 1:
                var = neg["args"][0]
                ref = var_subject_alias.get(var, f"'{var}'")
                na = f"neg{alias_idx}"
                alias_idx += 1
                where_parts.append(
                    f"NOT EXISTS {{ MATCH ({na}:{tbl}) "
                    f"WHERE {na}.predicate = '{RDF_TYPE}' "
                    f"AND {na}.object = '{self._esc(prop_uri)}' "
                    f"AND {na}.subject = {ref} }}"
                )

        disconnected_class_uris: Dict[str, str] = {}
        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                disconnected_class_uris[var] = SWRLParser.resolve_uri(
                    ca["name"], base_uri, uri_map
                )

        not_exists_parts: List[str] = []
        ne_idx = 0
        for atom in cons_atoms:
            if atom["arity"] == 1:
                var = atom["args"][0]
                cls_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                ref = var_subject_alias.get(var, f"'{var}'")
                not_exists_parts.append(
                    f"NOT EXISTS {{ MATCH (cx:{tbl}) "
                    f"WHERE cx.predicate = '{RDF_TYPE}' "
                    f"AND cx.object = '{self._esc(cls_uri)}' "
                    f"AND cx.subject = {ref} }}"
                )
            elif atom["arity"] == 2:
                subj_var = atom["args"][0]
                obj_var = atom["args"][1]
                prop_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                ref_s = var_subject_alias.get(subj_var, f"'{subj_var}'")

                if obj_var in var_subject_alias:
                    ref_o = var_subject_alias[obj_var]
                    not_exists_parts.append(
                        f"NOT EXISTS {{ MATCH (px:{tbl}) "
                        f"WHERE px.predicate = '{self._esc(prop_uri)}' "
                        f"AND px.subject = {ref_s} "
                        f"AND px.object = {ref_o} }}"
                    )
                elif obj_var in disconnected_class_uris:
                    obj_cls_uri = disconnected_class_uris[obj_var]
                    pa = f"ne_p{ne_idx}"
                    pc = f"ne_c{ne_idx}"
                    ne_idx += 1
                    not_exists_parts.append(
                        f"NOT EXISTS {{ MATCH ({pa}:{tbl}), ({pc}:{tbl}) "
                        f"WHERE {pa}.predicate = '{self._esc(prop_uri)}' "
                        f"AND {pa}.subject = {ref_s} "
                        f"AND {pc}.predicate = '{RDF_TYPE}' "
                        f"AND {pc}.object = '{self._esc(obj_cls_uri)}' "
                        f"AND {pc}.subject = {pa}.object }}"
                    )
                else:
                    not_exists_parts.append(
                        f"NOT EXISTS {{ MATCH (px:{tbl}) "
                        f"WHERE px.predicate = '{self._esc(prop_uri)}' "
                        f"AND px.subject = {ref_s} }}"
                    )

        if not not_exists_parts:
            return None

        violation_ref = var_subject_alias.get(violation_var, "''")
        lines = match_parts.copy()
        all_where = where_parts + [f"({' OR '.join(not_exists_parts)})"]
        lines.append("WHERE " + " AND ".join(all_where))
        lines.append(f"RETURN DISTINCT {violation_ref} AS s")

        query = "\n".join(lines)
        logger.debug("SWRL flat Cypher violation:\n%s", query)
        return query

    def build_violation_sql(self, table_name: str, params: Dict) -> Optional[str]:
        """Alias so the engine can call the same method name as SQL translator."""
        return self.build_violation_query(params)

    def build_materialization_query(self, params: Dict) -> Optional[str]:
        """Build Cypher CREATE for inferred triples on the flat model."""
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

        primary_var = class_atoms[0]["args"][0]
        connected = SWRLParser.find_connected_vars(primary_var, prop_atoms)

        tbl = self._table
        match_parts: List[str] = []
        where_parts: List[str] = []
        alias_idx = 0
        var_subject_alias: Dict[str, str] = {}

        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                continue
            cls_uri = SWRLParser.resolve_uri(ca["name"], base_uri, uri_map)
            alias = f"ca{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.predicate = '{RDF_TYPE}'")
            where_parts.append(f"{alias}.object = '{self._esc(cls_uri)}'")
            var_subject_alias[var] = f"{alias}.subject"

        for pa in prop_atoms:
            subj_var = pa["args"][0]
            obj_var = pa["args"][1]
            if subj_var not in connected or obj_var not in connected:
                continue
            prop_uri = SWRLParser.resolve_uri(pa["name"], base_uri, uri_map)
            alias = f"pa{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.predicate = '{self._esc(prop_uri)}'")
            if subj_var in var_subject_alias:
                where_parts.append(f"{alias}.subject = {var_subject_alias[subj_var]}")
            if obj_var not in var_subject_alias:
                var_subject_alias[obj_var] = f"{alias}.object"
            else:
                where_parts.append(f"{alias}.object = {var_subject_alias[obj_var]}")

        creates: List[str] = []
        for atom in cons_atoms:
            if atom["arity"] == 2:
                subj_var = atom["args"][0]
                obj_var = atom["args"][1]
                if subj_var not in connected or obj_var not in connected:
                    logger.warning(
                        "SWRL flat materialisation: skipping consequent '%s' — "
                        "variables %s/%s not connected (cartesian product)",
                        atom["name"], subj_var, obj_var,
                    )
                    continue
                prop_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                ref_s = var_subject_alias.get(subj_var, f"'{subj_var}'")
                ref_o = var_subject_alias.get(obj_var, f"'{obj_var}'")
                creates.append(
                    f"CREATE (:{tbl} {{id: 0, "
                    f"subject: {ref_s}, "
                    f"predicate: '{self._esc(prop_uri)}', "
                    f"object: {ref_o}}})"
                )

        if not creates:
            return None

        lines = match_parts.copy()
        if where_parts:
            lines.append("WHERE " + " AND ".join(where_parts))
        lines.extend(creates)

        query = "\n".join(lines)
        logger.debug("SWRL flat Cypher materialisation:\n%s", query)
        return query

    def build_materialization_sql(self, table_name: str, params: Dict) -> Optional[str]:
        """Alias so the engine can call the same method name as SQL translator."""
        return self.build_materialization_query(params)

    def build_inference_query(self, params: Dict) -> Optional[str]:
        """Build Cypher RETURN for inferred triples on the flat model."""
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

        primary_var = class_atoms[0]["args"][0]
        connected = SWRLParser.find_connected_vars(primary_var, prop_atoms)

        tbl = self._table
        match_parts: List[str] = []
        where_parts: List[str] = []
        alias_idx = 0
        var_subject_alias: Dict[str, str] = {}

        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                continue
            cls_uri = SWRLParser.resolve_uri(ca["name"], base_uri, uri_map)
            alias = f"ca{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.predicate = '{RDF_TYPE}'")
            where_parts.append(f"{alias}.object = '{self._esc(cls_uri)}'")
            var_subject_alias[var] = f"{alias}.subject"

        for pa in prop_atoms:
            subj_var = pa["args"][0]
            obj_var = pa["args"][1]
            if subj_var not in connected or obj_var not in connected:
                continue
            prop_uri = SWRLParser.resolve_uri(pa["name"], base_uri, uri_map)
            alias = f"pa{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.predicate = '{self._esc(prop_uri)}'")
            if subj_var in var_subject_alias:
                where_parts.append(f"{alias}.subject = {var_subject_alias[subj_var]}")
            if obj_var not in var_subject_alias:
                var_subject_alias[obj_var] = f"{alias}.object"
            else:
                where_parts.append(f"{alias}.object = {var_subject_alias[obj_var]}")

        return_blocks: List[tuple] = []
        ne_idx = 0
        for atom in cons_atoms:
            if atom["arity"] == 1:
                var = atom["args"][0]
                if var not in connected:
                    logger.warning(
                        "SWRL flat inference: skipping consequent class atom for "
                        "disconnected variable %s (would cause cartesian product)",
                        var,
                    )
                    continue
                cls_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                ref = var_subject_alias.get(var, f"'{var}'")
                ret = (
                    f"{ref} AS subject, '{RDF_TYPE}' AS predicate, "
                    f"'{self._esc(cls_uri)}' AS object"
                )
                ne_alias = f"ne{ne_idx}"
                ne_idx += 1
                not_exists = (
                    f"NOT EXISTS {{ MATCH ({ne_alias}:{tbl}) "
                    f"WHERE {ne_alias}.predicate = '{RDF_TYPE}' "
                    f"AND {ne_alias}.object = '{self._esc(cls_uri)}' "
                    f"AND {ne_alias}.subject = {ref} }}"
                )
                return_blocks.append((ret, not_exists))
            elif atom["arity"] == 2:
                subj_var = atom["args"][0]
                obj_var = atom["args"][1]
                if subj_var not in connected or obj_var not in connected:
                    logger.warning(
                        "SWRL flat inference: skipping consequent property atom '%s' — "
                        "variables %s/%s are not connected (would cause cartesian product)",
                        atom["name"], subj_var, obj_var,
                    )
                    continue
                prop_uri = SWRLParser.resolve_uri(atom["name"], base_uri, uri_map)
                ref_s = var_subject_alias.get(subj_var, f"'{subj_var}'")
                ref_o = var_subject_alias.get(obj_var, f"'{obj_var}'")
                ret = (
                    f"{ref_s} AS subject, '{self._esc(prop_uri)}' AS predicate, "
                    f"{ref_o} AS object"
                )
                ne_alias = f"ne{ne_idx}"
                ne_idx += 1
                not_exists = (
                    f"NOT EXISTS {{ MATCH ({ne_alias}:{tbl}) "
                    f"WHERE {ne_alias}.predicate = '{self._esc(prop_uri)}' "
                    f"AND {ne_alias}.subject = {ref_s} "
                    f"AND {ne_alias}.object = {ref_o} }}"
                )
                return_blocks.append((ret, not_exists))

        if not return_blocks:
            return None

        base_match = match_parts.copy()
        base_where = where_parts.copy()

        blocks: List[str] = []
        for ret, not_exists in return_blocks:
            lines = base_match.copy()
            where_with_ne = base_where + [not_exists]
            lines.append("WHERE " + " AND ".join(where_with_ne))
            lines.append(f"RETURN DISTINCT {ret}")
            blocks.append("\n".join(lines))

        query = "\nUNION ALL\n".join(blocks)
        logger.debug("SWRL flat Cypher inference:\n%s", query)
        return query

    def build_inference_sql(self, table_name: str, params: Dict) -> Optional[str]:
        """Alias so the engine can call the same method name as SQL translator."""
        return self.build_inference_query(params)

    @staticmethod
    def _esc(val: str) -> str:
        return val.replace("'", "\\'")
