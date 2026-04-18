"""Translate SWRL rules to SQL for violation detection and materialisation.

Extracted from ``app/frontend/digitaltwin/routes.py`` to be reusable across the
reasoning engine and quality-check pipeline.
"""

from typing import Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.constants import RDF_TYPE
from back.core.reasoning.SWRLBuiltinRegistry import SWRLBuiltinRegistry
from back.core.reasoning.SWRLParser import SWRLParser

logger = get_logger(__name__)


class SWRLSQLTranslator:
    """Build SQL from SWRL rules for the flat triple-store table."""

    @staticmethod
    def _escape(val: str) -> str:
        """Escape a string value for safe SQL embedding."""
        return val.replace("'", "''").replace("\\", "\\\\")

    @staticmethod
    def _resolve_arg(token: str, var_bindings: Dict[str, tuple]) -> str:
        """Resolve a SWRL argument to a SQL expression.

        Variables (``?x``) are resolved via *var_bindings*.  Literals are
        converted to SQL literal syntax.
        """
        if not SWRLBuiltinRegistry.is_literal(token):
            stripped = token.lstrip("?")
            if stripped in var_bindings or token in var_bindings:
                key = token if token in var_bindings else stripped
                a, c = var_bindings[key]
                return f"{a}.{c}"
            return f"'{stripped}'"
        return SWRLBuiltinRegistry.literal_sql(token)

    @staticmethod
    def _build_builtin_filters(
        builtin_atoms: List[Dict],
        var_bindings: Dict[str, tuple],
    ) -> List[str]:
        filters: List[str] = []
        for atom in builtin_atoms:
            bi = SWRLBuiltinRegistry.get(atom["name"])
            if bi is None:
                continue
            resolved = [
                SWRLSQLTranslator._resolve_arg(a, var_bindings) for a in atom["args"]
            ]
            if bi.category == "comparison" and bi.arity == 2:
                expr = bi.sql_template.format(*resolved[:2])
                filters.append(expr)
            elif bi.category == "string" and bi.arity == 2:
                expr = bi.sql_template.format(*resolved[:2])
                filters.append(expr)
            elif bi.category == "math" and bi.arity == 3:
                expr = bi.sql_template.format(*resolved[:2])
                result_ref = SWRLSQLTranslator._resolve_arg(
                    atom["args"][2],
                    var_bindings,
                )
                filters.append(f"{result_ref} = ({expr})")
            elif bi.category == "date" and bi.arity == 2:
                expr = bi.sql_template.format(*resolved[:2])
                filters.append(expr)
            elif bi.arity <= len(resolved):
                expr = bi.sql_template.format(*resolved[: bi.arity])
                filters.append(expr)
        return filters

    @staticmethod
    def _build_negated_atoms(
        negated_atoms: List[Dict],
        table: str,
        var_bindings: Dict[str, tuple],
        base_uri: str,
        uri_map: Dict,
        alias_factory,
    ) -> List[str]:
        parts: List[str] = []
        for atom in negated_atoms:
            if atom.get("builtin"):
                continue
            uri = SWRLSQLTranslator._escape(
                SWRLParser.resolve_uri(atom["name"], base_uri, uri_map),
            )
            alias = alias_factory()
            if atom["arity"] == 1:
                var = atom["args"][0]
                if var not in var_bindings:
                    continue
                a, c = var_bindings[var]
                parts.append(
                    f"NOT EXISTS (SELECT 1 FROM {table} {alias} "
                    f"WHERE {alias}.predicate = '{RDF_TYPE}' "
                    f"AND {alias}.object = '{uri}' "
                    f"AND {alias}.subject = {a}.{c})"
                )
            elif atom["arity"] == 2:
                var_s, var_o = atom["args"][0], atom["args"][1]
                if var_s not in var_bindings:
                    continue
                sa, sc = var_bindings[var_s]
                if var_o in var_bindings:
                    oa, oc = var_bindings[var_o]
                    parts.append(
                        f"NOT EXISTS (SELECT 1 FROM {table} {alias} "
                        f"WHERE {alias}.predicate = '{uri}' "
                        f"AND {alias}.subject = {sa}.{sc} "
                        f"AND {alias}.object = {oa}.{oc})"
                    )
                else:
                    parts.append(
                        f"NOT EXISTS (SELECT 1 FROM {table} {alias} "
                        f"WHERE {alias}.predicate = '{uri}' "
                        f"AND {alias}.subject = {sa}.{sc})"
                    )
        return parts

    def build_violation_sql(self, table: str, params: Dict) -> Optional[str]:
        """Build SQL that finds subjects violating a SWRL rule.

        The violation subject is determined from the consequent (first
        property-atom subject, or first class-atom variable).  When
        antecedent variables are not connected through property atoms,
        their type constraints are folded into the NOT EXISTS sub-query
        instead of creating a Cartesian product in the main query.

        Supports built-in filter atoms (comparison, math, string, date)
        and negated antecedent atoms (closed-world assumption).
        """
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        part = SWRLParser.partition_rule_atoms(params)
        if part is None:
            return None
        class_atoms = part.class_atoms
        prop_atoms = part.prop_atoms
        builtin_atoms = part.builtin_atoms
        negated_atoms = part.negated_atoms
        cons_atoms = part.consequent_atoms

        var_class: Dict[str, str] = {}
        for a in class_atoms:
            var_class[a["args"][0]] = self._escape(
                SWRLParser.resolve_uri(a["name"], base_uri, uri_map),
            )

        violation_var = SWRLParser.determine_violation_subject(cons_atoms, class_atoms)
        if not violation_var or violation_var not in var_class:
            return None

        connected = SWRLParser.find_connected_vars(violation_var, prop_atoms)

        alias_counter = 0
        var_bindings: Dict[str, tuple] = {}

        def _next():
            nonlocal alias_counter
            alias_counter += 1
            return f"a{alias_counter}"

        def _ref(var):
            a, c = var_bindings[var]
            return f"{a}.{c}"

        a_prim = _next()
        from_part = f"{table} {a_prim}"
        where_parts = [
            f"{a_prim}.predicate = '{RDF_TYPE}'",
            f"{a_prim}.object = '{var_class[violation_var]}'",
        ]
        var_bindings[violation_var] = (a_prim, "subject")

        join_parts: List[str] = []
        left_join_parts: List[str] = []
        null_checks: List[str] = []

        connected_props = SWRLParser.order_connected_props(
            violation_var,
            [
                p
                for p in prop_atoms
                if p["args"][0] in connected and p["args"][1] in connected
            ],
        )

        for idx, prop in enumerate(connected_props):
            p_uri = self._escape(
                SWRLParser.resolve_uri(prop["name"], base_uri, uri_map),
            )
            subj_var = prop["args"][0]
            obj_var = prop["args"][1]
            a_prop = _next()
            left = idx > 0
            kind = "LEFT JOIN" if left else "JOIN"

            on_clause = f"{a_prop}.predicate = '{p_uri}'"
            if subj_var in var_bindings:
                on_clause += f" AND {a_prop}.subject = {_ref(subj_var)}"
            elif obj_var in var_bindings:
                on_clause += f" AND {a_prop}.object = {_ref(obj_var)}"

            clause = f"{kind} {table} {a_prop} ON {on_clause}"
            if left:
                left_join_parts.append(clause)
                null_checks.append(f"{a_prop}.subject IS NULL")
            else:
                join_parts.append(clause)

            new_var = obj_var if subj_var in var_bindings else subj_var
            if new_var not in var_bindings:
                col = "object" if new_var == obj_var else "subject"
                if new_var in var_class:
                    a_cls = _next()
                    cls_clause = (
                        f"{kind} {table} {a_cls} "
                        f"ON {a_cls}.predicate = '{RDF_TYPE}' "
                        f"AND {a_cls}.object = '{var_class[new_var]}' "
                        f"AND {a_cls}.subject = {a_prop}.{col}"
                    )
                    if left:
                        left_join_parts.append(cls_clause)
                    else:
                        join_parts.append(cls_clause)
                    var_bindings[new_var] = (a_cls, "subject")
                else:
                    var_bindings[new_var] = (a_prop, col)

        # -- Built-in filter atoms (Phase 1) ----------------------------------
        builtin_filters = SWRLSQLTranslator._build_builtin_filters(
            builtin_atoms,
            var_bindings,
        )
        if builtin_filters:
            where_parts.extend(builtin_filters)

        # -- Negated antecedent atoms (Phase 3: closed-world) ----------------
        negated_sql = SWRLSQLTranslator._build_negated_atoms(
            negated_atoms,
            table,
            var_bindings,
            base_uri,
            uri_map,
            _next,
        )
        if negated_sql:
            where_parts.extend(negated_sql)

        disconnected_class_uris: Dict[str, str] = {}
        for ca in class_atoms:
            var = ca["args"][0]
            if var not in connected:
                disconnected_class_uris[var] = var_class[var]

        not_exists_parts: List[str] = []
        for atom in cons_atoms:
            c_uri = self._escape(
                SWRLParser.resolve_uri(atom["name"], base_uri, uri_map),
            )
            c_alias = _next()

            if atom["arity"] == 1:
                var = atom["args"][0]
                if var not in var_bindings:
                    continue
                not_exists_parts.append(
                    f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                    f"WHERE {c_alias}.predicate = '{RDF_TYPE}' "
                    f"AND {c_alias}.object = '{c_uri}' "
                    f"AND {c_alias}.subject = {_ref(var)})"
                )
            elif atom["arity"] == 2:
                var_s, var_o = atom["args"][0], atom["args"][1]
                if var_s not in var_bindings:
                    continue
                if var_o in var_bindings:
                    cond = (
                        f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                        f"WHERE {c_alias}.predicate = '{c_uri}' "
                        f"AND {c_alias}.subject = {_ref(var_s)}"
                        f" AND {c_alias}.object = {_ref(var_o)})"
                    )
                elif var_o in disconnected_class_uris:
                    c_alias2 = _next()
                    cond = (
                        f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                        f"JOIN {table} {c_alias2} "
                        f"ON {c_alias2}.predicate = '{RDF_TYPE}' "
                        f"AND {c_alias2}.object = '{disconnected_class_uris[var_o]}' "
                        f"AND {c_alias2}.subject = {c_alias}.object "
                        f"WHERE {c_alias}.predicate = '{c_uri}' "
                        f"AND {c_alias}.subject = {_ref(var_s)})"
                    )
                else:
                    cond = (
                        f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                        f"WHERE {c_alias}.predicate = '{c_uri}' "
                        f"AND {c_alias}.subject = {_ref(var_s)})"
                    )
                not_exists_parts.append(cond)

        if not not_exists_parts and not null_checks:
            return None

        violation_cond = " OR ".join(null_checks + not_exists_parts)
        lines = [f"SELECT DISTINCT {_ref(violation_var)} AS s"]
        lines.append(f"FROM {from_part}")
        lines.extend(join_parts)
        lines.extend(left_join_parts)
        lines.append(f"WHERE {' AND '.join(where_parts)}")
        lines.append(f"  AND ({violation_cond})")

        sql = "\n".join(lines)
        logger.debug(
            "SWRL SQL [%s -> %s]:\n%s",
            params.get("antecedent", ""),
            params.get("consequent", ""),
            sql,
        )
        return sql

    def build_antecedent_count_sql(self, table: str, params: Dict) -> Optional[str]:
        """Build SQL that counts entities matching the antecedent.

        Returns a query with a single ``cnt`` column — the number of
        distinct subjects that satisfy the antecedent conditions
        (ignoring the consequent).  This is the denominator for
        computing the pass rate of a SWRL rule used as a DQ indicator.
        """
        antecedent = params.get("antecedent", "")
        consequent = params.get("consequent", "")
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [
            a
            for a in ante_atoms
            if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")
        ]
        prop_atoms = [
            a
            for a in ante_atoms
            if a["arity"] == 2 and not a.get("builtin") and not a.get("negated")
        ]
        builtin_atoms = [
            a for a in ante_atoms if a.get("builtin") and not a.get("negated")
        ]
        negated_atoms = [a for a in ante_atoms if a.get("negated")]
        if not class_atoms:
            return None

        var_class: Dict[str, str] = {}
        for a in class_atoms:
            var_class[a["args"][0]] = self._escape(
                SWRLParser.resolve_uri(a["name"], base_uri, uri_map),
            )

        violation_var = SWRLParser.determine_violation_subject(cons_atoms, class_atoms)
        if not violation_var or violation_var not in var_class:
            return None

        connected = SWRLParser.find_connected_vars(violation_var, prop_atoms)

        alias_counter = 0
        var_bindings: Dict[str, tuple] = {}

        def _next():
            nonlocal alias_counter
            alias_counter += 1
            return f"a{alias_counter}"

        def _ref(var):
            a, c = var_bindings[var]
            return f"{a}.{c}"

        a_prim = _next()
        from_part = f"{table} {a_prim}"
        where_parts = [
            f"{a_prim}.predicate = '{RDF_TYPE}'",
            f"{a_prim}.object = '{var_class[violation_var]}'",
        ]
        var_bindings[violation_var] = (a_prim, "subject")

        join_parts: List[str] = []

        connected_props = SWRLParser.order_connected_props(
            violation_var,
            [
                p
                for p in prop_atoms
                if p["args"][0] in connected and p["args"][1] in connected
            ],
        )

        for idx, prop in enumerate(connected_props):
            p_uri = self._escape(
                SWRLParser.resolve_uri(prop["name"], base_uri, uri_map),
            )
            subj_var = prop["args"][0]
            obj_var = prop["args"][1]
            a_prop = _next()

            on_clause = f"{a_prop}.predicate = '{p_uri}'"
            if subj_var in var_bindings:
                on_clause += f" AND {a_prop}.subject = {_ref(subj_var)}"
            elif obj_var in var_bindings:
                on_clause += f" AND {a_prop}.object = {_ref(obj_var)}"

            join_parts.append(f"JOIN {table} {a_prop} ON {on_clause}")

            new_var = obj_var if subj_var in var_bindings else subj_var
            if new_var not in var_bindings:
                col = "object" if new_var == obj_var else "subject"
                if new_var in var_class:
                    a_cls = _next()
                    join_parts.append(
                        f"JOIN {table} {a_cls} "
                        f"ON {a_cls}.predicate = '{RDF_TYPE}' "
                        f"AND {a_cls}.object = '{var_class[new_var]}' "
                        f"AND {a_cls}.subject = {a_prop}.{col}"
                    )
                    var_bindings[new_var] = (a_cls, "subject")
                else:
                    var_bindings[new_var] = (a_prop, col)

        builtin_filters = SWRLSQLTranslator._build_builtin_filters(
            builtin_atoms,
            var_bindings,
        )
        if builtin_filters:
            where_parts.extend(builtin_filters)

        negated_sql = SWRLSQLTranslator._build_negated_atoms(
            negated_atoms,
            table,
            var_bindings,
            base_uri,
            uri_map,
            _next,
        )
        if negated_sql:
            where_parts.extend(negated_sql)

        lines = [f"SELECT COUNT(DISTINCT {_ref(violation_var)}) AS cnt"]
        lines.append(f"FROM {from_part}")
        lines.extend(join_parts)
        lines.append(f"WHERE {' AND '.join(where_parts)}")

        return "\n".join(lines)

    def build_materialization_sql(self, table: str, params: Dict) -> Optional[str]:
        """Build INSERT SQL that materialises consequent triples.

        Finds rows where the antecedent matches AND the consequent is
        missing, then inserts the missing consequent triples.
        """
        antecedent = params.get("antecedent", "")
        consequent = params.get("consequent", "")
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [a for a in ante_atoms if a["arity"] == 1]
        prop_atoms = [a for a in ante_atoms if a["arity"] == 2]
        if not class_atoms:
            return None

        var_class: Dict[str, str] = {}
        for a in class_atoms:
            var_class[a["args"][0]] = self._escape(
                SWRLParser.resolve_uri(a["name"], base_uri, uri_map),
            )

        primary_var = class_atoms[0]["args"][0]
        primary_props = [p for p in prop_atoms if p["args"][0] == primary_var]
        if not primary_props:
            return None

        alias_counter = 0
        var_bindings: Dict[str, tuple] = {}

        def _next():
            nonlocal alias_counter
            alias_counter += 1
            return f"a{alias_counter}"

        def _ref(var):
            a, c = var_bindings[var]
            return f"{a}.{c}"

        a_prim = _next()
        from_part = f"{table} {a_prim}"
        where_parts = [
            f"{a_prim}.predicate = '{RDF_TYPE}'",
            f"{a_prim}.object = '{var_class.get(primary_var, '')}'",
        ]
        var_bindings[primary_var] = (a_prim, "subject")

        join_parts: List[str] = []

        for prop in primary_props:
            p_uri = self._escape(
                SWRLParser.resolve_uri(prop["name"], base_uri, uri_map),
            )
            obj_var = prop["args"][1]
            a_prop = _next()
            join_parts.append(
                f"JOIN {table} {a_prop} "
                f"ON {a_prop}.predicate = '{p_uri}' "
                f"AND {a_prop}.subject = {_ref(primary_var)}"
            )
            if obj_var in var_class:
                a_cls = _next()
                join_parts.append(
                    f"JOIN {table} {a_cls} "
                    f"ON {a_cls}.predicate = '{RDF_TYPE}' "
                    f"AND {a_cls}.object = '{var_class[obj_var]}' "
                    f"AND {a_cls}.subject = {a_prop}.object"
                )
                var_bindings[obj_var] = (a_cls, "subject")
            else:
                var_bindings[obj_var] = (a_prop, "object")

        stmts: List[str] = []
        for atom in cons_atoms:
            c_uri = self._escape(
                SWRLParser.resolve_uri(atom["name"], base_uri, uri_map),
            )
            c_alias = _next()
            if atom["arity"] == 1:
                var = atom["args"][0]
                if var not in var_bindings:
                    continue
                not_exists = (
                    f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                    f"WHERE {c_alias}.predicate = '{RDF_TYPE}' "
                    f"AND {c_alias}.object = '{c_uri}' "
                    f"AND {c_alias}.subject = {_ref(var)})"
                )
                stmts.append(
                    f"INSERT INTO {table} (subject, predicate, object)\n"
                    f"SELECT DISTINCT {_ref(var)}, '{RDF_TYPE}', '{c_uri}'\n"
                    f"FROM {from_part}\n" + "\n".join(join_parts) + "\n"
                    f"WHERE {' AND '.join(where_parts)}\n"
                    f"  AND {not_exists}"
                )
            elif atom["arity"] == 2:
                var_s, var_o = atom["args"][0], atom["args"][1]
                if var_s not in var_bindings:
                    continue
                not_exists = (
                    f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                    f"WHERE {c_alias}.predicate = '{c_uri}' "
                    f"AND {c_alias}.subject = {_ref(var_s)}"
                )
                if var_o in var_bindings:
                    not_exists += f" AND {c_alias}.object = {_ref(var_o)}"
                not_exists += ")"
                obj_expr = _ref(var_o) if var_o in var_bindings else "''"
                stmts.append(
                    f"INSERT INTO {table} (subject, predicate, object)\n"
                    f"SELECT DISTINCT {_ref(var_s)}, '{c_uri}', {obj_expr}\n"
                    f"FROM {from_part}\n" + "\n".join(join_parts) + "\n"
                    f"WHERE {' AND '.join(where_parts)}\n"
                    f"  AND {not_exists}"
                )

        return ";\n".join(stmts) if stmts else None

    def build_inference_sql(self, table: str, params: Dict) -> Optional[str]:
        """Build SELECT SQL that returns inferred consequent triples.

        Uses the same antecedent binding as ``build_materialization_sql``
        but emits ``SELECT subject, predicate, object`` instead of
        ``INSERT INTO``.  Multiple consequent atoms are combined with
        ``UNION ALL``.
        """
        antecedent = params.get("antecedent", "")
        consequent = params.get("consequent", "")
        base_uri = params.get("base_uri", "")
        uri_map = params.get("uri_map") or {}

        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [a for a in ante_atoms if a["arity"] == 1]
        prop_atoms = [a for a in ante_atoms if a["arity"] == 2]
        if not class_atoms:
            return None

        var_class: Dict[str, str] = {}
        for a in class_atoms:
            var_class[a["args"][0]] = self._escape(
                SWRLParser.resolve_uri(a["name"], base_uri, uri_map),
            )

        primary_var = class_atoms[0]["args"][0]
        primary_props = [p for p in prop_atoms if p["args"][0] == primary_var]
        if not primary_props:
            return None

        alias_counter = 0
        var_bindings: Dict[str, tuple] = {}

        def _next():
            nonlocal alias_counter
            alias_counter += 1
            return f"a{alias_counter}"

        def _ref(var):
            a, c = var_bindings[var]
            return f"{a}.{c}"

        a_prim = _next()
        from_part = f"{table} {a_prim}"
        where_parts = [
            f"{a_prim}.predicate = '{RDF_TYPE}'",
            f"{a_prim}.object = '{var_class.get(primary_var, '')}'",
        ]
        var_bindings[primary_var] = (a_prim, "subject")

        join_parts: List[str] = []

        for prop in primary_props:
            p_uri = self._escape(
                SWRLParser.resolve_uri(prop["name"], base_uri, uri_map),
            )
            obj_var = prop["args"][1]
            a_prop = _next()
            join_parts.append(
                f"JOIN {table} {a_prop} "
                f"ON {a_prop}.predicate = '{p_uri}' "
                f"AND {a_prop}.subject = {_ref(primary_var)}"
            )
            if obj_var in var_class:
                a_cls = _next()
                join_parts.append(
                    f"JOIN {table} {a_cls} "
                    f"ON {a_cls}.predicate = '{RDF_TYPE}' "
                    f"AND {a_cls}.object = '{var_class[obj_var]}' "
                    f"AND {a_cls}.subject = {a_prop}.object"
                )
                var_bindings[obj_var] = (a_cls, "subject")
            else:
                var_bindings[obj_var] = (a_prop, "object")

        selects: List[str] = []
        for atom in cons_atoms:
            c_uri = self._escape(
                SWRLParser.resolve_uri(atom["name"], base_uri, uri_map),
            )
            c_alias = _next()
            if atom["arity"] == 1:
                var = atom["args"][0]
                if var not in var_bindings:
                    continue
                not_exists = (
                    f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                    f"WHERE {c_alias}.predicate = '{RDF_TYPE}' "
                    f"AND {c_alias}.object = '{c_uri}' "
                    f"AND {c_alias}.subject = {_ref(var)})"
                )
                selects.append(
                    f"SELECT DISTINCT {_ref(var)} AS subject, "
                    f"'{RDF_TYPE}' AS predicate, '{c_uri}' AS object\n"
                    f"FROM {from_part}\n" + "\n".join(join_parts) + "\n"
                    f"WHERE {' AND '.join(where_parts)}\n"
                    f"  AND {not_exists}"
                )
            elif atom["arity"] == 2:
                var_s, var_o = atom["args"][0], atom["args"][1]
                if var_s not in var_bindings:
                    continue
                not_exists = (
                    f"NOT EXISTS (SELECT 1 FROM {table} {c_alias} "
                    f"WHERE {c_alias}.predicate = '{c_uri}' "
                    f"AND {c_alias}.subject = {_ref(var_s)}"
                )
                if var_o in var_bindings:
                    not_exists += f" AND {c_alias}.object = {_ref(var_o)}"
                not_exists += ")"
                obj_expr = _ref(var_o) if var_o in var_bindings else "''"
                selects.append(
                    f"SELECT DISTINCT {_ref(var_s)} AS subject, "
                    f"'{c_uri}' AS predicate, {obj_expr} AS object\n"
                    f"FROM {from_part}\n" + "\n".join(join_parts) + "\n"
                    f"WHERE {' AND '.join(where_parts)}\n"
                    f"  AND {not_exists}"
                )

        if not selects:
            return None

        sql = "\nUNION ALL\n".join(selects)
        logger.debug("SWRL inference SQL [%s -> %s]:\n%s", antecedent, consequent, sql)
        return sql
