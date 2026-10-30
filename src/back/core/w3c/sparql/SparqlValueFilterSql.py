"""Compile SPARQL value FILTERs into Spark SQL WHERE fragments."""

from __future__ import annotations

from back.core.errors import ValidationError
from back.core.helpers import (
    extract_local_name as _extract_local,
    sql_cast as _sql_cast,
    sql_escape as _escape_sql,
)


class SparqlValueFilterSql:
    """Turn parsed string FILTERs into SQL predicates; fail closed if they cannot map."""

    @staticmethod
    def conditions_for(
        pred_to_column, value_filters, column_qualifier: str, dialect: str
    ) -> list[str]:
        """Build SQL WHERE fragments for ``value_filters``.

        ``column_qualifier`` is a prefix such as ``\"\"`` or ``\"e.\"``.
        """
        where_conditions = []
        for vf in value_filters:
            pred_uri = vf["predicate_uri"]
            column = pred_to_column.get(pred_uri) or pred_to_column.get(
                _extract_local(pred_uri).lower()
            )
            if not column:
                continue
            col_ref = f"{column_qualifier}{column}" if column_qualifier else column
            operator = vf["operator"]
            value = _escape_sql(vf["value"], dialect)
            if operator == "contains":
                where_conditions.append(f"LOWER({col_ref}) LIKE '%{value}%'")
            elif operator == "equals":
                where_conditions.append(f"{_sql_cast(col_ref, 'STRING')} = '{value}'")
            elif operator == "starts":
                where_conditions.append(f"LOWER({col_ref}) LIKE '{value}%'")
            elif operator == "ends":
                where_conditions.append(f"LOWER({col_ref}) LIKE '%{value}'")
            elif operator in ("gt", "lt", "gte", "lte"):
                op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}
                where_conditions.append(f"{col_ref} {op_map[operator]} {value}")
        return where_conditions

    @staticmethod
    def apply_to_typed_bgp(
        value_filters, var_to_column, where_conditions, dialect
    ) -> None:
        """Append compiled FILTERs to the typed BGP WHERE clause.

        Raises ``ValidationError`` when a validated FILTER variable has no
        mapped SQL column so the query never returns unfiltered ``WHERE 1=1``.
        """
        if not value_filters:
            return
        pred_to_column = {}
        for vf in value_filters:
            var_name = vf.get("variable")
            col_ref = var_to_column.get(var_name) or var_to_column.get(f"?{var_name}")
            if not col_ref:
                raise ValidationError(
                    "Spark SPARQL could not compile FILTER.",
                    detail=f"No SQL column for FILTER variable {var_name!r}.",
                )
            pred_to_column[vf["predicate_uri"]] = col_ref
        compiled = SparqlValueFilterSql.conditions_for(
            pred_to_column, value_filters, "", dialect
        )
        if len(compiled) != len(value_filters):
            raise ValidationError(
                "Spark SPARQL could not compile FILTER.",
                detail="A value FILTER did not map to a SQL predicate.",
            )
        where_conditions.extend(compiled)
