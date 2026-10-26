"""Legacy quality-check SQL builders against the VIEW triple store.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

from typing import Optional

from back.core.helpers import LIKE_ESCAPE, sql_like_escape
from back.core.helpers import sql_escape as escape_sql_value
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL


class QualitySqlBuilder:
    """Build SQL for SHACL-style cardinality, value, property, label, orphan, and SWRL checks."""

    _swrl_sql_translator = None

    @staticmethod
    def build_quality_sql(check_type: str, table: str, params: dict) -> Optional[str]:
        """Build SQL for a quality check against the triple store table."""
        if check_type == "cardinality":
            return QualitySqlBuilder._build_cardinality_sql(table, params)
        elif check_type == "value":
            return QualitySqlBuilder._build_value_sql(table, params)
        elif check_type in (
            "functional",
            "inverseFunctional",
            "symmetric",
            "asymmetric",
            "irreflexive",
        ):
            return QualitySqlBuilder._build_property_sql(table, check_type, params)
        elif check_type == "requireLabels":
            return QualitySqlBuilder._build_require_labels_sql(table, params)
        elif check_type == "noOrphans":
            return QualitySqlBuilder._build_no_orphans_sql(table, params)
        elif check_type == "swrl":
            return QualitySqlBuilder._build_swrl_sql(table, params)
        return None

    @staticmethod
    def _build_cardinality_sql(table, params):
        class_uri = escape_sql_value(params.get("class_uri", ""))
        property_uri = escape_sql_value(params.get("property_uri", ""))
        constraint_type = params.get("constraint_type", "")
        cardinality_value = int(params.get("cardinality_value", 0))
        if not class_uri or not property_uri:
            return None
        if constraint_type == "minCardinality":
            having = f"HAVING COUNT(t2.object) < {cardinality_value}"
        elif constraint_type == "maxCardinality":
            having = f"HAVING COUNT(t2.object) > {cardinality_value}"
        elif constraint_type == "exactCardinality":
            having = f"HAVING COUNT(t2.object) != {cardinality_value}"
        else:
            return None
        return (
            f"SELECT t1.subject AS s, COUNT(t2.object) AS count\n"
            f"FROM {table} t1\n"
            f"JOIN {table} t2\n"
            f"  ON t1.subject = t2.subject\n"
            f"  AND t2.predicate = '{property_uri}'\n"
            f"WHERE t1.predicate = '{RDF_TYPE}'\n"
            f"  AND t1.object = '{class_uri}'\n"
            f"GROUP BY t1.subject\n"
            f"{having}"
        )

    @staticmethod
    def _build_value_sql(table, params):
        class_uri = escape_sql_value(params.get("class_uri", ""))
        attribute_uri = escape_sql_value(params.get("attribute_uri", ""))
        value_check_type = params.get("value_check_type", "")
        check_value = escape_sql_value(params.get("check_value", ""))
        if not class_uri or not attribute_uri:
            return None
        if value_check_type == "notNull":
            return (
                f"SELECT t1.subject AS s\n"
                f"FROM {table} t1\n"
                f"LEFT JOIN {table} t2\n"
                f"  ON t1.subject = t2.subject\n"
                f"  AND t2.predicate = '{attribute_uri}'\n"
                f"WHERE t1.predicate = '{RDF_TYPE}'\n"
                f"  AND t1.object = '{class_uri}'\n"
                f"  AND t2.subject IS NULL"
            )
        like_value = escape_sql_value(sql_like_escape(params.get("check_value", "")))
        if value_check_type == "startsWith":
            filter_clause = (
                f"AND NOT LOWER(t2.object) LIKE LOWER('{like_value}%') {LIKE_ESCAPE}"
            )
        elif value_check_type == "endsWith":
            filter_clause = (
                f"AND NOT LOWER(t2.object) LIKE LOWER('%{like_value}') {LIKE_ESCAPE}"
            )
        elif value_check_type == "contains":
            filter_clause = (
                f"AND NOT LOWER(t2.object) LIKE LOWER('%{like_value}%') {LIKE_ESCAPE}"
            )
        elif value_check_type == "equals":
            filter_clause = f"AND LOWER(t2.object) != LOWER('{check_value}')"
        elif value_check_type == "notEquals":
            filter_clause = f"AND LOWER(t2.object) = LOWER('{check_value}')"
        elif value_check_type == "matches":
            filter_clause = f"AND NOT t2.object RLIKE '{check_value}'"
        else:
            return None
        return (
            f"SELECT t1.subject AS s, t2.object AS val\n"
            f"FROM {table} t1\n"
            f"JOIN {table} t2\n"
            f"  ON t1.subject = t2.subject\n"
            f"  AND t2.predicate = '{attribute_uri}'\n"
            f"WHERE t1.predicate = '{RDF_TYPE}'\n"
            f"  AND t1.object = '{class_uri}'\n"
            f"  {filter_clause}"
        )

    @staticmethod
    def _build_property_sql(table, check_type, params):
        property_uri = escape_sql_value(params.get("property_uri", ""))
        if not property_uri:
            return None
        if check_type == "functional":
            return (
                f"SELECT subject AS s, COUNT(object) AS count\n"
                f"FROM {table}\n"
                f"WHERE predicate = '{property_uri}'\n"
                f"GROUP BY subject\n"
                f"HAVING COUNT(object) > 1"
            )
        if check_type == "inverseFunctional":
            return (
                f"SELECT object AS o, COUNT(subject) AS count\n"
                f"FROM {table}\n"
                f"WHERE predicate = '{property_uri}'\n"
                f"GROUP BY object\n"
                f"HAVING COUNT(subject) > 1"
            )
        if check_type == "symmetric":
            return (
                f"SELECT t1.subject AS s, t1.object AS o\n"
                f"FROM {table} t1\n"
                f"LEFT JOIN {table} t2\n"
                f"  ON t1.subject = t2.object\n"
                f"  AND t1.object = t2.subject\n"
                f"  AND t2.predicate = '{property_uri}'\n"
                f"WHERE t1.predicate = '{property_uri}'\n"
                f"  AND t2.subject IS NULL"
            )
        if check_type == "asymmetric":
            return (
                f"SELECT t1.subject AS s, t1.object AS o\n"
                f"FROM {table} t1\n"
                f"JOIN {table} t2\n"
                f"  ON t1.subject = t2.object\n"
                f"  AND t1.object = t2.subject\n"
                f"  AND t2.predicate = '{property_uri}'\n"
                f"WHERE t1.predicate = '{property_uri}'"
            )
        if check_type == "irreflexive":
            return (
                f"SELECT subject AS s\n"
                f"FROM {table}\n"
                f"WHERE predicate = '{property_uri}'\n"
                f"  AND subject = object"
            )
        return None

    @staticmethod
    def _build_require_labels_sql(table, params):
        return (
            f"SELECT t1.subject AS s\n"
            f"FROM {table} t1\n"
            f"LEFT JOIN {table} t2\n"
            f"  ON t1.subject = t2.subject\n"
            f"  AND t2.predicate = '{RDFS_LABEL}'\n"
            f"WHERE t1.predicate = '{RDF_TYPE}'\n"
            f"  AND t2.subject IS NULL"
        )

    @staticmethod
    def _build_no_orphans_sql(table, params):
        return (
            f"SELECT t1.subject AS s\n"
            f"FROM {table} t1\n"
            f"WHERE t1.predicate = '{RDF_TYPE}'\n"
            f"  AND NOT EXISTS (\n"
            f"    SELECT 1 FROM {table} t2\n"
            f"    WHERE t2.subject = t1.subject\n"
            f"      AND t2.predicate != '{RDF_TYPE}'\n"
            f"      AND t2.predicate != '{RDFS_LABEL}'\n"
            f"  )"
        )

    @staticmethod
    def _get_swrl_translator():
        if QualitySqlBuilder._swrl_sql_translator is None:
            from back.core.reasoning import SWRLSQLTranslator

            QualitySqlBuilder._swrl_sql_translator = SWRLSQLTranslator()
        return QualitySqlBuilder._swrl_sql_translator

    @staticmethod
    def _build_swrl_sql(table, params):
        return QualitySqlBuilder._get_swrl_translator().build_violation_sql(
            table, params
        )
