"""The SQL that projects a mapped source row into triples: subjects, casts, ``stack()`` pairs.

Shared by the SPARQL translator and the slot pipeline compiler
(``SlotProjectionCompiler``). Kept out of ``back.core.w3c`` (whose package
imports ``rdflib``), so the realtime stream job imports the slot projection
without the SPARQL / OWL stack.
"""

from __future__ import annotations

from back.core.helpers.SQLHelpers import SQL_DIALECT_SPARK as DIALECT_SPARK
from back.core.helpers.SQLHelpers import SQLHelpers
from back.core.helpers.URIHelpers import URIHelpers

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


class TripleProjectionSql:
    """Spark / Postgres expressions of a mapping's triples, independent of the query parser."""

    @staticmethod
    def string_type(dialect: str = DIALECT_SPARK) -> str:
        """The string type name for the dialect."""
        return "STRING"

    @staticmethod
    def cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """``TRY_CAST(<expr> AS STRING)`` (ANSI-safe stringification)."""
        return SQLHelpers.sql_cast(expr, TripleProjectionSql.string_type(dialect))

    @staticmethod
    def coalesce_cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """``COALESCE(TRY_CAST(<expr> AS STRING), '')``."""
        return f"COALESCE({TripleProjectionSql.cast_str(expr, dialect)}, '')"

    @staticmethod
    def predicate_local_name_key(uri: str) -> str:
        """Lower-case RDF local name for SQL filter / column-key matching."""
        return URIHelpers.extract_local_name(uri).lower()

    @staticmethod
    def subject_expr_from_template(
        uri_template: str,
        id_column: str,
        alias: str = "",
        dialect: str = DIALECT_SPARK,
    ) -> str:
        """A subject expression from a URI template, like ``CONCAT(base, TRY_CAST(col AS STRING))``."""
        col_ref = f"{alias}.{id_column}" if alias else id_column
        if uri_template and "{" in uri_template:
            base_uri = uri_template.split("{")[0]
            return f"CONCAT('{base_uri}', {TripleProjectionSql.cast_str(col_ref, dialect)})"
        return TripleProjectionSql.cast_str(col_ref, dialect)

    @staticmethod
    def predicate_output_allowed(pred_uri, predicate_filter_set, predicate_filter_local) -> bool:
        """True when *pred_uri* passes the query's predicate filter (``None``: no filter)."""
        if predicate_filter_set is None:
            return True
        if pred_uri.lower() in predicate_filter_set:
            return True
        return bool(
            predicate_filter_local
            and TripleProjectionSql.predicate_local_name_key(pred_uri) in predicate_filter_local
        )

    @staticmethod
    def stack_predicates_for_mapping(
        mapping,
        class_uri,
        label_column,
        dialect,
        predicate_filter_set,
        predicate_filter_local,
    ) -> list:
        """``(predicate, object SQL)`` pairs of an entity mapping: type, label, column attributes."""
        allowed = TripleProjectionSql.predicate_output_allowed
        predicates = []
        if allowed(_RDF_TYPE, predicate_filter_set, predicate_filter_local):
            predicates.append((_RDF_TYPE, f"'{class_uri}'"))
        if label_column and allowed(_RDFS_LABEL, predicate_filter_set, predicate_filter_local):
            predicates.append(
                (_RDFS_LABEL, TripleProjectionSql.coalesce_cast_str(label_column, dialect))
            )
        for pred_uri, pred_info in mapping.get("predicates", {}).items():
            if pred_info.get("type") == "column":
                column = pred_info["column"]
                if column != label_column and allowed(
                    pred_uri, predicate_filter_set, predicate_filter_local
                ):
                    predicates.append(
                        (pred_uri, TripleProjectionSql.coalesce_cast_str(column, dialect))
                    )
        return predicates
