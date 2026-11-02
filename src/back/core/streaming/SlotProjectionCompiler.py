"""Compile row-local R2RML mappings into Lakeflow SDP slot-event SQL.

A *slot* is one ``(map_id, source_key, predicate)`` row of the ``_slots``
streaming table. Each row-local mapping gets a temporary view that projects
the source table's change feed into slot events, and an AUTO CDC flow that
applies those events (SCD type 1) to the slots table. Mappings that need a
join or a ``sql_query`` source stay on the batch path.
"""

import hashlib
import json
import re

from back.core.errors import ValidationError
from back.core.helpers import (
    SAFE_COL_IDENT,
    SAFE_SQL_IDENT,
    sql_escape,
    validate_table_name,
)
from back.core.logging import get_logger
from back.core.streaming.SlotPipelineSpec import SlotPipelineSpec
from back.core.w3c.sparql.constants import DIALECT_SPARK
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

logger = get_logger(__name__)

# Same placeholder grammar as ``SparqlQueryRunner.extract_r2rml_mappings``.
_TEMPLATE_COLUMN = re.compile(r'\{"?([^"{}]+)"?\}')


class SlotProjectionCompiler:
    """Static compiler from mapping dicts to slot-event SQL.

    Entity mappings are the dicts of ``extract_r2rml_mappings`` carrying a
    ``class_uri`` key (see :meth:`compile`); relationship mappings are its
    relationship dicts.
    """

    ROW_LOCAL = "row_local"
    BATCH = "batch"

    # ------------------------------------------------------------------
    # Classification and identity
    # ------------------------------------------------------------------

    @staticmethod
    def classify(mapping: dict, relationship_mappings: list | None = None) -> str:
        """Return ``"row_local"`` when *mapping* can be projected per source row.

        An entity mapping is row-local when it has a ``class_uri``, reads a
        ``table`` (no ``sql_query``), its subject template has at least one
        column, every predicate is a plain column (a ``reference`` predicate
        is a join) and all identifiers are safe to interpolate. Relationship
        mappings are always ``"batch"``: the slot key is ``(map_id, source_key,
        predicate)``, so a subject with several objects of one predicate would
        collapse onto a single slot. Everything else is ``"batch"`` too; the
        reason is logged at debug level. A row-local mapping never makes the
        emitters raise.

        *relationship_mappings* is accepted for signature stability and unused.
        """
        compiler = SlotProjectionCompiler
        reason = compiler._batch_reason(mapping)
        if reason:
            logger.debug(
                "Mapping %s is batch: %s",
                mapping.get("class_uri")
                or mapping.get("predicate")
                or compiler._source(mapping),
                reason,
            )
            return compiler.BATCH
        return compiler.ROW_LOCAL

    @staticmethod
    def map_id(mapping: dict) -> str:
        """Return the stable id of an entity *mapping*: ``map_`` + 12 hex chars of a SHA-1.

        The hash covers the source (table or ``sql_query``), the subject
        template, the class, the label column, and the predicates in canonical
        JSON, so it is stable across runs and key order, and changes when the
        mapping's output changes. (Only entity mappings are ever row-local.)
        """
        compiler = SlotProjectionCompiler
        payload = {
            "kind": "entity",
            "source": compiler._source(mapping),
            "subject": mapping.get("uri_template") or "",
            "class_uri": mapping.get("class_uri") or "",
            "label_column": mapping.get("label_column") or "",
            "predicates": mapping.get("predicates") or {},
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha1(canonical.encode("utf-8"), usedforsecurity=False)
        return "map_" + digest.hexdigest()[:12]

    @staticmethod
    def source_key_expr(mapping: dict) -> str:
        """Return the SQL expression identifying the source row of a slot.

        ``CAST(<pk> AS STRING)``; a subject template with several columns
        gives ``concat_ws('|', CAST(c1 AS STRING), CAST(c2 AS STRING))`` in
        template order.
        """
        compiler = SlotProjectionCompiler
        columns = compiler._key_columns(mapping)
        for column in columns:
            compiler._require_col(column)
        casts = [f"CAST({column} AS STRING)" for column in columns]
        if len(casts) == 1:
            return casts[0]
        return f"concat_ws('|', {', '.join(casts)})"

    # ------------------------------------------------------------------
    # SQL emission
    # ------------------------------------------------------------------

    @staticmethod
    def slot_event_view_sql(
        mapping: dict, *, base_uri: str, view_name: str, source_fqn: str
    ) -> str:
        """Return the temporary view projecting *mapping*'s change feed to slot events.

        An entity *mapping* must carry its ``class_uri`` (``rdf:type`` row).
        The view intentionally drops source rows whose key column(s) are NULL
        (``WHERE <key> IS NOT NULL``, every column of a composite key): a NULL
        key cannot form a subject URI and would leave orphan slots that the
        AUTO CDC ``KEYS`` equality can never match or delete, whereas the full
        build would emit NULL subjects for them.
        *base_uri* is accepted for signature stability and unused: subject
        templates carry their own prefix, exactly as in the full build.
        Within one commit, pre-images and deletes rank 0 and everything else
        1 (``seq_rank``): a key deleted and inserted again by one commit
        (``REPLACE WHERE``, a partial ``INSERT OVERWRITE``) ends upserted.
        """
        compiler = SlotProjectionCompiler
        compiler._require_ident(view_name, "view name")
        compiler._require_fqn(source_fqn, "source table")
        pairs = compiler._stack_pairs(mapping)
        template, id_column = mapping.get("uri_template"), mapping.get("id_column")
        compiler._require_col(id_column)
        source_key = compiler.source_key_expr(mapping)  # validates the key columns
        not_null = " AND ".join(
            f"{column} IS NOT NULL" for column in compiler._key_columns(mapping)
        )
        subject_expr = SparqlTranslator._subject_expr_from_template(
            sql_escape(template or ""), id_column, dialect=DIALECT_SPARK
        )
        stack_args = ", ".join(
            f"'{sql_escape(predicate)}', {expr}" for predicate, expr in pairs
        )
        return (
            f"CREATE TEMPORARY VIEW {view_name} AS\n"
            f"SELECT '{compiler.map_id(mapping)}' AS map_id, "
            f"{source_key} AS source_key,\n"
            f"       s.predicate, s.object, {subject_expr} AS subject,\n"
            "       CASE WHEN _change_type IN ('delete','update_preimage') "
            "OR s.object IS NULL\n"
            "            THEN 'DELETE' ELSE 'UPSERT' END AS op,\n"
            "       _commit_version,\n"
            "       CASE WHEN _change_type IN ('update_preimage','delete') THEN 0 ELSE 1 END "
            "AS seq_rank\n"
            f"FROM STREAM {source_fqn} WITH (readChangeFeed = true)\n"
            f"LATERAL VIEW stack({len(pairs)}, {stack_args}) s AS predicate, object\n"
            f"WHERE {not_null}"
        )

    @staticmethod
    def auto_cdc_flow_sql(*, flow_name: str, target_fqn: str, view_name: str) -> str:
        """Return the AUTO CDC flow applying *view_name* to the slots table.

        The clause order is binding in the Lakeflow SDP SQL grammar: ``APPLY AS
        DELETE WHEN`` precedes ``SEQUENCE BY``, and multi-column ordering is
        ``SEQUENCE BY STRUCT(a, b)``.
        """
        compiler = SlotProjectionCompiler
        compiler._require_ident(flow_name, "flow name")
        compiler._require_ident(view_name, "view name")
        compiler._require_fqn(target_fqn, "slots table")
        return (
            f"CREATE FLOW {flow_name} AS AUTO CDC INTO {target_fqn}\n"
            f"FROM STREAM({view_name})\n"
            "KEYS (map_id, source_key, predicate)\n"
            "APPLY AS DELETE WHEN op = 'DELETE'\n"
            "SEQUENCE BY STRUCT(_commit_version, seq_rank)\n"
            "COLUMNS * EXCEPT (op, _commit_version, seq_rank)\n"
            "STORED AS SCD TYPE 1"
        )

    @staticmethod
    def slots_table_sql(target_fqn: str) -> str:
        """Return the streaming-table DDL the AUTO CDC flows write into."""
        SlotProjectionCompiler._require_fqn(target_fqn, "slots table")
        return (
            f"CREATE OR REFRESH STREAMING TABLE {target_fqn}\n"
            "CLUSTER BY (predicate, subject)\n"
            "TBLPROPERTIES (delta.enableRowTracking = true, "
            "delta.enableDeletionVectors = true)"
        )

    @staticmethod
    def compile(
        entity_mappings: dict,
        relationship_mappings: list,
        *,
        base_uri: str,
        slots_fqn: str,
    ) -> SlotPipelineSpec:
        """Compile every row-local mapping; collect the rest as batch mappings.

        *entity_mappings* is the ``{class_uri: mapping}`` dict of
        ``extract_r2rml_mappings``. A TriplesMap with several classes shares
        one dict, so each pair is compiled as ``{**mapping, "class_uri":
        class_uri}``: every class gets its own ``map_id``, view
        (``slot_events_<map_id>``) and flow (``slot_flow_<map_id>``).
        Statements are ordered: the slots table, then a view and a flow per
        mapping.

        ``batch_mappings`` holds the *original* dicts (each once, even when
        several classes share it), never the class-tagged copies, so callers
        can select them by identity in the translator's ``mappings_filter``.
        """
        compiler = SlotProjectionCompiler
        statements = [compiler.slots_table_sql(slots_fqn)]
        row_local_ids: list[str] = []
        batch_mappings: list[dict] = []
        source_tables: set[str] = set()
        candidates = [
            ({**mapping, "class_uri": class_uri}, mapping)
            for class_uri, mapping in (entity_mappings or {}).items()
        ] + [(mapping, mapping) for mapping in relationship_mappings or []]
        for mapping, original in candidates:
            if compiler.classify(mapping) != compiler.ROW_LOCAL:
                if not any(original is seen for seen in batch_mappings):
                    batch_mappings.append(original)
                continue
            mapping_id = compiler.map_id(mapping)
            if mapping_id in row_local_ids:
                continue
            view_name = f"slot_events_{mapping_id}"
            statements.append(
                compiler.slot_event_view_sql(
                    mapping,
                    base_uri=base_uri,
                    view_name=view_name,
                    source_fqn=mapping["table"],
                )
            )
            statements.append(
                compiler.auto_cdc_flow_sql(
                    flow_name=f"slot_flow_{mapping_id}",
                    target_fqn=slots_fqn,
                    view_name=view_name,
                )
            )
            row_local_ids.append(mapping_id)
            source_tables.add(mapping["table"])
        logger.info(
            "Compiled slot pipeline for %s: %d row-local, %d batch mappings",
            slots_fqn,
            len(row_local_ids),
            len(batch_mappings),
        )
        return SlotPipelineSpec(
            slots_fqn=slots_fqn,
            statements=statements,
            row_local_map_ids=row_local_ids,
            batch_mappings=batch_mappings,
            source_tables=sorted(source_tables),
        )

    # ------------------------------------------------------------------
    # Mapping shape helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _batch_reason(mapping: dict) -> str | None:
        """Return why *mapping* is not row-local, or ``None`` when it is."""
        compiler = SlotProjectionCompiler
        table = mapping.get("table")
        if not (table or "").strip():
            return "no table"
        if (mapping.get("sql_query") or "").strip():
            return "sql_query source"
        if not compiler._is_safe_fqn(table):
            return "unsafe table name"
        if compiler._is_relationship(mapping):
            if not mapping.get("predicate"):
                return "relationship without predicate"
            # Multi-valued objects would collapse under the SCD1 key (map_id, subject key, predicate).
            return "relationship (several objects per subject share one slot key)"
        if not mapping.get("class_uri"):
            return "entity without class_uri"
        predicates = mapping.get("predicates") or {}
        if any(info.get("type") != "column" for info in predicates.values()):
            return "reference predicate (join)"
        if not compiler._template_columns(mapping.get("uri_template")):
            return "subject template has no column"
        columns = compiler._entity_columns(mapping)
        if not all(compiler._is_safe_col(column) for column in columns):
            return "unsafe or missing column name"
        return None

    @staticmethod
    def _is_relationship(mapping: dict) -> bool:
        return "subject_template" in mapping

    @staticmethod
    def _source(mapping: dict) -> str:
        return (mapping.get("sql_query") or "").strip() or (mapping.get("table") or "")

    @staticmethod
    def _template_columns(template: str | None) -> list[str]:
        return _TEMPLATE_COLUMN.findall(template or "")

    @staticmethod
    def _key_columns(mapping: dict) -> list:
        """Return the columns forming the source key (``None`` if one is missing).

        Every column of a composite subject template, else the subject id column.
        """
        compiler = SlotProjectionCompiler
        id_column = mapping.get("id_column")
        columns = compiler._template_columns(mapping.get("uri_template"))
        if len(columns) <= 1:
            return [id_column or (columns[0] if columns else None)]
        return columns

    @staticmethod
    def _entity_columns(mapping: dict) -> list:
        """Every column an entity slot view references (``None`` if one is missing)."""
        compiler = SlotProjectionCompiler
        columns = [
            mapping.get("id_column"),
            *compiler._template_columns(mapping.get("uri_template")),
        ]
        if mapping.get("label_column"):
            columns.append(mapping["label_column"])
        for info in (mapping.get("predicates") or {}).values():
            if info.get("type") == "column":
                columns.append(info.get("column"))
        return columns

    @staticmethod
    def _stack_pairs(mapping: dict) -> list[tuple[str, str]]:
        """Return the ``(predicate, object SQL expr)`` pairs of the ``stack()`` call."""
        compiler = SlotProjectionCompiler
        class_uri = mapping.get("class_uri")
        if not class_uri:
            raise ValidationError("Entity mapping has no class_uri")
        for column in compiler._entity_columns(mapping):
            compiler._require_col(column)
        return SparqlTranslator._generic_triples_stack_predicates_for_mapping(
            {"predicates": mapping.get("predicates") or {}},
            sql_escape(class_uri),
            mapping.get("label_column") or "",
            DIALECT_SPARK,
            None,
            None,
        )

    # ------------------------------------------------------------------
    # Identifier checks
    # ------------------------------------------------------------------

    @staticmethod
    def _is_safe_col(name) -> bool:
        return isinstance(name, str) and SAFE_COL_IDENT.fullmatch(name) is not None

    @staticmethod
    def _is_safe_fqn(name) -> bool:
        return isinstance(name, str) and SAFE_SQL_IDENT.fullmatch(name) is not None

    @staticmethod
    def _require_col(name) -> None:
        if not SlotProjectionCompiler._is_safe_col(name):
            raise ValidationError(f"Unsafe or missing column name: {name!r}")

    @staticmethod
    def _require_ident(name, what: str) -> None:
        if not SlotProjectionCompiler._is_safe_col(name):
            raise ValidationError(f"Invalid {what}: {name!r}")

    @staticmethod
    def _require_fqn(name, what: str) -> None:
        validate_table_name(name)
        if not SlotProjectionCompiler._is_safe_fqn(name):
            raise ValidationError(f"Invalid {what}: {name!r}")
