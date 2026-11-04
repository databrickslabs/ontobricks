"""Materialized-view DDL for the incremental ``_data`` / ``_graph`` relations."""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable, Iterator, Mapping
from typing import Any

from back.core.errors import ValidationError
from back.core.graphdb.delta import materialize
from back.core.graphdb.entity_search import is_missing_relation_error
from back.core.helpers import SAFE_SQL_IDENT, SQLHelpers, validate_table_name
from back.core.helpers.SQLHelpers import NON_BLANK_OBJECT_SQL
from back.core.logging import get_logger

logger = get_logger(__name__)

_REFRESH_POLICIES = ("AUTO", "INCREMENTAL", "INCREMENTAL STRICT", "FULL")

#: The warehouse's answer to ``REFRESH MATERIALIZED VIEW`` while the view's own
#: refresh (its ``TRIGGER ON UPDATE`` / schedule, or another caller's) still runs —
#: seen live: "A refresh is already running in the background. You can monitor the
#: previous refresh by visiting the following URL: …" — plus the pipelines API's
#: "An active update … already exists" (the wording :class:`SlotPipelineRunner`
#: adopts an update on) and refresh / update reported as in progress. A build
#: waits for that refresh instead of failing over to a full rebuild.
_REFRESH_IN_PROGRESS = re.compile(
    r"refresh (is |was )?(already )?(in progress|running|ongoing)"
    r"|(already|currently) (being )?refresh"
    r"|being refreshed"
    r"|concurrent(ly)? refresh"
    r"|active update .* already exists"
    r"|update .* (is )?(already )?(in progress|active)",
    re.IGNORECASE,
)
#: How long :meth:`MaterializedViewBuilder.refresh` waits for a running refresh, and the pause
#: between its attempts (a ``microbatch`` view's trigger refresh has taken minutes live).
REFRESH_WAIT_S = 600.0
REFRESH_PAUSE_S = 20.0

#: ``pipelines.externalMetadata.enabled`` is what lets ``table_changes`` / a change-feed
#: stream read the view (Databricks "external data access to streaming tables and
#: materialized views"; the change feed on a materialized view is a Beta behind it).
#: It costs nothing where the workspace does not produce the metadata yet — the
#: feed stays unreadable and the callers fall back as documented.
_MV_HEADER = (
    "CREATE OR REPLACE MATERIALIZED VIEW {fqn}\n"
    "  CLUSTER BY (predicate, subject)\n"
    "  REFRESH POLICY {policy}\n"
    "  TBLPROPERTIES ('pipelines.channel' = 'preview', "
    "'pipelines.externalMetadata.enabled' = 'true', "
    "'delta.enableRowTracking' = 'true'{extra})\n"
)

#: Refresh clauses of a ``microbatch`` data MV (CREATE MATERIALIZED VIEW reference,
#: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view).
#: ``TRIGGER ON UPDATE`` refreshes when an upstream source changes (at most once a
#: minute) and accepts no more than ten upstream sources. ``SCHEDULE EVERY`` only
#: takes hours, days or weeks, so a 15-minute schedule is a Quartz cron
#: (``seconds minutes hours day-of-month month day-of-week``); ``AT TIME ZONE`` is
#: given because creating it fails when the session has no time zone.
TRIGGER_ON_UPDATE = "TRIGGER ON UPDATE"
SCHEDULE_EVERY_15_MINUTES = "SCHEDULE CRON '0 0/15 * * * ?' AT TIME ZONE 'UTC'"
TRIGGER_ON_UPDATE_MAX_SOURCES = 10
_REFRESH_CLAUSES = ("", TRIGGER_ON_UPDATE, SCHEDULE_EVERY_15_MINUTES)

#: Table property holding the fingerprint of the data MV definition.
MV_SHA_PROPERTY = "ontobricks.mv_sql_sha256"
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")

_NON_BLANK_OBJECT = NON_BLANK_OBJECT_SQL

_INCREMENTAL_TECHNIQUES = re.compile(
    r"\b(?:ROW_BASED|GROUP_AGGREGATE|APPEND_ONLY|PARTITION_OVERWRITE"
    r"|WINDOW_FUNCTION|GENERIC_AGGREGATE|NO_OP)\b",
    re.IGNORECASE,
)
#: ``COMPLETE_RECOMPUTE`` is what the event log reports for a full recompute
#: (verified live, 2026-11-02); ``FULL_RECOMPUTE`` is the documented alias.
_FULL_RECOMPUTE = re.compile(r"\b(?:FULL|COMPLETE)_RECOMPUTE\b", re.IGNORECASE)
#: ``EXPLAIN CREATE MATERIALIZED VIEW`` on Databricks SQL opens with an
#: "Incremental Update Eligibility" section whose verdict is one of these
#: sentences (verified live on 2026-11-02); the event-log technique names
#: below are what a *refresh* reports afterwards.
_ELIGIBLE = re.compile(r"can be incrementally refreshed", re.IGNORECASE)
_NOT_ELIGIBLE = re.compile(r"can\s*not be incrementally refreshed", re.IGNORECASE)
_OR_REPLACE_MV = re.compile(r"\A\s*CREATE\s+OR\s+REPLACE\s+MATERIALIZED\s+VIEW\b", re.IGNORECASE)
_PLANNED_TECHNIQUE = re.compile(r"executed as ([A-Z_]+)")
UNKNOWN_TECHNIQUE = "UNKNOWN"


class MaterializedViewBuilder:
    """Builds and applies the ``_data`` / ``_graph`` materialized views.

    The views are *standalone* materialized views created from a SQL
    warehouse, hence ``CREATE OR REPLACE MATERIALIZED VIEW`` (``CREATE OR
    REFRESH`` is pipeline-only). Their SQL is shaped so Enzyme can refresh
    them incrementally: plain projections, ``UNION ALL`` and a ``GROUP BY``.

    The only I/O is the ``client`` handed to :meth:`apply` and
    :meth:`last_refresh_technique` (``execute_statement`` / ``execute_query``).
    """

    @staticmethod
    def data_mv_sql(
        data_fqn: str,
        *,
        slots_fqn: str | None = None,
        batch_branch_sql: str | None = None,
        refresh_policy: str = "INCREMENTAL",
        refresh_clause: str = "",
    ) -> str:
        """``CREATE OR REPLACE MATERIALIZED VIEW`` for ``_data``.

        The view unions up to two sides, either of which may be omitted by
        passing ``None`` (``ValidationError`` when both are):

        * the **slots** table, ``slots_fqn``: row-local mappings streamed
          through AUTO CDC;
        * the **batch branch**, ``batch_branch_sql``: the translator's
          ``incremental=True`` SQL for the join / aggregate mappings. It is
          parenthesised so a leading ``WITH`` stays valid after ``UNION ALL``.

        Absorbs three facts about the slots table:

        * It is an AUTO CDC SCD TYPE 1 target: deletes are *physical*, so
          there is no ``deleted`` column and the slots side reads with no
          tombstone filter.
        * Classes that share one TriplesMap each emit their own copy of the
          attribute slots. The outer ``GROUP BY subject, predicate, object``
          is what collapses those copies into one triple.
        * Attribute objects are never NULL (the reused helper wraps them in
          ``COALESCE(..., '')``), so an unset attribute arrives as ``''``.
          The ``TRIM(object) != ''`` filter therefore stays in the slots
          select, ahead of the grouping, to drop those empty objects (the
          gateway VIEW does the same). The batch SQL carries its own filter.

        ``object_hash`` (``sha2(object, 256)``) is the Lakeflow sync key.

        The ``ontobricks.mv_sql_sha256`` table property records
        :meth:`data_mv_sha` so a later build can tell an unchanged definition
        (see :meth:`existing_mv_sha`) and refresh instead of replacing.

        *refresh_clause* (:meth:`microbatch_refresh_clause`, ``""`` for none)
        goes on its own line between the table properties and ``AS``; it is
        part of the fingerprint, so switching mode replaces the view.
        """
        validate_table_name(data_fqn)
        policy = MaterializedViewBuilder._policy(refresh_policy)
        clause = MaterializedViewBuilder._clause(refresh_clause)
        body = MaterializedViewBuilder._data_mv_body(slots_fqn, batch_branch_sql)
        sha = MaterializedViewBuilder._sha(policy, body, clause)
        extra = f", '{MV_SHA_PROPERTY}' = '{sha}'"
        header = _MV_HEADER.format(fqn=data_fqn, policy=policy, extra=extra)
        return header + (f"  {clause}\n" if clause else "") + body

    @staticmethod
    def data_mv_sha(
        *,
        slots_fqn: str | None = None,
        batch_branch_sql: str | None = None,
        refresh_policy: str = "INCREMENTAL",
        refresh_clause: str = "",
    ) -> str:
        """SHA-256 of the data MV definition: header template, policy, clause and body.

        The target name is not part of it. The header template (CLUSTER BY,
        table properties), the refresh policy and the refresh clause are, so
        changing any of them replaces existing views on their next build.
        """
        policy = MaterializedViewBuilder._policy(refresh_policy)
        clause = MaterializedViewBuilder._clause(refresh_clause)
        body = MaterializedViewBuilder._data_mv_body(slots_fqn, batch_branch_sql)
        return MaterializedViewBuilder._sha(policy, body, clause)

    @staticmethod
    def microbatch_refresh_clause(source_count: int | None) -> str:
        """How a ``microbatch`` data MV refreshes between builds.

        ``TRIGGER ON UPDATE`` when it reads at most
        :data:`TRIGGER_ON_UPDATE_MAX_SOURCES` relations, otherwise (or when the
        count is unknown) every 15 minutes (:data:`SCHEDULE_EVERY_15_MINUTES`).
        """
        if source_count is not None and source_count <= TRIGGER_ON_UPDATE_MAX_SOURCES:
            return TRIGGER_ON_UPDATE
        return SCHEDULE_EVERY_15_MINUTES

    @staticmethod
    def existing_mv_sha(client: Any, fqn: str, *, strict: bool = False) -> str:
        """The ``ontobricks.mv_sql_sha256`` of *fqn*, or ``""``.

        ``""`` when the relation is absent, has no such property (a TABLE or
        VIEW left by a full build), the name is invalid, or anything else goes
        wrong: the caller then simply re-creates the view. Never raises —
        unless *strict*, where a lookup that fails for another reason than an
        absent relation raises (an unknown answer is not "no fingerprint").
        """
        try:
            validate_table_name(fqn)
            if SAFE_SQL_IDENT.fullmatch(fqn) is None:
                return ""
            rows = client.execute_query(f"SHOW TBLPROPERTIES {fqn} ('{MV_SHA_PROPERTY}')")
            for row in rows or []:
                if not isinstance(row, Mapping):
                    continue
                # Filtered by key, Databricks SQL answers a single ``value``
                # column (verified live, 2026-11-02); unfiltered answers carry
                # ``key`` + ``value`` — accept both shapes.
                if "key" in row and row.get("key") != MV_SHA_PROPERTY:
                    continue
                value = str(row.get("value") or "").strip()
                return value if _SHA256_HEX.fullmatch(value) else ""
        except Exception as exc:  # noqa: BLE001 — contract: never raises unless strict
            if strict and not is_missing_relation_error(exc):
                raise
            logger.debug("No materialized view fingerprint for %s: %s", fqn, exc)
        return ""

    @staticmethod
    def graph_mv_sql(
        graph_fqn: str,
        data_fqn: str,
        inferred_fqn: str,
        *,
        refresh_policy: str = "INCREMENTAL",
        refresh_clause: str = "",
    ) -> str:
        """``CREATE OR REPLACE MATERIALIZED VIEW`` for ``_graph``.

        A plain ``UNION ALL`` of ``_data`` and ``_inferred``, as the union
        VIEW it replaces. ``_inferred`` needs row tracking for Enzyme to read
        it incrementally (see :meth:`apply`). Like the data MV it carries its
        definition fingerprint (:meth:`graph_mv_sha`), which is also how a
        graph MV is told from the union VIEW.
        """
        validate_table_name(graph_fqn)
        validate_table_name(data_fqn)
        validate_table_name(inferred_fqn)
        policy = MaterializedViewBuilder._policy(refresh_policy)
        clause = MaterializedViewBuilder._clause(refresh_clause)
        body = MaterializedViewBuilder._graph_mv_body(data_fqn, inferred_fqn)
        extra = f", '{MV_SHA_PROPERTY}' = '{MaterializedViewBuilder._sha(policy, body, clause)}'"
        header = _MV_HEADER.format(fqn=graph_fqn, policy=policy, extra=extra)
        return header + (f"  {clause}\n" if clause else "") + body

    @staticmethod
    def graph_mv_sha(
        data_fqn: str,
        inferred_fqn: str,
        *,
        refresh_policy: str = "INCREMENTAL",
        refresh_clause: str = "",
    ) -> str:
        """SHA-256 of the graph MV definition (header template, policy, clause and body)."""
        policy = MaterializedViewBuilder._policy(refresh_policy)
        clause = MaterializedViewBuilder._clause(refresh_clause)
        body = MaterializedViewBuilder._graph_mv_body(data_fqn, inferred_fqn)
        return MaterializedViewBuilder._sha(policy, body, clause)

    @staticmethod
    def _graph_mv_body(data_fqn: str, inferred_fqn: str) -> str:
        return (
            f"AS SELECT subject, predicate, object FROM {data_fqn}\n"
            "UNION ALL\n"
            f"SELECT subject, predicate, object FROM {inferred_fqn}"
        )

    @staticmethod
    def explain_sql(mv_sql: str) -> str:
        """``EXPLAIN CREATE MATERIALIZED VIEW …``: how a refresh of *mv_sql* would run.

        The documented statement has no ``OR REPLACE``, so a ``CREATE OR
        REPLACE MATERIALIZED VIEW`` (what :meth:`data_mv_sql` emits) is
        explained as its ``CREATE``.
        """
        return "EXPLAIN " + _OR_REPLACE_MV.sub("CREATE MATERIALIZED VIEW", mv_sql, count=1)

    @staticmethod
    def refresh_sql(fqn: str, *, full: bool = False) -> str:
        """``REFRESH MATERIALIZED VIEW`` (``… FULL`` forces a full recompute)."""
        validate_table_name(fqn)
        return f"REFRESH MATERIALIZED VIEW {fqn}" + (" FULL" if full else "")

    @staticmethod
    def refresh(
        client: Any,
        fqn: str,
        *,
        full: bool = False,
        wait_s: float = REFRESH_WAIT_S,
        pause_s: float = REFRESH_PAUSE_S,
        sleep: Callable[[float], None] = time.sleep,
    ) -> int:
        """Run :meth:`refresh_sql`, waiting for a refresh that already runs; the attempts made.

        A ``microbatch`` view refreshes on its own (``TRIGGER ON UPDATE``) when the
        slots change, and the warehouse refuses a second refresh while one runs
        (:data:`_REFRESH_IN_PROGRESS`). That answer is retried every *pause_s*
        seconds for up to *wait_s*; any other error, or the wait running out,
        propagates (the caller's fallback decides).
        """
        sql = MaterializedViewBuilder.refresh_sql(fqn, full=full)
        attempts = max(1, int(wait_s // pause_s))
        for attempt in range(1, attempts + 1):
            try:
                materialize.run_sql(client, sql)
                return attempt
            except Exception as exc:  # noqa: BLE001 — only the in-progress answer is retried
                if attempt == attempts or not _REFRESH_IN_PROGRESS.search(str(exc)):
                    raise
                logger.info(
                    "A refresh of %s is already running; waiting %.0fs (attempt %d/%d): %s",
                    fqn,
                    pause_s,
                    attempt,
                    attempts,
                    exc,
                )
                sleep(pause_s)
        return attempts

    @staticmethod
    def refresh_technique_sql(fqn: str) -> str:
        """Latest ``planning_information`` event of the view's event log."""
        validate_table_name(fqn)
        return (
            "SELECT timestamp, message "
            f"FROM event_log(TABLE({fqn})) "
            "WHERE event_type = 'planning_information' "
            "ORDER BY timestamp DESC LIMIT 1"
        )

    @staticmethod
    def plan_is_incremental(explain_rows: Any) -> bool:
        """True when an ``EXPLAIN`` result plans an incremental refresh.

        Scans every value of every row. The eligibility verdict wins: "cannot
        be incrementally refreshed" (or ``FULL_RECOMPUTE`` anywhere) means
        false, "can be incrementally refreshed" means true; otherwise any
        event-log technique name means true. An empty or unrecognised plan is
        false.
        """
        text = "\n".join(MaterializedViewBuilder._texts(explain_rows))
        if _NOT_ELIGIBLE.search(text) or _FULL_RECOMPUTE.search(text):
            return False
        if _ELIGIBLE.search(text):
            return True
        return _INCREMENTAL_TECHNIQUES.search(text) is not None

    @staticmethod
    def apply(
        client: Any,
        *,
        data_fqn: str,
        graph_fqn: str,
        inferred_fqn: str,
        slots_fqn: str | None,
        batch_branch_sql: str | None,
        drop_existing_table: bool,
        refresh_policy: str = "INCREMENTAL",
        with_graph_mv: bool = False,
        refresh_clause: str = "",
        graph_refresh_clause: str = "",
    ) -> dict:
        """Create or replace the ``_data`` materialized view and the ``_graph`` relation.

        ``_graph`` is the union of ``_data`` and ``_inferred``. By default
        (*with_graph_mv* false) it stays the plain union VIEW
        (``materialize.ensure_graph_view``); with *with_graph_mv* it becomes a
        materialized view too (*graph_refresh_clause*: its own refresh
        clause), which the inferred reset and the Delta store's app writes
        then refresh instead of replacing (``GraphRelation``).

        Every statement is built (and validated) before anything runs. Then,
        in order:

        1. ``DROP TABLE IF EXISTS <data>`` when *drop_existing_table*: an
           existing ``_data`` TABLE blocks ``CREATE MATERIALIZED VIEW``.
        2. ``DROP VIEW IF EXISTS <data>``, always: view-mode Lakehouse domains
           (``materialize.apply_data_relation``) keep ``_data`` as a plain
           VIEW, which ``CREATE OR REPLACE MATERIALIZED VIEW`` cannot replace.
           Without it such a domain could never enter incremental mode.
        3. The ``_graph`` relation of the other kind: ``DROP VIEW`` before a
           graph MV, ``DROP MATERIALIZED VIEW`` before the union VIEW.
           All the drops are best-effort (``materialize.drop_relation``): a
           relation that is absent, or of another kind, makes the drop fail
           harmlessly and ``CREATE OR REPLACE`` takes over.
        4. the ``_data`` view;
        5. the ``_inferred`` companion (``CREATE TABLE IF NOT EXISTS``);
        6. graph MV only: ``ALTER TABLE <inferred> SET TBLPROPERTIES`` for row
           tracking and deletion vectors, so a companion created before
           incremental mode becomes readable incrementally too;
        7. the ``_graph`` materialized view, or the union VIEW.

        Returns ``{"data_mv": data_fqn, "graph_mv": graph_fqn}`` with a graph
        MV, ``{"data_mv": data_fqn, "graph_view": graph_fqn}`` otherwise.
        """
        data_sql = MaterializedViewBuilder.data_mv_sql(
            data_fqn,
            slots_fqn=slots_fqn,
            batch_branch_sql=batch_branch_sql,
            refresh_policy=refresh_policy,
            refresh_clause=refresh_clause,
        )
        if with_graph_mv:
            graph_sql = MaterializedViewBuilder.graph_mv_sql(
                graph_fqn,
                data_fqn,
                inferred_fqn,
                refresh_policy=refresh_policy,
                refresh_clause=graph_refresh_clause,
            )
            enable_tracking_sql = materialize.build_enable_row_tracking_sql(inferred_fqn)
        else:
            # Validates the three names; ensure_graph_view runs the same statement.
            materialize.build_union_view_sql(graph_fqn, data_fqn, inferred_fqn)

        if drop_existing_table:
            materialize.drop_relation(client, data_fqn, kind="table")
        materialize.drop_relation(client, data_fqn, kind="view")
        materialize.drop_relation(
            client, graph_fqn, kind="view" if with_graph_mv else "materialized_view"
        )
        logger.info("Creating the data materialized view %s", data_fqn)
        materialize.run_sql(client, data_sql)
        MaterializedViewBuilder.sync_external_metadata(client, data_fqn)
        materialize.ensure_inferred_table(client, inferred_fqn)
        if not with_graph_mv:
            materialize.ensure_graph_view(client, graph_fqn, data_fqn, inferred_fqn)
            return {"data_mv": data_fqn, "graph_view": graph_fqn}
        materialize.run_sql(client, enable_tracking_sql)
        logger.info("Creating the graph materialized view %s", graph_fqn)
        materialize.run_sql(client, graph_sql)
        MaterializedViewBuilder.sync_external_metadata(client, graph_fqn)
        return {"data_mv": data_fqn, "graph_mv": graph_fqn}

    @staticmethod
    def sync_external_metadata(client: Any, fqn: str) -> bool:
        """``REPAIR TABLE <fqn> SYNC METADATA`` — the one-time step a materialized
        view's change data feed needs (``table_changes``, ``DESCRIBE HISTORY``,
        ``VERSION AS OF`` on the view).

        Best-effort: on a workspace whose pipelines have no external metadata
        the statement fails with ``REPAIR_TABLE_REQUIRES_EXTERNAL_METADATA``
        (verified live, 2026-11-02) and the change-feed consumers fall back
        (``cdf_unavailable``); returns whether the sync succeeded.
        """
        try:
            materialize.run_sql(client, f"REPAIR TABLE {fqn} SYNC METADATA")
        except Exception as exc:  # noqa: BLE001 — preview feature; consumers fall back
            logger.info(
                "External metadata not synced for %s (%s); change-feed reads on the view "
                "will fall back",
                fqn,
                str(exc).splitlines()[0][:200],
            )
            return False
        return True

    @staticmethod
    def last_refresh_technique(client: Any, fqn: str) -> str:
        """Technique of the latest planned refresh, e.g. ``ROW_BASED``.

        Reads the ``planning_information`` message (``"... planned to be
        executed as ROW_BASED ..."``). Returns ``"UNKNOWN"`` when there is no
        event, the message does not parse, or anything at all goes wrong:
        the technique is diagnostic, so this never raises.
        """
        try:
            rows = client.execute_query(MaterializedViewBuilder.refresh_technique_sql(fqn))
            row = rows[0] if rows else None
            message = row.get("message") if isinstance(row, Mapping) else None
            match = _PLANNED_TECHNIQUE.search(str(message)) if message else None
            return match.group(1) if match else UNKNOWN_TECHNIQUE
        except Exception as exc:  # noqa: BLE001 — contract: never raises
            logger.debug("No refresh technique for %s: %s", fqn, exc)
            return UNKNOWN_TECHNIQUE

    @staticmethod
    def _data_mv_body(slots_fqn: str | None, batch_branch_sql: str | None) -> str:
        """The ``AS SELECT … GROUP BY`` part of the data MV (see :meth:`data_mv_sql`)."""
        batch = (batch_branch_sql or "").strip()
        if slots_fqn is None and not batch:
            raise ValidationError(
                "A data materialized view needs a slots table, a batch branch, or both"
            )
        branches = []
        if slots_fqn is not None:
            validate_table_name(slots_fqn)
            branches.append(
                f"  SELECT subject, predicate, object FROM {slots_fqn}\n  {_NON_BLANK_OBJECT}"
            )
        if batch:
            branches.append(f"  ({batch})" if slots_fqn is not None else f"  {batch}")
        return (
            f"AS SELECT subject, predicate, object, {SQLHelpers.object_hash_sql('object')}"
            " AS object_hash\n"
            "FROM (\n" + "\n  UNION ALL\n".join(branches) + "\n) t\n"
            "GROUP BY subject, predicate, object"
        )

    @staticmethod
    def _sha(policy: str, body: str, clause: str = "") -> str:
        """Fingerprint input: without a clause exactly what it was before clauses existed."""
        text = f"{_MV_HEADER}\n{policy}\n{body}" + (f"\n{clause}" if clause else "")
        return hashlib.sha256(text.encode()).hexdigest()

    @staticmethod
    def _clause(refresh_clause: str) -> str:
        """*refresh_clause* when it is one of ours (it is inlined), else ValidationError."""
        clause = (refresh_clause or "").strip()
        if clause not in _REFRESH_CLAUSES:
            raise ValidationError(f"Unsupported materialized view refresh clause {clause!r}")
        return clause

    @staticmethod
    def _policy(refresh_policy: str) -> str:
        """Upper-cased policy, restricted to the documented set (it is inlined)."""
        policy = " ".join(str(refresh_policy).upper().split())
        if policy not in _REFRESH_POLICIES:
            raise ValidationError(
                f"Unsupported refresh policy {refresh_policy!r}; "
                f"expected one of {', '.join(_REFRESH_POLICIES)}"
            )
        return policy

    @staticmethod
    def _texts(value: Any) -> Iterator[str]:
        """Every string inside *value*, descending into rows (dict / list / tuple)."""
        if isinstance(value, str):
            yield value
        elif isinstance(value, Mapping):
            for item in value.values():
                yield from MaterializedViewBuilder._texts(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from MaterializedViewBuilder._texts(item)
