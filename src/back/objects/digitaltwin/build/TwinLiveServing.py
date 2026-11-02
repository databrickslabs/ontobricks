"""A realtime domain's Lakebase ``_live`` table: seeded and served, or emptied and retired."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)

_SLOT_COLUMNS = "map_id, source_key, predicate, subject, object"
_SEED_READ_BATCH = 5000
#: How far before the seed snapshot's commit the stream replays the topic: the
#: slot pipeline's lag between a source change and its ``_slots`` commit.
REPLAY_MARGIN_MS = 10 * 60 * 1000


class TwinLiveServing:
    """The ``_live`` lifecycle of one domain version on its Lakebase store.

    :meth:`start` creates the table (granting the stream's Postgres role),
    seeds it from the ``…_slots`` snapshot at a pinned Delta version — read on
    the warehouse in batches, at the snapshot ``seq`` — and marks it served;
    :meth:`retire` empties it and stops serving it. Both re-create the union
    view (the synced view, or the app-managed layout), so readers see the
    change at once.
    """

    @staticmethod
    def live(store: Any) -> Any:
        """The store's ``_live`` table, or ``ValidationError`` for a store without one."""
        from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase

        if not isinstance(store, LakebaseBase):
            raise ValidationError("Realtime serving needs the Lakebase graph backend")
        return store.live_table()

    @staticmethod
    def start(store: Any, domain: Any, settings: Any, client: Any) -> dict:
        """Create, seed and serve *domain*'s ``_live``.

        Returns ``{"seeded", "slots_version", "start_timestamp_ms"}``. The seed
        is ``…_slots`` ``VERSION AS OF`` its newest version, which the stream
        job reads its initial state from too (``--slots-version``); the job
        replays the topic from ``start_timestamp_ms`` — that version's commit
        time less :data:`REPLAY_MARGIN_MS` — so no change between the snapshot
        and the stream's start is lost. The overlap replays in order and
        converges: the sink keeps the newest ``seq`` of every slot.
        """
        from back.core.graphdb.delta import _table_naming
        from back.core.graphdb.delta.ChangeFeedReader import ChangeFeedReader
        from back.core.helpers import WarehouseStatements, effective_graph_name

        live = TwinLiveServing.live(store)
        graph = effective_graph_name(domain)
        live.ensure(graph, writer_role=str(getattr(settings, "realtime_lakebase_user", "") or ""))
        slots = _table_naming.slots_table_fqn(domain, settings)
        WarehouseStatements.require_fqn(slots, what="slots table name")
        if client is None:
            raise InfrastructureError("No warehouse client to read the slots snapshot")
        commit = ChangeFeedReader.last_commit(client, slots)
        try:
            rows = client.iter_rows(
                TwinLiveServing.seed_sql(slots, commit["version"]), batch_size=_SEED_READ_BATCH
            )
            seeded = live.attach(graph, rows)
        except InfrastructureError:
            raise
        except Exception as exc:  # noqa: BLE001 — driver errors, translated
            raise InfrastructureError(f"Could not seed {graph} live from {slots}", detail=str(exc)) from exc
        TwinLiveServing.refresh_view(store, graph, domain=domain, settings=settings)
        return {
            "seeded": seeded,
            "slots_version": commit["version"],
            "start_timestamp_ms": TwinLiveServing.replay_from_ms(commit["committed_at"]),
        }

    @staticmethod
    def seed_sql(slots_fqn: str, version: int) -> str:
        """The seed read: the slot rows of *slots_fqn* at Delta *version*."""
        return f"SELECT {_SLOT_COLUMNS} FROM {slots_fqn} VERSION AS OF {int(version)}"

    @staticmethod
    def replay_from_ms(committed_at: str | None, *, now_ms: int | None = None) -> int:
        """Epoch ms the stream replays from: *committed_at* (ISO 8601; else now) less the margin."""
        stamp = now_ms if now_ms is not None else int(time.time() * 1000)
        if committed_at:
            try:
                parsed = datetime.fromisoformat(str(committed_at).replace("Z", "+00:00"))
                parsed = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                stamp = int(parsed.timestamp() * 1000)
            except ValueError:
                logger.warning("Unreadable slots commit time %r: replaying from now", committed_at)
        return max(0, stamp - REPLAY_MARGIN_MS)

    @staticmethod
    def retire(store: Any, domain: Any, settings: Any = None) -> bool:
        """Empty and stop serving *domain*'s ``_live`` when it is served; True when it was."""
        from back.core.helpers import effective_graph_name

        live = TwinLiveServing.live(store)
        graph = effective_graph_name(domain)
        if not live.serving(graph):
            return False
        live.detach(graph)
        TwinLiveServing.refresh_view(store, graph, domain=domain, settings=settings)
        logger.info("Live serving of %s retired", graph)
        return True

    @staticmethod
    def serving(store: Any, domain: Any) -> bool:
        from back.core.helpers import effective_graph_name

        return TwinLiveServing.live(store).serving(effective_graph_name(domain))

    @staticmethod
    def purge(store: Any, domain: Any, older_than_hours: int) -> int:
        from back.core.helpers import effective_graph_name

        return TwinLiveServing.live(store).purge_tombstones(
            effective_graph_name(domain), older_than_hours
        )

    @staticmethod
    def refresh_view(store: Any, graph: str, *, domain: Any = None, settings: Any = None) -> None:
        """Re-create *graph*'s union view so it picks up (or drops) the ``_live`` rows.

        On a synced store the view reads the synced table the registration
        actually uses (a ghost-state fallback name included, as the build passes
        it), so re-creating it never points the view at a missing table.
        """
        try:
            if getattr(store, "is_synced", False) is True:
                store.ensure_synced_union_view(
                    graph,
                    synced_phy_override=TwinLiveServing._synced_phy_override(
                        store, graph, domain, settings
                    ),
                )
            else:
                store.create_table(graph)
        except Exception as exc:  # noqa: BLE001 — the next build or tick re-creates it
            logger.warning("Union view of %s not refreshed yet: %s", graph, exc)

    @staticmethod
    def _synced_phy_override(store: Any, graph: str, domain: Any, settings: Any) -> str:
        """The synced table's Postgres name when a fallback registration is in use, else ``""``."""
        try:
            from back.core.graphdb.lakebase.LakebaseFlatStore import (
                resolve_sync_uc_fallback_catalog,
            )
            from back.core.graphdb.lakebase.SyncedTableName import SyncedTableName

            fallback = resolve_sync_uc_fallback_catalog(
                domain, settings, getattr(domain, "delta", None) or {}
            )
            primary = store.synced_uc_name(graph, fallback_catalog=fallback)
            registered = SyncedTableName.registered(store.synced_manager(), primary)
        except Exception as exc:  # noqa: BLE001 — best-effort: the primary name is the default
            logger.debug("Synced registration of %s not resolved: %s", graph, exc)
            return ""
        return registered.rsplit(".", 1)[-1] if registered != primary else ""
