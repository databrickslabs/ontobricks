"""Explorer filter preview and subgraph expand.

Extracted from :class:`DigitalTwin`. ``DigitalTwin.filter_preview`` and
``filter_expand`` remain thin delegators for ``/dtwin/sync/filter``.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Set

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.logging import get_logger
from back.core.w3c.rdf_utils import uri_local_name

logger = get_logger(__name__)

# Must match ``#sgMaxEntities`` in ``_query_sigmagraph.html``.
EXPLORER_MAX_ENTITIES_MIN = 100
EXPLORER_MAX_ENTITIES_DEFAULT = 5000
EXPLORER_MAX_ENTITIES_UI_MAX = 20_000


class GraphFilter:
    """Seed-search preview and BFS expand for the Explorer filter modal."""

    @staticmethod
    def clamp_max_entities(raw: Optional[Any]) -> int:
        """Clamp Explorer expand ``max_entities`` to the Graph Explorer dropdown range."""
        try:
            client_max = int(raw)
        except (TypeError, ValueError):
            client_max = EXPLORER_MAX_ENTITIES_DEFAULT
        return max(
            EXPLORER_MAX_ENTITIES_MIN,
            min(client_max, EXPLORER_MAX_ENTITIES_UI_MAX),
        )

    @staticmethod
    def preview(
        store: Any,
        graph_name: str,
        entity_type: str,
        field: str,
        match_type: str,
        value: str,
        max_preview: int = 500,
    ) -> Dict[str, Any]:
        """Seed-search phase: return a flat entity list for the filter modal.

        Fetches up to *max_preview* + 1 indexed entity rows so the caller can
        detect capping without an extra count query. Backends without a ready
        entity-search index retain their SPO fallback.

        Returns a dict suitable for spreading into a ``{"success": True, ...}``
        response.
        """
        probe_limit = max_preview + 1
        try:
            entity_rows = store.find_preview_seeds(
                graph_name,
                entity_type=entity_type,
                field=field,
                match_type=match_type,
                value=value,
                limit=probe_limit,
            )
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            msg = str(e)
            if "does not exist" in msg.lower():
                raise NotFoundError(
                    f"Graph {graph_name} does not exist. Run Build first.",
                    detail=msg,
                )
            raise InfrastructureError("Error querying graph", detail=msg)

        if not entity_rows:
            return {
                "phase": "preview",
                "seeds": [],
                "total": 0,
                "capped": False,
                "message": "No entities found matching the filter criteria.",
            }

        total = len(entity_rows)
        capped = total > max_preview
        preview_rows = entity_rows[:max_preview]

        seeds = [
            {
                "uri": m["uri"],
                "type": uri_local_name(m["type"]) if m["type"] else "Unknown",
                "type_uri": m["type"],
                "label": m["label"] or uri_local_name(m["uri"]),
            }
            for m in preview_rows
        ]
        seeds.sort(key=lambda s: (s["type"], s["label"]))

        logger.info(
            "Filter preview – %d seeds returned (total=%d, capped=%s)",
            len(seeds),
            total,
            capped,
        )
        return {
            "phase": "preview",
            "seeds": seeds,
            "total": total,
            "capped": capped,
        }

    @staticmethod
    def expand(
        store: Any,
        graph_name: str,
        selected_uris: List[str],
        include_rels: bool = True,
        depth: int = 3,
        max_entities: int = 5000,
        batch_size: int = 1000,
        max_triples: int = 100_000,
        max_fetch_seconds: float = 120.0,
    ) -> Dict[str, Any]:
        """BFS-expand selected URIs and return the induced subgraph triples.

        Returns a dict suitable for spreading into a ``{"success": True, ...}``
        response.
        """
        entity_set: Set[str] = set(selected_uris)
        initial_count = len(entity_set)
        capped = False

        logger.info(
            "Filter expand – %d selected URIs, depth=%d, max=%d",
            initial_count,
            depth,
            max_entities,
        )

        single_statement_expand = getattr(
            store, "expand_and_fetch_subgraph", None
        )
        if callable(single_statement_expand):
            payload = single_statement_expand(
                graph_name,
                list(entity_set),
                depth if include_rels else 0,
                max_entities,
                max_triples,
            )
            return {
                "phase": "expand",
                **payload,
                "columns": ["subject", "predicate", "object"],
                "initial_count": initial_count,
            }

        if include_rels and depth > 0:
            current_level = set(entity_set)
            for d in range(depth):
                if not current_level or len(entity_set) >= max_entities:
                    break
                logger.debug(
                    "Filter expand – level %d (%d entities so far)",
                    d + 1,
                    len(entity_set),
                )
                try:
                    neighbors = store.expand_entity_neighbors(
                        graph_name, current_level
                    )
                except Exception as e:
                    logger.warning(
                        "Expansion query at level %d failed: %s", d + 1, e
                    )
                    break
                new_entities = neighbors - entity_set
                if not new_entities:
                    break
                remaining = max_entities - len(entity_set)
                if len(new_entities) > remaining:
                    new_entities = set(list(new_entities)[:remaining])
                    capped = True
                entity_set.update(new_entities)
                if capped:
                    break
                current_level = new_entities

        logger.info(
            "Filter expand – fetching triples for %d entities "
            "(%d seed + %d expanded, capped=%s)",
            len(entity_set),
            initial_count,
            len(entity_set) - initial_count,
            capped,
        )

        subject_list = list(entity_set)
        fetch_t0 = time.monotonic()
        timeout_capped = False
        results = []
        try:
            for i in range(0, len(subject_list), batch_size):
                if (time.monotonic() - fetch_t0) > max_fetch_seconds:
                    timeout_capped = True
                    capped = True
                    logger.warning(
                        "Filter expand – capped by time budget after %d/%d entities",
                        i,
                        len(subject_list),
                    )
                    break
                batch_rows = store.get_triples_for_subjects(
                    graph_name, subject_list[i : i + batch_size]
                )
                if batch_rows:
                    results.extend(batch_rows)
                if len(results) >= max_triples:
                    results = results[:max_triples]
                    capped = True
                    break
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            logger.exception("Filter final query failed: %s", e)
            raise InfrastructureError(
                "Error fetching triples for the filter", detail=str(e)
            )

        return {
            "phase": "expand",
            "results": results,
            "columns": ["subject", "predicate", "object"],
            "count": len(results),
            "initial_count": initial_count,
            "expanded_count": len(entity_set),
            "capped": capped,
            "timeout_capped": timeout_capped,
        }
