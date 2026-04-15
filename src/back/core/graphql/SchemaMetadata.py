"""GraphQL schema metadata registry and entity resolution."""
import dataclasses
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from back.core.helpers import extract_local_name as _extract_local
from back.core.logging import get_logger

from back.core.graphql.constants import DEFAULT_DEPTH, MAX_DEPTH, RDF_TYPE, RDFS_LABEL
from back.core.graphql.models import TypeInfo

logger = get_logger(__name__)


class SchemaMetadata:
    """Registry of all generated GraphQL types and their resolution logic.

    Created once during schema generation, then shared with every resolver
    via closure so they can build typed entity instances from triple-store
    rows.
    """

    def __init__(self, base_uri: str = ""):
        self.types: Dict[str, TypeInfo] = {}
        self.base_uri = base_uri

    def register(self, info: TypeInfo) -> None:
        self.types[info.name] = info

    # ------------------------------------------------------------------
    # Public resolver entry points
    # ------------------------------------------------------------------

    def resolve_list(
        self,
        store: Any,
        table: str,
        type_name: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
        depth: Optional[int] = None,
    ) -> list:
        info = self.types.get(type_name)
        if not info:
            logger.warning("GraphQL resolve_list: unknown type '%s'. Registered types: %s",
                           type_name, list(self.types.keys()))
            return []

        logger.info(
            "GraphQL resolve_list: type=%s, cls_uri=%s, table=%s, search=%s, limit=%d",
            type_name, info.cls_uri, table, search, limit,
        )

        uris = self._query_subjects(store, table, info.cls_uri, limit, offset, search)
        if not uris:
            logger.warning(
                "GraphQL resolve_list: no subjects found for type=%s (cls_uri=%s, search=%s). "
                "Try the /graphql/{domain}/debug?type_name=%s endpoint to diagnose.",
                type_name, info.cls_uri, search, type_name,
            )
            return []

        effective_depth = min(depth if depth is not None else DEFAULT_DEPTH, MAX_DEPTH)
        logger.info("GraphQL resolve_list %s: %d URIs found, depth=%d", type_name, len(uris), effective_depth)
        return self._build_entities(store, table, info, uris, depth=effective_depth)

    def resolve_single(
        self,
        store: Any,
        table: str,
        type_name: str,
        entity_id: str,
        depth: Optional[int] = None,
    ) -> Any:
        info = self.types.get(type_name)
        if not info:
            return None

        uri = self._resolve_id_to_uri(store, table, info.cls_uri, entity_id)
        if not uri:
            return None

        effective_depth = min(depth if depth is not None else DEFAULT_DEPTH, MAX_DEPTH)
        entities = self._build_entities(store, table, info, [uri], depth=effective_depth)
        return entities[0] if entities else None

    # ------------------------------------------------------------------
    # Triple-store queries
    # ------------------------------------------------------------------

    def _query_subjects(
        self,
        store: Any,
        table: str,
        cls_uri: str,
        limit: int,
        offset: int,
        search: Optional[str],
    ) -> List[str]:
        try:
            return store.find_subjects_by_type(table, cls_uri, limit, offset, search)
        except Exception as e:
            logger.error("GraphQL _query_subjects failed: %s", e)
            return []

    def _resolve_id_to_uri(
        self, store: Any, table: str, cls_uri: str, entity_id: str
    ) -> Optional[str]:
        try:
            return store.resolve_subject_by_id(table, cls_uri, entity_id)
        except Exception as e:
            logger.error("GraphQL _resolve_id_to_uri failed: %s", e)
            return None

    def _load_triples(
        self, store: Any, table: str, uris: List[str]
    ) -> Dict[str, List[Dict[str, str]]]:
        if not uris:
            return {}

        result: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        try:
            for r in store.get_triples_for_subjects(table, uris):
                result[r["subject"]].append(r)
        except Exception as e:
            logger.error("GraphQL _load_triples failed: %s", e)
        return result

    # ------------------------------------------------------------------
    # Entity construction
    # ------------------------------------------------------------------

    def _build_entities(
        self,
        store: Any,
        table: str,
        type_info: TypeInfo,
        uris: List[str],
        depth: int = 1,
    ) -> list:
        triples_by_subject = self._load_triples(store, table, uris)

        entities: List[Tuple[Any, Dict[str, List[str]]]] = []
        all_rel_targets: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

        for uri in uris:
            entity, pending = self._assemble_entity(
                type_info, uri, triples_by_subject.get(uri, [])
            )
            entities.append((entity, pending))

            if depth > 0:
                for field_name, target_uris in pending.items():
                    rel_entry = type_info.relationships.get(field_name)
                    if rel_entry:
                        _, target_type_name = rel_entry
                        for t_uri in target_uris:
                            all_rel_targets[(field_name, target_type_name)].add(t_uri)

        resolved: Dict[Tuple[str, str], Dict[str, Any]] = {}
        if depth > 0:
            for (fname, tname), t_uris in all_rel_targets.items():
                target_info = self.types.get(tname)
                if not target_info:
                    continue
                target_entities = self._build_entities(
                    store, table, target_info, list(t_uris), depth=depth - 1
                )
                uri_to_entity: Dict[str, Any] = {
                    getattr(e, "uri", ""): e for e in target_entities
                }
                resolved[(fname, tname)] = uri_to_entity

        result = []
        for entity, pending in entities:
            for fname, target_uris in pending.items():
                rel_entry = type_info.relationships.get(fname)
                if not rel_entry:
                    continue
                _, tname = rel_entry
                uri_map = resolved.get((fname, tname), {})
                related = [uri_map[u] for u in target_uris if u in uri_map]
                try:
                    object.__setattr__(entity, fname, related or None)
                except (AttributeError, TypeError):
                    pass
            result.append(entity)

        return result

    def _assemble_entity(
        self,
        type_info: TypeInfo,
        uri: str,
        triples: List[Dict[str, str]],
    ) -> Tuple[Any, Dict[str, List[str]]]:
        """Build a single entity instance and return pending relationship URIs."""
        kwargs: Dict[str, Any] = {
            "id": _extract_local(uri),
            "uri": uri,
            "label": None,
        }
        pending_rels: Dict[str, List[str]] = defaultdict(list)

        _logged_diag = False
        for t in triples:
            pred = t["predicate"]
            obj = t["object"]

            if pred == RDFS_LABEL:
                kwargs["label"] = obj
                continue

            field_name = type_info.predicate_to_field.get(pred)
            if not field_name:
                if not _logged_diag:
                    store_preds = [
                        tr["predicate"] for tr in triples
                        if tr["predicate"] != RDF_TYPE
                        and tr["predicate"] != RDFS_LABEL
                    ]
                    logger.warning(
                        "GraphQL predicate mismatch for %s (%s). "
                        "Triple-store predicates: %s  |  "
                        "Schema predicate_to_field keys: %s",
                        type_info.name, uri,
                        store_preds[:8],
                        list(type_info.predicate_to_field.keys())[:8],
                    )
                    _logged_diag = True
                continue

            if field_name in type_info.relationships:
                pending_rels[field_name].append(obj)
            else:
                kwargs[field_name] = obj

        known = {f.name for f in dataclasses.fields(type_info.gql_type)}
        filtered = {k: v for k, v in kwargs.items() if k in known}

        try:
            entity = type_info.gql_type(**filtered)
        except Exception as e:
            logger.warning(
                "GraphQL entity build failed for %s (%s): %s",
                type_info.name, uri, e,
            )
            entity = type_info.gql_type(id=_extract_local(uri), uri=uri)

        return entity, dict(pending_rels)
