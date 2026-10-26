"""Whether the session holds a domain workspace pages can render."""

from __future__ import annotations


class WorkspacePresence:
    """Loaded-domain check shared by workspace HTML routes and the navbar.

    Mirrors ``domainIsLoaded`` in ``front/static/global/js/navbar.js``: a
    custom name (anything other than the empty-session placeholders) or
    ontology/mapping content counts as loaded.
    """

    _PLACEHOLDER_NAMES = frozenset({"NewDomain", "NewProject"})

    @staticmethod
    def is_loaded(session) -> bool:
        name = ((session.info or {}).get("name") or "").strip()
        if name and name not in WorkspacePresence._PLACEHOLDER_NAMES:
            return True
        from back.objects.domain.Domain import Domain

        stats = Domain(session).get_domain_stats()
        return (stats.get("entities") or 0) > 0 or (
            stats.get("entity_mappings") or 0
        ) > 0
