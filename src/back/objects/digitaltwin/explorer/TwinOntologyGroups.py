"""Ontology entity groups for Sigma graph expand/collapse.

Extracted from ``api.routers.internal.dtwin.get_groups``.
"""

from __future__ import annotations


class TwinOntologyGroups:
    """Build the group payload the Explorer uses for super-nodes."""

    @staticmethod
    def list_groups(domain, default_base_uri: str) -> list[dict]:
        base_uri = domain.ontology.get("base_uri", default_base_uri).rstrip("#") + "#"
        groups = []
        for g in domain.groups:
            members = g.get("members", [])
            member_uris = [
                m if m.startswith("http") else (base_uri + m) for m in members if m
            ]
            groups.append(
                {
                    "name": g.get("name", ""),
                    "label": g.get("label", g.get("name", "")),
                    "color": g.get("color", ""),
                    "icon": g.get("icon", ""),
                    "members": members,
                    "memberUris": member_uris,
                }
            )
        return groups
