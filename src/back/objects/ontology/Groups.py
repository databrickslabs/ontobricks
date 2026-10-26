"""Entity-group CRUD extracted from :class:`Ontology`.

Fowler Extract Class. Distinct from Explorer Sigma ``TwinOntologyGroups``.
``Ontology`` keeps one-line delegators.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from back.core.errors import ValidationError

if TYPE_CHECKING:
    from back.objects.session.DomainSession import DomainSession

class Groups:
    """owl:unionOf-style entity groups on the domain session."""

    def __init__(self, session: "DomainSession") -> None:
        self._domain = session

    def save_group(self, group: Dict, index: int = -1) -> List[Dict]:
        """Create or update an entity group.

        Args:
            group: Group dict with keys *name*, *label*, *description*, *color*,
                   *icon*, *members*.
            index: When ``>= 0`` the group at that position is replaced;
                   otherwise a new group is appended (duplicate names are rejected).

        Returns:
            The updated list of all groups.

        Raises:
            ValidationError: if the name is missing or already exists (on create).
        """
        name = (group.get("name") or "").strip()
        if not name:
            raise ValidationError("Group name is required")

        groups = self._domain.groups
        was_update = 0 <= index < len(groups)
        old = dict(groups[index]) if was_update else {}

        if was_update:
            groups[index] = group
        else:
            if any(g.get("name") == name for g in groups):
                raise ValidationError(f'Group "{name}" already exists')
            groups.append(group)

        self._enforce_exclusive_membership(groups, name)
        self._sync_class_group_field(groups)
        self._domain.groups = groups
        self._domain.record_change(
            "group_updated" if was_update else "group_added",
            entity_type="group", entity_ref=name, summary=name,
            meta=self._domain.diff_meta(old, group),
        )
        self._domain.save()
        return self._domain.groups

    def delete_group(self, *, index: int = -1, name: str = "") -> List[Dict]:
        """Delete an entity group by *index* or *name*.

        Returns:
            The updated list of all groups.

        Raises:
            ValidationError: if neither *index* nor *name* identifies a group.
        """
        groups = self._domain.groups

        if 0 <= index < len(groups):
            removed = dict(groups[index])
            removed_ref = removed.get("name", "") or str(index)
            groups.pop(index)
        elif name:
            removed = next(
                (dict(g) for g in groups if g.get("name") == name),
                {},
            )
            removed_ref = name
            groups[:] = [g for g in groups if g.get("name") != name]
        else:
            raise ValidationError("Provide index or name to identify the group")

        self._sync_class_group_field(groups)
        self._domain.groups = groups
        self._domain.record_change(
            "group_removed", entity_type="group",
            entity_ref=removed_ref, summary=removed_ref,
            meta=self._domain.diff_meta(removed or {}, {}),
        )
        self._domain.save()
        return self._domain.groups

    def update_group_members(
        self, group_name: str, *, add: List[str] = None, remove: List[str] = None
    ) -> List[Dict]:
        """Add or remove members from the group identified by *group_name*.

        Returns:
            The updated list of all groups.

        Raises:
            ValidationError: if *group_name* is empty or not found.
        """
        if not group_name:
            raise ValidationError("Group name is required")

        groups = self._domain.groups
        target = next((g for g in groups if g.get("name") == group_name), None)
        if target is None:
            raise ValidationError(f'Group "{group_name}" not found')

        to_remove = set(remove or [])
        old = dict(target)
        members = [m for m in target.get("members", []) if m not in to_remove]
        existing = set(members)
        for m in add or []:
            if m and m not in existing:
                members.append(m)
                existing.add(m)
        target["members"] = members

        self._enforce_exclusive_membership(groups, group_name)
        self._sync_class_group_field(groups)
        self._domain.groups = groups
        self._domain.record_change(
            "group_updated",
            entity_type="group",
            entity_ref=group_name,
            summary=group_name,
            meta=self._domain.diff_meta(old, target),
        )
        self._domain.save()
        return self._domain.groups

    @staticmethod
    def _enforce_exclusive_membership(
        groups: List[Dict], authoritative_group_name: str
    ) -> None:
        """Ensure every entity belongs to at most one group.

        After the group identified by *authoritative_group_name* has been
        updated, remove any of its members that appear in other groups.
        """
        target = next(
            (g for g in groups if g.get("name") == authoritative_group_name), None
        )
        if target is None:
            return
        owner_members = set(target.get("members", []))
        for g in groups:
            if g.get("name") == authoritative_group_name:
                continue
            g["members"] = [m for m in g.get("members", []) if m not in owner_members]

    def _sync_class_group_field(self, groups: List[Dict]) -> None:
        """Keep each class's ``group`` field in sync with the groups list."""
        class_to_group: Dict[str, str] = {}
        for g in groups:
            for m in g.get("members", []):
                class_to_group[m] = g.get("name", "")
        for cls in self._domain.get_classes():
            cls["group"] = class_to_group.get(cls.get("name", ""), "")
