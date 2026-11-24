"""Mapping domain: entity/relationship mappings, R2RML, SQL preview, session merge."""

from back.objects.mapping.AutoAssignLiveFeed import AutoAssignLiveFeed
from back.objects.mapping.json_views import get_mapping_info
from back.objects.mapping.Mapping import Mapping

__all__ = ["AutoAssignLiveFeed", "Mapping", "get_mapping_info"]
