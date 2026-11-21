"""Read-only questions Evolve asks about mapped source tables."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class SourceProbe(ABC):
    """Bounded, read-only inspection of Unity Catalog tables.

    Tables are fully qualified ``catalog.schema.table`` names already
    validated by the caller. Implementations raise
    :class:`SourceAccessDenied` or :class:`SourceUnavailable` rather than
    returning partial answers.
    """

    @abstractmethod
    def columns(self, table: str) -> Dict[str, str]:
        """Column name → data type."""

    @abstractmethod
    def foreign_keys(self, table: str) -> List[Dict[str, str]]:
        """Declared FKs of *table*: ``{column, ref_table, ref_column}``."""

    @abstractmethod
    def count_value(self, table: str, column: str, value: str) -> int:
        """Rows of *table* where *column* equals *value*."""

    @abstractmethod
    def key_is_unique(self, table: str, column: str) -> bool:
        """True when *column* has no duplicate values in *table*."""

    @abstractmethod
    def join_profile(
        self, source: str, source_column: str, target: str, target_column: str
    ) -> Dict[str, object]:
        """``{source_rows, matched, target_column_unique}`` for a candidate join."""

    @abstractmethod
    def mapping_profile(
        self,
        sql: str,
        source_column: str,
        target_column: str,
        source_entity: Dict[str, str],
        target_entity: Dict[str, str],
    ) -> Dict[str, int]:
        """``{rows, dangling_source, dangling_target}`` for an existing mapping SQL.

        ``*_entity`` are ``{table, id_column}`` of the mapped endpoint classes.
        """

    @abstractmethod
    def table_version(self, table: str) -> Optional[int]:
        """Current Delta version of *table*; ``None`` for a view (nothing to pin)."""

    @abstractmethod
    def gold_ids(self, table: str, question_id: str) -> List[str]:
        """Expected ids for *question_id* from a ``(question_id, expected_ids)`` table.

        Raises :class:`~back.core.errors.NotFoundError` when the question is
        not in the table.
        """
