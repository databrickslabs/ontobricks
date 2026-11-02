"""One change to a slot of the Real-Time-Mode ``_live`` table: an upsert or a tombstone."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SlotChange:
    """An upsert or tombstone of one ``(map_id, source_key, predicate)`` slot, sequenced by ``seq``.

    ``subject`` / ``object`` / ``object_hash`` are ``None`` only on a tombstone
    (``deleted``). ``object_hash`` is the SHA-256 hex of the UTF-8 object, the
    value Spark's ``sha2(object, 256)`` (``SQLHelpers.object_hash_sql``) gives.
    ``seq`` increases monotonically per source key (Kafka offset or commit
    version): the sink keeps the change with the highest one.
    """

    map_id: str
    source_key: str
    predicate: str
    subject: str | None
    object: str | None
    object_hash: str | None
    deleted: bool
    seq: int

    @classmethod
    def upsert(
        cls,
        *,
        map_id: str,
        source_key: str,
        predicate: str,
        subject: str | None,
        object: str,
        seq: int,
    ) -> SlotChange:
        """The slot now holds *object* for *subject*."""
        return cls(
            map_id=map_id,
            source_key=source_key,
            predicate=predicate,
            subject=subject,
            object=object,
            object_hash=cls.hash_object(object),
            deleted=False,
            seq=seq,
        )

    @classmethod
    def tombstone(cls, *, map_id: str, source_key: str, predicate: str, seq: int) -> SlotChange:
        """The slot no longer exists."""
        return cls(
            map_id=map_id,
            source_key=source_key,
            predicate=predicate,
            subject=None,
            object=None,
            object_hash=None,
            deleted=True,
            seq=seq,
        )

    @staticmethod
    def hash_object(obj: str) -> str:
        """``sha2(obj, 256)``: the lower-case SHA-256 hex digest of the UTF-8 bytes."""
        return hashlib.sha256(obj.encode("utf-8")).hexdigest()

    def as_row(self) -> dict:
        """The change as an output row, fields in declaration (output schema) order."""
        return asdict(self)
