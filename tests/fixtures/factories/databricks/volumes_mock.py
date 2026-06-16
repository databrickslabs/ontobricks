"""Mock UC Volume — in-memory file storage with same surface as VolumeFileService."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MockVolume:
    """In-memory file ops matching `back.core.databricks.VolumeFileService`.

    Stores files under a `/Volumes/<catalog>/<schema>/<volume>/<path>` key.
    Treats paths as opaque strings.
    """

    files: dict[str, bytes] = field(default_factory=dict)

    def write(self, path: str, content: bytes | str) -> None:
        if isinstance(content, str):
            content = content.encode("utf-8")
        self.files[path] = content

    def read(self, path: str) -> bytes:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def read_text(self, path: str) -> str:
        return self.read(path).decode("utf-8")

    def exists(self, path: str) -> bool:
        return path in self.files

    def delete(self, path: str) -> None:
        self.files.pop(path, None)

    def list(self, prefix: str = "") -> list[str]:
        return sorted(p for p in self.files if p.startswith(prefix))

    def list_files(self, prefix: str = "") -> list[dict[str, Any]]:
        return [
            {"path": p, "size": len(self.files[p])}
            for p in self.list(prefix)
        ]
