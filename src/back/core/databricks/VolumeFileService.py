"""Volume file I/O service for Unity Catalog.

Provides file read / write / list / delete operations on UC Volumes
through the Databricks Files API.  Uses a shared ``DatabricksAuth``
instance so OAuth / PAT logic is not duplicated.
"""

import requests
from urllib.parse import quote
from typing import Dict, List, Optional, Tuple

from back.core.logging import get_logger
from .constants import _REQUEST_TIMEOUT, FS_FILES_PATH, FS_DIRS_PATH
from .DatabricksAuth import DatabricksAuth

logger = get_logger(__name__)


class VolumeFileService:
    """File operations on Unity Catalog Volumes.

    Accepts either explicit ``host`` / ``token`` strings **or** a
    pre-built ``DatabricksAuth`` instance.
    """

    @staticmethod
    def _fs_api_path_suffix(absolute_path: str) -> str:
        """URL-encode path for Files API (matches Databricks SDK ``_escape_multi_segment_path_parameter``)."""
        return quote(absolute_path)

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        *,
        auth: Optional[DatabricksAuth] = None,
    ) -> None:
        if auth is not None:
            self._auth = auth
        else:
            self._auth = DatabricksAuth(host=host, token=token)
        self._session = requests.Session()

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._auth.get_bearer_token()}"}

    def _host(self) -> str:
        return self._auth.host

    def is_configured(self) -> bool:
        """Return *True* when host + credentials are available."""
        return bool(self._host() and self._auth.has_valid_auth())

    @staticmethod
    def get_volume_path(catalog: str, schema: str, volume: str) -> str:
        return f"/Volumes/{catalog}/{schema}/{volume}"

    def list_files(
        self,
        catalog: str,
        schema: str,
        volume: str,
        extensions: Optional[List[str]] = None,
    ) -> Tuple[bool, List[Dict], str]:
        """List files in a UC Volume.

        Returns:
            ``(success, files_list, message)``
        """
        if not self.is_configured():
            return False, [], "Databricks configuration missing"
        try:
            volume_path = self.get_volume_path(catalog, schema, volume)
            host = self._host()
            response = self._session.get(
                f"{host}{FS_DIRS_PATH}{VolumeFileService._fs_api_path_suffix(volume_path)}",
                headers=self._headers(),
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code == 200:
                files = self._parse_directory_contents(
                    response.json(),
                    volume_path,
                    extensions=extensions,
                )
                return True, files, f"Found {len(files)} files"
            if response.status_code == 404:
                return False, [], f"Volume not found: {volume_path}"
            return False, [], f"Error listing files: {response.status_code}"
        except Exception as exc:
            logger.exception("Error listing files: %s", exc)
            return False, [], f"Error: {exc}"

    def list_directory(
        self,
        dir_path: str,
        extensions: Optional[List[str]] = None,
        dirs_only: bool = False,
    ) -> Tuple[bool, List[Dict], str]:
        """List contents at an arbitrary ``/Volumes/…`` path."""
        if not self.is_configured():
            return False, [], "Databricks configuration missing"
        try:
            host = self._host()
            response = self._session.get(
                f"{host}{FS_DIRS_PATH}{VolumeFileService._fs_api_path_suffix(dir_path)}",
                headers=self._headers(),
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code == 200:
                items = self._parse_directory_contents(
                    response.json(),
                    dir_path,
                    extensions=extensions,
                    dirs_only=dirs_only,
                )
                return True, items, f"Found {len(items)} items"
            if response.status_code == 404:
                return False, [], f"Directory not found: {dir_path}"
            return False, [], f"Error listing directory: {response.status_code}"
        except Exception as exc:
            logger.exception("Error listing directory %s: %s", dir_path, exc)
            return False, [], f"Error: {exc}"

    def _read_file_raw(self, file_path: str) -> Tuple[bool, requests.Response, str]:
        """GET a file from the Files API and return ``(success, response, message)``."""
        if not self.is_configured():
            return False, None, "Databricks configuration missing"  # type: ignore[return-value]
        try:
            host = self._host()
            response = self._session.get(
                f"{host}{FS_FILES_PATH}{VolumeFileService._fs_api_path_suffix(file_path)}",
                headers=self._headers(),
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code == 200:
                return True, response, "File read successfully"
            if response.status_code == 404:
                return False, response, f"File not found: {file_path}"
            if response.status_code == 403:
                return False, response, "Access denied. Check your permissions."
            return False, response, f"Error reading file: {response.status_code}"
        except Exception as exc:
            logger.exception("Error reading file %s: %s", file_path, exc)
            return False, None, f"Error: {exc}"  # type: ignore[return-value]

    def read_file(self, file_path: str) -> Tuple[bool, str, str]:
        """Read a text file. Returns ``(success, content, message)``."""
        ok, resp, msg = self._read_file_raw(file_path)
        if ok and resp is not None:
            return True, resp.text, msg
        return False, "", msg

    def read_binary_file(self, file_path: str) -> Tuple[bool, bytes, str]:
        """Read raw bytes. Returns ``(success, data, message)``."""
        ok, resp, msg = self._read_file_raw(file_path)
        if ok and resp is not None:
            return True, resp.content, msg
        return False, b"", msg

    def create_directory(self, directory_path: str) -> Tuple[bool, str]:
        """Create a directory and parents if missing (idempotent). Matches Files API PUT directories."""
        if not self.is_configured():
            return False, "Databricks configuration missing"
        try:
            host = self._host()
            response = self._session.put(
                f"{host}{FS_DIRS_PATH}{VolumeFileService._fs_api_path_suffix(directory_path)}",
                headers=self._headers(),
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code in (200, 201, 204):
                return True, f"Directory ensured: {directory_path}"
            return (
                False,
                f"Failed to create directory: {response.status_code} - {response.text}",
            )
        except Exception as exc:
            logger.exception("Error creating directory %s: %s", directory_path, exc)
            return False, f"Error: {exc}"

    def write_file(
        self, file_path: str, content: str, overwrite: bool = True
    ) -> Tuple[bool, str]:
        """Write text to a file. Returns ``(success, message)``."""
        if not self.is_configured():
            return False, "Databricks configuration missing"
        try:
            host = self._host()
            headers = self._headers()
            headers["Content-Type"] = "application/octet-stream"
            response = self._session.put(
                f"{host}{FS_FILES_PATH}{VolumeFileService._fs_api_path_suffix(file_path)}",
                headers=headers,
                data=content.encode("utf-8"),
                params={"overwrite": "true" if overwrite else "false"},
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code in (200, 201, 204):
                return True, f"File saved successfully: {file_path}"
            return (
                False,
                f"Failed to write file: {response.status_code} - {response.text}",
            )
        except Exception as exc:
            logger.exception("Error writing file: %s", exc)
            return False, f"Error: {exc}"

    def write_binary_file(
        self, file_path: str, data: bytes, overwrite: bool = True
    ) -> Tuple[bool, str]:
        """Write raw bytes to a file. Returns ``(success, message)``."""
        if not self.is_configured():
            return False, "Databricks configuration missing"
        try:
            host = self._host()
            headers = self._headers()
            headers["Content-Type"] = "application/octet-stream"
            response = self._session.put(
                f"{host}{FS_FILES_PATH}{VolumeFileService._fs_api_path_suffix(file_path)}",
                headers=headers,
                data=data,
                params={"overwrite": "true" if overwrite else "false"},
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code in (200, 201, 204):
                return True, f"File saved successfully: {file_path}"
            return (
                False,
                f"Failed to write file: {response.status_code} - {response.text}",
            )
        except Exception as exc:
            logger.exception("Error writing binary file: %s", exc)
            return False, f"Error: {exc}"

    def delete_file(self, file_path: str) -> Tuple[bool, str]:
        """Delete a file. Returns ``(success, message)``."""
        if not self.is_configured():
            return False, "Databricks configuration missing"
        try:
            host = self._host()
            response = self._session.delete(
                f"{host}{FS_FILES_PATH}{VolumeFileService._fs_api_path_suffix(file_path)}",
                headers=self._headers(),
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code in (200, 204):
                return True, "File deleted successfully"
            if response.status_code == 404:
                return False, f"File not found: {file_path}"
            return False, f"Failed to delete file ({response.status_code}): {file_path}"
        except Exception as exc:
            logger.exception("Error deleting file %s: %s", file_path, exc)
            return False, f"Error: {exc}"

    def delete_directory(self, dir_path: str) -> Tuple[bool, str]:
        """Delete an empty directory. Returns ``(success, message)``."""
        if not self.is_configured():
            return False, "Databricks configuration missing"
        try:
            host = self._host()
            response = self._session.delete(
                f"{host}{FS_DIRS_PATH}{VolumeFileService._fs_api_path_suffix(dir_path)}",
                headers=self._headers(),
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code in (200, 204):
                return True, "Directory deleted successfully"
            if response.status_code == 404:
                return True, "Directory already gone"
            return (
                False,
                f"Failed to delete directory ({response.status_code}): {dir_path}",
            )
        except Exception as exc:
            logger.exception("Error deleting directory %s: %s", dir_path, exc)
            return False, f"Error: {exc}"

    @staticmethod
    def _parse_directory_contents(
        data: dict,
        base_path: str,
        extensions: Optional[List[str]] = None,
        dirs_only: bool = False,
    ) -> List[Dict]:
        items: list = []
        for item in data.get("contents", []):
            name = item.get("name", "")
            is_dir = item.get("is_directory", name.endswith("/"))
            if dirs_only and not is_dir:
                continue
            if extensions and not is_dir:
                if not any(name.lower().endswith(ext.lower()) for ext in extensions):
                    continue
            items.append(
                {
                    "name": name.rstrip("/"),
                    "path": f"{base_path}/{name}".replace("//", "/"),
                    "is_directory": is_dir,
                    "size": item.get("file_size", 0),
                }
            )
        return items
