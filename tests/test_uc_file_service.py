"""Tests for back.core.databricks.uc_file_service — UC volume file operations."""

import pytest
from unittest.mock import MagicMock

from back.core.databricks.VolumeFileService import VolumeFileService as UCFileService


class TestUCFileServiceInit:
    def test_basic_init(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://host.com", token="tok")
        assert svc._auth.host == "https://host.com"
        assert svc._auth.token == "tok"
        assert svc._auth.is_app_mode is False

    def test_volume_path(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService()
        path = svc.get_volume_path("cat", "sch", "vol")
        assert path == "/Volumes/cat/sch/vol"


class TestIsConfigured:
    def test_configured_with_token(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com")
        assert svc.is_configured() is True

    def test_not_configured(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        svc = UCFileService()
        assert svc.is_configured() is False


class TestReadFile:
    def test_not_configured(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        svc = UCFileService()
        ok, content, msg = svc.read_file("/Volumes/c/s/v/file.txt")
        assert ok is False
        assert "configuration missing" in msg

    def test_read_success(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=200, text="file content")
        ok, content, msg = svc.read_file("/Volumes/c/s/v/file.txt")
        assert ok is True
        assert content == "file content"

    def test_read_not_found(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=404)
        ok, content, msg = svc.read_file("/Volumes/c/s/v/missing.txt")
        assert ok is False
        assert "not found" in msg.lower()


class TestWriteFile:
    def test_not_configured(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        svc = UCFileService()
        ok, msg = svc.write_file("/path", "content")
        assert ok is False

    def test_write_success(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.put.return_value = MagicMock(status_code=200)
        ok, msg = svc.write_file("/path", "content")
        assert ok is True


class TestDeleteFile:
    def test_delete_success(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.delete.return_value = MagicMock(status_code=204)
        ok, msg = svc.delete_file("/path")
        assert ok is True

    def test_delete_not_found(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.delete.return_value = MagicMock(status_code=404)
        ok, msg = svc.delete_file("/path")
        assert ok is False


class TestListFiles:
    def test_list_success(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [
                    {"name": "file.json", "file_size": 100},
                    {"name": "subdir/", "is_directory": True},
                ]
            },
        )
        ok, files, msg = svc.list_files("cat", "sch", "vol")
        assert ok is True
        assert len(files) == 2

    def test_list_with_extension_filter(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [
                    {"name": "data.json", "file_size": 100},
                    {"name": "readme.txt", "file_size": 50},
                ]
            },
        )
        ok, files, msg = svc.list_files("cat", "sch", "vol", extensions=[".json"])
        assert ok is True
        assert len(files) == 1
        assert files[0]["name"] == "data.json"


class TestListDirectory:
    def test_list_directory(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        svc = UCFileService(host="https://h.com", token="tok")
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [
                    {"name": "proj1/", "is_directory": True},
                    {"name": "proj2/", "is_directory": True},
                    {"name": "readme.md", "file_size": 50},
                ]
            },
        )
        ok, items, msg = svc.list_directory("/Volumes/c/s/v/domains", dirs_only=True)
        assert ok is True
        assert len(items) == 2
