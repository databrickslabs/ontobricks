"""Tests for back.core.databricks.VolumeFileService."""

import pytest
from unittest.mock import MagicMock

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.VolumeFileService import VolumeFileService


@pytest.fixture
def clean_databricks_env(monkeypatch):
    for key in (
        "DATABRICKS_APP_PORT",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(key, raising=False)


class TestVolumeFileServiceInit:
    def test_host_token(self, clean_databricks_env):
        svc = VolumeFileService(host="https://h.com", token="tok")
        assert svc._auth.host == "https://h.com"
        assert svc._auth.token == "tok"

    def test_auth_kwarg(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        assert svc._auth is auth


class TestIsConfigured:
    def test_true_with_valid_auth(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        assert svc.is_configured() is True

    def test_false_without_auth(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="")
        svc = VolumeFileService(auth=auth)
        assert svc.is_configured() is False


class TestGetVolumePath:
    def test_static_path(self):
        assert VolumeFileService.get_volume_path("c", "s", "v") == "/Volumes/c/s/v"


class TestListFiles:
    def test_not_configured(self, clean_databricks_env):
        svc = VolumeFileService(auth=DatabricksAuth(host="https://h.com", token=""))
        ok, files, msg = svc.list_files("c", "s", "v")
        assert ok is False
        assert files == []
        assert "configuration missing" in msg

    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [
                    {
                        "name": "a.txt",
                        "is_directory": False,
                        "file_size": 5,
                    }
                ]
            },
        )
        ok, files, msg = svc.list_files("c", "s", "v")
        assert ok is True
        assert len(files) == 1
        assert files[0]["name"] == "a.txt"
        assert "Found" in msg

    def test_404(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=404)
        ok, files, msg = svc.list_files("c", "s", "missing")
        assert ok is False
        assert files == []
        assert "not found" in msg.lower()


class TestListDirectory:
    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [
                    {"name": "d/", "is_directory": True},
                    {"name": "f.txt", "is_directory": False},
                ]
            },
        )
        ok, items, msg = svc.list_directory("/Volumes/c/s/v")
        assert ok is True
        assert len(items) == 2

    def test_dirs_only(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "contents": [
                    {"name": "d/", "is_directory": True},
                    {"name": "f.txt", "is_directory": False},
                ]
            },
        )
        ok, items, _ = svc.list_directory("/Volumes/c/s/v", dirs_only=True)
        assert ok is True
        assert len(items) == 1
        assert items[0]["is_directory"] is True


class TestReadFile:
    def test_not_configured(self, clean_databricks_env):
        svc = VolumeFileService(auth=DatabricksAuth(host="https://h.com", token=""))
        ok, content, msg = svc.read_file("/Volumes/c/s/v/f.txt")
        assert ok is False
        assert content == ""
        assert "configuration missing" in msg

    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=200, text="hello")
        ok, content, msg = svc.read_file("/Volumes/c/s/v/f.txt")
        assert ok is True
        assert content == "hello"
        assert "success" in msg.lower()

    def test_404(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=404)
        ok, content, msg = svc.read_file("/missing")
        assert ok is False
        assert "not found" in msg.lower()

    def test_403(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=403)
        ok, content, msg = svc.read_file("/secret")
        assert ok is False
        assert "denied" in msg.lower()


class TestReadBinaryFile:
    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=200, content=b"\x00\xff")
        ok, data, msg = svc.read_binary_file("/Volumes/c/s/v/bin")
        assert ok is True
        assert data == b"\x00\xff"

    def test_404(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.get.return_value = MagicMock(status_code=404)
        ok, data, msg = svc.read_binary_file("/nope")
        assert ok is False
        assert data == b""


class TestWriteFile:
    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.put.return_value = MagicMock(status_code=200, text="")
        ok, msg = svc.write_file("/Volumes/c/s/v/out.txt", "text")
        assert ok is True
        assert "saved" in msg.lower()

    def test_failure(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.put.return_value = MagicMock(status_code=400, text="bad")
        ok, msg = svc.write_file("/Volumes/c/s/v/out.txt", "text")
        assert ok is False
        assert "400" in msg


class TestWriteBinaryFile:
    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.put.return_value = MagicMock(status_code=204, text="")
        ok, msg = svc.write_binary_file("/Volumes/c/s/v/b.bin", b"\x01")
        assert ok is True
        assert "saved" in msg.lower()
        put_url = svc._session.put.call_args[0][0]
        assert "/api/2.0/fs/files" in put_url
        assert "Volumes" in put_url


class TestCreateDirectory:
    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.put.return_value = MagicMock(status_code=200)
        ok, msg = svc.create_directory("/Volumes/c/s/v/domains/p/documents")
        assert ok is True
        assert "documents" in msg

    def test_failure(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.put.return_value = MagicMock(status_code=403, text="no")
        ok, msg = svc.create_directory("/Volumes/c/s/v/doc")
        assert ok is False
        assert "403" in msg


class TestDeleteFile:
    def test_success(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.delete.return_value = MagicMock(status_code=204)
        ok, msg = svc.delete_file("/Volumes/c/s/v/f.txt")
        assert ok is True
        assert "deleted" in msg.lower()

    def test_404(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = VolumeFileService(auth=auth)
        svc._session = MagicMock()
        svc._session.delete.return_value = MagicMock(status_code=404)
        ok, msg = svc.delete_file("/missing")
        assert ok is False
        assert "not found" in msg.lower()


class TestParseDirectoryContents:
    def test_extension_filter(self, clean_databricks_env):
        data = {
            "contents": [
                {"name": "a.txt", "is_directory": False},
                {"name": "b.pdf", "is_directory": False},
            ]
        }
        items = VolumeFileService._parse_directory_contents(
            data, "/Volumes/c/s/v", extensions=[".txt"]
        )
        assert len(items) == 1
        assert items[0]["name"] == "a.txt"

    def test_dirs_only(self, clean_databricks_env):
        data = {
            "contents": [
                {"name": "sub/", "is_directory": True},
                {"name": "f.txt", "is_directory": False},
            ]
        }
        items = VolumeFileService._parse_directory_contents(
            data, "/Volumes/c/s/v", dirs_only=True
        )
        assert len(items) == 1
        assert items[0]["name"] == "sub"
