"""Tests for the LadybugDB sync-to/from-Volume module."""
import io
import os
import shutil
import tarfile
import pytest
from unittest.mock import MagicMock

from back.core.triplestore.ladybugdb import (
    local_db_path,
    _sanitize_db_name,
    graph_volume_path,
    sync_from_volume,
    sync_to_volume,
)

TEMP_BASE = "/tmp/ladybug_sync_test"


@pytest.fixture(autouse=True)
def clean_temp():
    """Create and clean the temp directory for each test."""
    os.makedirs(TEMP_BASE, exist_ok=True)
    yield
    shutil.rmtree(TEMP_BASE, ignore_errors=True)


class TestSanitizeDbName:
    def test_clean_name(self):
        assert _sanitize_db_name("mydb") == "mydb"

    def test_special_chars(self):
        assert _sanitize_db_name("my-db.v2") == "my_db_v2"


class TestLocalDbPath:
    def test_default_base(self):
        path = local_db_path("test")
        assert path == "/tmp/ontobricks/test.lbug"

    def test_custom_base(self):
        path = local_db_path("test", local_base="/data")
        assert path == "/data/test.lbug"


class TestGraphVolumePath:
    def test_path(self):
        path = graph_volume_path("/Volumes/cat/sch/vol/domains/proj1", "mydb")
        assert path == "/Volumes/cat/sch/vol/domains/proj1/ontobricks_mydb.lbug.tar.gz"


class TestSyncToVolume:
    def test_success(self):
        db_dir = os.path.join(TEMP_BASE, "test.lbug")
        os.makedirs(db_dir)
        with open(os.path.join(db_dir, "data.bin"), "wb") as f:
            f.write(b"fake database content")

        uc_mock = MagicMock()
        uc_mock.write_binary_file.return_value = (True, "OK")

        ok, msg = sync_to_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert ok
        uc_mock.write_binary_file.assert_called_once()
        call_args = uc_mock.write_binary_file.call_args
        assert call_args[0][0].endswith(".lbug.tar.gz")
        archive_bytes = call_args[0][1]
        assert len(archive_bytes) > 0

    def test_local_missing(self):
        uc_mock = MagicMock()
        ok, msg = sync_to_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "nonexistent",
            local_base=TEMP_BASE,
        )
        assert not ok
        assert "not found" in msg.lower()
        uc_mock.write_binary_file.assert_not_called()

    def test_upload_failure(self):
        db_dir = os.path.join(TEMP_BASE, "test.lbug")
        os.makedirs(db_dir)
        with open(os.path.join(db_dir, "data.bin"), "wb") as f:
            f.write(b"content")

        uc_mock = MagicMock()
        uc_mock.write_binary_file.return_value = (False, "Permission denied")

        ok, msg = sync_to_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert not ok
        assert "Permission denied" in msg


class TestSyncFromVolume:
    @staticmethod
    def _make_archive(db_name: str = "test", legacy_prefix: bool = False) -> bytes:
        """Build a valid tar.gz archive containing a fake .lbug directory.

        Args:
            legacy_prefix: If True, use the old ``ontobricks_`` naming inside the
                archive (simulates archives created before the naming change).
        """
        safe = _sanitize_db_name(db_name)
        dir_name = f"ontobricks_{safe}.lbug" if legacy_prefix else f"{safe}.lbug"
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            data = b"fake graph data"
            info = tarfile.TarInfo(name=f"{dir_name}/data.bin")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        return buf.getvalue()

    def test_success(self):
        archive = self._make_archive()
        uc_mock = MagicMock()
        uc_mock.read_binary_file.return_value = (True, archive, "OK")

        ok, msg = sync_from_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert ok
        restored = os.path.join(TEMP_BASE, "test.lbug")
        assert os.path.exists(restored)

    def test_legacy_archive_renamed(self):
        """Archives with the old ``ontobricks_`` prefix are renamed on extraction."""
        archive = self._make_archive(legacy_prefix=True)
        uc_mock = MagicMock()
        uc_mock.read_binary_file.return_value = (True, archive, "OK")

        ok, msg = sync_from_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert ok
        expected = os.path.join(TEMP_BASE, "test.lbug")
        legacy = os.path.join(TEMP_BASE, "ontobricks_test.lbug")
        assert os.path.exists(expected)
        assert not os.path.exists(legacy)

    def test_archive_not_found(self):
        uc_mock = MagicMock()
        uc_mock.read_binary_file.return_value = (False, None, "File not found")

        ok, msg = sync_from_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert ok  # not-found is not an error (new domain)
        assert "new domain" in msg.lower()

    def test_download_error(self):
        uc_mock = MagicMock()
        uc_mock.read_binary_file.return_value = (False, None, "Connection timeout")

        ok, msg = sync_from_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert not ok
        assert "Connection timeout" in msg

    def test_replaces_existing_local(self):
        existing = os.path.join(TEMP_BASE, "test.lbug")
        os.makedirs(existing)
        with open(os.path.join(existing, "old.bin"), "wb") as f:
            f.write(b"old data")

        archive = self._make_archive()
        uc_mock = MagicMock()
        uc_mock.read_binary_file.return_value = (True, archive, "OK")

        ok, msg = sync_from_volume(
            uc_mock,
            "/Volumes/cat/sch/vol/domains/proj",
            "test",
            local_base=TEMP_BASE,
        )
        assert ok
        assert not os.path.exists(os.path.join(existing, "old.bin"))
        assert os.path.exists(os.path.join(existing, "data.bin"))
