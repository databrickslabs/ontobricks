"""Sync a LadybugDB on-disk graph to / from a Unity Catalog Volume.

The graph database (``/tmp/ontobricks/<name>.lbug`` — may be a single file
or a directory depending on the LadybugDB version) is archived into a
``tar.gz`` and transferred via the UC Files API.  The archive is stored
alongside the domain version files in the registry:

    /Volumes/{cat}/{sch}/{vol}/domains/{folder}/ontobricks_{name}.lbug.tar.gz

Legacy registries may still use ``projects/`` instead of ``domains/``;
the path is resolved by :class:`RegistryService` at I/O time.
"""

import io
import os
import shutil
import tarfile
from typing import Tuple

from back.core.logging import get_logger

logger = get_logger(__name__)


class GraphSyncService:
    """Sync LadybugDB graphs between local disk and a UC Volume.

    Bundles the ``uc_service``, ``db_name``, and ``local_base`` parameters
    that the previous free functions required on every call.
    """

    def __init__(
        self,
        uc_service,
        db_name: str,
        local_base: str = "/tmp/ontobricks",
    ) -> None:
        self._uc = uc_service
        self._db_name = db_name
        self._local_base = local_base

    @staticmethod
    def sanitize_db_name(db_name: str) -> str:
        """Sanitize *db_name* for paths (uses ``safe_identifier``)."""
        from back.core.helpers import safe_identifier

        return safe_identifier(db_name)

    @property
    def local_db_path(self) -> str:
        """Absolute path to the ``.lbug`` file/directory on local disk."""
        safe = GraphSyncService.sanitize_db_name(self._db_name)
        return os.path.join(self._local_base, f"{safe}.lbug")

    def volume_path(self, uc_domain_path: str) -> str:
        """Build the archive path inside the registry Volume."""
        safe = GraphSyncService.sanitize_db_name(self._db_name)
        return f"{uc_domain_path}/ontobricks_{safe}.lbug.tar.gz"

    def sync_to_volume(self, uc_domain_path: str) -> Tuple[bool, str]:
        """Archive the local ``.lbug`` directory and upload it to the registry."""
        local_path = self.local_db_path
        if not os.path.exists(local_path):
            msg = f"Local graph not found: {local_path}"
            logger.warning(msg)
            return False, msg

        vol_path = self.volume_path(uc_domain_path)

        try:
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                tar.add(local_path, arcname=os.path.basename(local_path))
            archive_bytes = buf.getvalue()
            logger.info(
                "LadybugDB graph archived: %s (%.1f KB)",
                local_path,
                len(archive_bytes) / 1024,
            )
        except Exception as exc:
            msg = f"Failed to archive graph directory: {exc}"
            logger.exception(msg)
            return False, msg

        ok, msg = self._uc.write_binary_file(vol_path, archive_bytes)
        if ok:
            logger.info("LadybugDB graph synced to Volume: %s", vol_path)
        else:
            logger.error("Failed to upload graph archive: %s", msg)
        return ok, msg

    def sync_from_volume(self, uc_domain_path: str) -> Tuple[bool, str]:
        """Download the graph archive from the registry and extract locally."""
        vol_path = self.volume_path(uc_domain_path)

        ok, data, msg = self._uc.read_binary_file(vol_path)
        if not ok:
            if "not found" in msg.lower():
                logger.info(
                    "No graph archive in registry yet (%s) — nothing to restore",
                    vol_path,
                )
                return True, "No graph archive in registry (new domain)"
            logger.error("Failed to download graph archive: %s", msg)
            return False, msg

        local_path = self.local_db_path
        try:
            os.makedirs(self._local_base, exist_ok=True)
            if os.path.exists(local_path):
                if os.path.isdir(local_path):
                    shutil.rmtree(local_path)
                else:
                    os.remove(local_path)
                logger.debug("Removed existing local graph: %s", local_path)

            buf = io.BytesIO(data)
            with tarfile.open(fileobj=buf, mode="r:gz") as tar:
                tar.extractall(path=self._local_base)

            if not os.path.exists(local_path):
                legacy_path = os.path.join(
                    self._local_base,
                    f"ontobricks_{GraphSyncService.sanitize_db_name(self._db_name)}.lbug",
                )
                if os.path.exists(legacy_path):
                    os.rename(legacy_path, local_path)
                    logger.info(
                        "Renamed legacy graph %s -> %s", legacy_path, local_path
                    )

            logger.info(
                "LadybugDB graph restored from Volume: %s -> %s",
                vol_path,
                local_path,
            )
            return True, f"Graph restored to {local_path}"
        except Exception as exc:
            msg = f"Failed to extract graph archive: {exc}"
            logger.exception(msg)
            return False, msg

    @staticmethod
    def local_path_for_db(db_name: str, local_base: str = "/tmp/ontobricks") -> str:
        """Return the absolute path to the ``.lbug`` file or directory."""
        safe = GraphSyncService.sanitize_db_name(db_name)
        return os.path.join(local_base, f"{safe}.lbug")

    @staticmethod
    def volume_archive_path(uc_domain_path: str, db_name: str) -> str:
        """Build the archive path inside the registry Volume."""
        safe = GraphSyncService.sanitize_db_name(db_name)
        return f"{uc_domain_path}/ontobricks_{safe}.lbug.tar.gz"

    @staticmethod
    def upload_to_volume(
        uc_service,
        uc_domain_path: str,
        db_name: str,
        local_base: str = "/tmp/ontobricks",
    ) -> Tuple[bool, str]:
        return GraphSyncService(uc_service, db_name, local_base).sync_to_volume(
            uc_domain_path,
        )

    @staticmethod
    def download_from_volume(
        uc_service,
        uc_domain_path: str,
        db_name: str,
        local_base: str = "/tmp/ontobricks",
    ) -> Tuple[bool, str]:
        return GraphSyncService(uc_service, db_name, local_base).sync_from_volume(
            uc_domain_path,
        )
