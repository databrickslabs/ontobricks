"""Push registry domains to the instance GitHub repository and read them back."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ConflictError, ValidationError
from back.core.github import GITHUB_TOKEN_ENV, GitHubClient
from back.core.logging import get_logger
from back.objects.registry.gitsync.DomainRepoLayout import DomainRepoLayout
from back.objects.registry.gitsync.GitHubConfig import GitHubConfig
from back.objects.registry.version_lifecycle import STATUS_PUBLISHED
from shared.config.constants import APP_VERSION

logger = get_logger(__name__)

INSTANCE_FILE = ".ontobricks/instance.json"
GLOBAL_CONFIG_FILE = ".ontobricks/global_config.json"

# Non-secret instance defaults worth versioning next to the domains.
_GLOBAL_CONFIG_KEYS = ("default_base_uri", "default_emoji", "graph_engine_config")


class GitHubSyncService:
    """Mirror operations for one :class:`GitHubConfig`."""

    def __init__(
        self,
        cfg: GitHubConfig,
        client_factory: Optional[Callable[[GitHubConfig], GitHubClient]] = None,
    ) -> None:
        self.cfg = cfg
        self._client_factory = client_factory or self._default_client
        self._client: Optional[GitHubClient] = None

    # ------------------------------------------------------------------
    # Client / identity
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_token(cfg: GitHubConfig) -> str:
        """Resolve the GitHub token from the configured Databricks secret.

        Falls back to the ``ONTOBRICKS_GITHUB_TOKEN`` env var for local
        development when no secret scope/key is configured.
        """
        if cfg.token_secret_scope and cfg.token_secret_key:
            from back.core.databricks.DatabricksAuth import DatabricksAuth
            from back.core.databricks.SecretsService import SecretsService

            return SecretsService(DatabricksAuth()).get_secret_value(
                cfg.token_secret_scope, cfg.token_secret_key
            )
        token = os.environ.get(GITHUB_TOKEN_ENV, "").strip()
        if not token:
            raise ValidationError(
                "GitHub token not configured — pick a secret scope and key in "
                "Settings → GitHub"
            )
        return token

    @classmethod
    def _default_client(cls, cfg: GitHubConfig) -> GitHubClient:
        return GitHubClient(cls.resolve_token(cfg), cfg.owner, cfg.repo, cfg.branch)

    @property
    def client(self) -> GitHubClient:
        if self._client is None:
            self._client = self._client_factory(self.cfg)
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def instance_identity(self) -> Dict[str, str]:
        return {
            "instance_uuid": self.cfg.instance_uuid,
            "workspace_host": os.environ.get("DATABRICKS_HOST", ""),
            "app_name": os.environ.get("DATABRICKS_APP_NAME", ""),
        }

    @property
    def _author(self) -> Dict[str, str]:
        return {"name": self.cfg.author_name, "email": self.cfg.author_email}

    @staticmethod
    def _message(action: str, target: str, actor: str = "") -> str:
        return f"{action}({target})" + (f": by {actor}" if actor else "")

    # ------------------------------------------------------------------
    # Instance claim (one repository per instance)
    # ------------------------------------------------------------------

    def read_claim(self) -> Optional[Dict[str, Any]]:
        raw = self.client.read_file(self.cfg.path(INSTANCE_FILE))
        if not raw:
            return None
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def ensure_claim(self, *, take_over: bool = False) -> Dict[str, Any]:
        """Make sure the repository belongs to this instance, claiming it if free.

        Raises :class:`ConflictError` when another instance already claimed it
        and *take_over* is false.
        """
        if not self.cfg.instance_uuid:
            raise ValidationError("GitHub settings have no instance id — save them first")
        claim = self.read_claim()
        owner_uuid = (claim or {}).get("instance_uuid", "")
        if owner_uuid == self.cfg.instance_uuid:
            return claim or {}
        if owner_uuid and not take_over:
            raise ConflictError(
                f"Repository {self.cfg.owner}/{self.cfg.repo} is already used by "
                f"another OntoBricks instance ({claim.get('app_name') or owner_uuid} "
                f"on {claim.get('workspace_host') or 'unknown host'}). "
                "Pick another repository or tick 'Take over'."
            )
        new_claim = {
            **self.instance_identity(),
            "ontobricks_version": APP_VERSION,
            "layout_format_version": DomainRepoLayout.FORMAT_VERSION,
            "claimed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self.client.commit_files(
            {self.cfg.path(INSTANCE_FILE): DomainRepoLayout.dump(new_claim)},
            self._message("claim", self.cfg.instance_uuid),
            author=self._author,
        )
        return new_claim

    def test_connection(self, *, take_over: bool = False) -> Dict[str, Any]:
        repo = self.client.get_repo()
        perms = repo.get("permissions") or {}
        if perms and not (perms.get("push") or perms.get("admin")):
            raise ValidationError(
                f"The token cannot push to {self.cfg.owner}/{self.cfg.repo} "
                "(Contents: read and write is required)"
            )
        claim = self.ensure_claim(take_over=take_over)
        return {
            "repository": repo.get("full_name") or f"{self.cfg.owner}/{self.cfg.repo}",
            "private": bool(repo.get("private")),
            "default_branch": repo.get("default_branch", ""),
            "branch": self.cfg.branch,
            "claim": claim,
        }

    # ------------------------------------------------------------------
    # Push
    # ------------------------------------------------------------------

    def _version_files(
        self, doc: Dict[str, Any], folder: str, version: str
    ) -> tuple:
        rel_files, errors = DomainRepoLayout.split_version(doc, folder, version)
        vdir = DomainRepoLayout.version_dir(folder, version)
        return (
            {self.cfg.path(f"{vdir}/{rel}"): text for rel, text in rel_files.items()},
            errors,
        )

    def _domain_files(
        self, doc: Dict[str, Any], folder: str, version: str, versions: List[str]
    ) -> Dict[str, str]:
        return {
            self.cfg.path(rel): text
            for rel, text in DomainRepoLayout.domain_files(
                doc, folder, version, versions
            ).items()
        }

    @staticmethod
    def _versions(store, folder: str) -> List[str]:
        """Ascending version list for *folder* (empty on failure)."""
        ok, versions, _ = store.list_versions(folder)
        if not ok:
            return []
        return sorted(versions, key=lambda v: [int(x) for x in v.split(".")])

    def push_version(
        self, store, folder: str, version: str, *, actor: str = ""
    ) -> Dict[str, Any]:
        """Mirror one version folder (plus the domain summary) in one commit."""
        ok, doc, msg = store.read_version(folder, version)
        if not ok:
            raise ValidationError(f"Cannot read {folder}/V{version}: {msg}")
        files, errors = self._version_files(doc, folder, version)
        versions = self._versions(store, folder)
        files.update(self._domain_files(doc, folder, version, versions))
        sha = self.client.commit_files(
            files,
            self._message("save", f"{folder}/V{version}", actor),
            replace_prefixes=[
                self.cfg.path(DomainRepoLayout.version_dir(folder, version))
            ],
            author=self._author,
        )
        status = (doc.get("info") or {}).get("status") or ""
        if status == STATUS_PUBLISHED:
            target = sha or self.client.head_sha()
            if target:
                self.client.tag(f"{folder}/V{version}", target)
        return {"commit_sha": sha, "errors": errors, "status": status}

    def delete_version(
        self, store, folder: str, version: str, *, actor: str = ""
    ) -> Dict[str, Any]:
        files: Dict[str, str] = {}
        versions = self._versions(store, folder)
        if versions:
            ok, doc, _ = store.read_version(folder, versions[-1])
            if ok:
                files.update(self._domain_files(doc, folder, versions[-1], versions))
        sha = self.client.commit_files(
            files,
            self._message("delete", f"{folder}/V{version}", actor),
            replace_prefixes=[
                self.cfg.path(DomainRepoLayout.version_dir(folder, version))
            ],
            author=self._author,
        )
        return {"commit_sha": sha, "errors": []}

    def delete_domain(self, folder: str, *, actor: str = "") -> Dict[str, Any]:
        sha = self.client.commit_files(
            {},
            self._message("delete", folder, actor),
            replace_prefixes=[self.cfg.path(DomainRepoLayout.domain_dir(folder))],
            author=self._author,
        )
        return {"commit_sha": sha, "errors": []}

    def sync_all(
        self,
        store,
        *,
        actor: str = "",
        global_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Rewrite the whole ``domains/`` tree from the registry in one commit."""
        self.ensure_claim()
        ok, folders, msg = store.list_domain_folders()
        if not ok:
            raise ValidationError(f"Cannot list registry domains: {msg}")
        files: Dict[str, str] = {}
        errors: List[str] = []
        index: Dict[str, List[str]] = {}
        published: List[tuple] = []
        pushed: List[Dict[str, Any]] = []
        for folder in sorted(folders):
            versions = self._versions(store, folder)
            index[folder] = versions
            last_doc: Optional[Dict[str, Any]] = None
            for version in versions:
                ok, doc, msg = store.read_version(folder, version)
                if not ok:
                    errors.append(f"{folder}/V{version}: {msg}")
                    continue
                vfiles, verrors = self._version_files(doc, folder, version)
                files.update(vfiles)
                errors.extend(f"{folder}/V{version}: {e}" for e in verrors)
                pushed.append({"folder": folder, "version": version, "errors": verrors})
                last_doc = doc
                if (doc.get("info") or {}).get("status") == STATUS_PUBLISHED:
                    published.append((folder, version))
            if last_doc is not None:
                files.update(
                    self._domain_files(last_doc, folder, versions[-1], versions)
                )
        files[self.cfg.path("README.md")] = DomainRepoLayout.readme(index)
        if global_config:
            snapshot = {
                k: global_config[k] for k in _GLOBAL_CONFIG_KEYS if k in global_config
            }
            files[self.cfg.path(GLOBAL_CONFIG_FILE)] = DomainRepoLayout.dump(
                DomainRepoLayout._scrub(snapshot)
            )
        sha = self.client.commit_files(
            files,
            self._message("sync", "all domains", actor),
            replace_prefixes=[self.cfg.path("domains")],
            author=self._author,
        )
        target = sha or self.client.head_sha()
        if target:
            for folder, version in published:
                self.client.tag(f"{folder}/V{version}", target)
        return {
            "commit_sha": sha,
            "errors": errors,
            "domains": len(index),
            "versions": sum(len(v) for v in index.values()),
            "pushed": pushed,
        }

    # ------------------------------------------------------------------
    # Read back (Import from GitHub)
    # ------------------------------------------------------------------

    @staticmethod
    def _version_sort_key(version: str) -> List[int]:
        return [int(x) for x in version.split(".") if x.isdigit()] or [0]

    def list_domain_versions(self) -> Dict[str, List[str]]:
        """Return ``{folder: [version, ...]}`` from the Git tree (no blob reads)."""
        prefix = self.cfg.path("domains")
        listing = self.client.list_files(prefix)
        base = f"{self.cfg.base_path}/" if self.cfg.base_path else ""
        grouped: Dict[str, set[str]] = {}
        for path in listing:
            rel = path[len(base) :] if base and path.startswith(base) else path
            parsed = DomainRepoLayout.parse_version_path(rel)
            if not parsed:
                continue
            folder, version, file_rel = parsed
            if not file_rel.endswith(".json"):
                continue
            grouped.setdefault(folder, set()).add(version)
        return {
            folder: sorted(versions, key=self._version_sort_key, reverse=True)
            for folder, versions in grouped.items()
        }

    def build_obx_envelope(self, *, exported_by: str = "") -> Dict[str, Any]:
        """Rebuild an ``.obx`` envelope from the repository JSON sources."""
        from back.objects.registry import obx_format

        started = time.monotonic()
        prefix = self.cfg.path("domains")
        listing = self.client.list_files(prefix)
        grouped: Dict[str, Dict[str, Dict[str, str]]] = {}
        base = f"{self.cfg.base_path}/" if self.cfg.base_path else ""
        for path, sha in listing.items():
            parsed = DomainRepoLayout.parse_version_path(path[len(base):])
            if not parsed:
                continue
            folder, version, rel = parsed
            if not rel.endswith(".json"):
                continue
            grouped.setdefault(folder, {}).setdefault(version, {})[rel] = (
                self.client.read_blob(sha).decode("utf-8")
            )

        domains: List[Dict[str, Any]] = []
        for folder in sorted(grouped):
            versions: Dict[str, Any] = {}
            for version, rel_files in grouped[folder].items():
                versions[version] = DomainRepoLayout.merge_version(rel_files, version)
            latest = max(
                versions,
                key=lambda v: [int(x) for x in v.split(".") if x.isdigit()] or [0],
            )
            domains.append(
                {
                    "name": folder,
                    "info": versions[latest].get("info", {}),
                    "versions": versions,
                }
            )
        envelope = obx_format.build_envelope(domains, exported_by=exported_by)
        envelope["source"] = f"github:{self.cfg.label}"
        logger.info(
            "GitHub OBX envelope from %s: %d files, %d domains in %.1fs",
            self.cfg.label,
            len(listing),
            len(domains),
            time.monotonic() - started,
        )
        return envelope
