"""Instance-wide GitHub mirror settings (``global_config['github_config']``)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict


@dataclass
class GitHubConfig:
    """One repository per OntoBricks instance.

    The token itself is never stored here — only the Databricks secret
    scope/key it is resolved from at push time.
    """

    enabled: bool = False
    owner: str = ""
    repo: str = ""
    branch: str = "main"
    base_path: str = ""
    token_secret_scope: str = ""
    token_secret_key: str = ""
    author_name: str = "OntoBricks"
    author_email: str = ""
    push_on_save: bool = True
    instance_uuid: str = ""

    @classmethod
    def from_dict(cls, data: Any) -> "GitHubConfig":
        if not isinstance(data, dict):
            return cls()
        return cls(
            enabled=bool(data.get("enabled", False)),
            owner=str(data.get("owner") or "").strip(),
            repo=str(data.get("repo") or "").strip(),
            branch=str(data.get("branch") or "main").strip() or "main",
            base_path=str(data.get("base_path") or "").strip().strip("/"),
            token_secret_scope=str(data.get("token_secret_scope") or "").strip(),
            token_secret_key=str(data.get("token_secret_key") or "").strip(),
            author_name=str(data.get("author_name") or "OntoBricks").strip(),
            author_email=str(data.get("author_email") or "").strip(),
            push_on_save=bool(data.get("push_on_save", True)),
            instance_uuid=str(data.get("instance_uuid") or "").strip(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def is_ready(self) -> bool:
        return bool(self.enabled and self.owner and self.repo)

    @classmethod
    def ready_for(cls, store) -> bool:
        """True when Settings → GitHub is enabled and has owner/repo."""
        try:
            data = store.load_global_config()
            if not isinstance(data, dict):
                return False
            return cls.from_dict(data.get("github_config")).is_ready
        except Exception:
            return False

    @property
    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def commit_url(self, sha: str) -> str:
        return f"https://github.com/{self.owner}/{self.repo}/commit/{sha}" if sha else ""

    def path(self, rel: str) -> str:
        """Prefix a repository-relative path with :attr:`base_path`."""
        rel = rel.strip("/")
        return f"{self.base_path}/{rel}" if self.base_path else rel
