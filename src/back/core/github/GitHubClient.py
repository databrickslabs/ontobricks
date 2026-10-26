"""Minimal GitHub REST client for mirroring files into a repository.

Uses the Git Data API (refs, trees, commits) over ``httpx`` so a batch of
file writes and deletions lands as **one atomic commit** — no ``git``
binary and no clone, which a Databricks App cannot rely on.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx

from back.core.errors import InfrastructureError, NotFoundError
from back.core.logging import get_logger

from .constants import (
    GITHUB_API_URL,
    GITHUB_API_VERSION,
    GITHUB_COMMIT_RETRIES,
    GITHUB_TIMEOUT_S,
)

logger = get_logger(__name__)


class GitHubClient:
    """Read and commit files on one branch of one repository."""

    def __init__(
        self,
        token: str,
        owner: str,
        repo: str,
        branch: str = "main",
        *,
        api_url: str = GITHUB_API_URL,
        timeout: float = GITHUB_TIMEOUT_S,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        if not token:
            raise InfrastructureError("GitHub token is missing")
        if not owner or not repo:
            raise InfrastructureError("GitHub owner and repository are required")
        self.owner = owner
        self.repo = repo
        self.branch = branch or "main"
        self._repo_url = f"{api_url.rstrip('/')}/repos/{owner}/{repo}"
        # Trailing slash + relative paths: a leading "/" on the request path
        # would make httpx join against the host and drop /repos/{owner}/{repo}.
        self._http = httpx.Client(
            base_url=self._repo_url + "/",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": GITHUB_API_VERSION,
            },
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "GitHubClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # HTTP plumbing
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, **kw: Any) -> httpx.Response:
        rel = path if path.startswith("http") else path.lstrip("/")
        try:
            return self._http.request(method, rel, **kw)
        except httpx.HTTPError as exc:
            raise InfrastructureError(
                "Could not reach the GitHub API", detail=str(exc)
            ) from exc

    def _json(self, method: str, path: str, *, ok=(200, 201), **kw: Any) -> Any:
        resp = self._request(method, path, **kw)
        if resp.status_code in ok:
            return resp.json() if resp.content else {}
        self._raise(resp, f"{method} {path}")
        return None

    @staticmethod
    def _raise(resp: httpx.Response, what: str) -> None:
        try:
            message = resp.json().get("message", "")
        except ValueError:
            message = resp.text[:200]
        if resp.status_code == 401:
            raise InfrastructureError(
                "GitHub rejected the token (401) — check the secret value",
                detail=message,
            )
        if resp.status_code == 403:
            raise InfrastructureError(
                "GitHub denied access (403) — the token needs Contents read/write "
                "on this repository",
                detail=message,
            )
        if resp.status_code == 404:
            raise NotFoundError(f"GitHub resource not found: {what}", detail=message)
        raise InfrastructureError(
            f"GitHub API error {resp.status_code} on {what}", detail=message
        )

    # ------------------------------------------------------------------
    # Repository / branch
    # ------------------------------------------------------------------

    def get_repo(self) -> Dict[str, Any]:
        """Return repository metadata (``permissions``, ``default_branch``, ...)."""
        # Absolute URL without a trailing slash — GitHub 404s ``.../repo/``.
        return self._json("GET", self._repo_url)

    def _head(self) -> Optional[Tuple[str, str]]:
        """Return ``(commit_sha, tree_sha)`` of the branch head, or ``None``."""
        resp = self._request("GET", f"/git/ref/heads/{self.branch}")
        if resp.status_code in (404, 409):
            return None
        if resp.status_code != 200:
            self._raise(resp, f"ref heads/{self.branch}")
        commit_sha = resp.json()["object"]["sha"]
        commit = self._json("GET", f"/git/commits/{commit_sha}")
        return commit_sha, commit["tree"]["sha"]

    def _ensure_branch(self) -> Tuple[str, str]:
        """Return the branch head, creating the branch (or first commit) if needed."""
        head = self._head()
        if head:
            return head
        repo = self.get_repo()
        default = repo.get("default_branch") or "main"
        if default != self.branch:
            resp = self._request("GET", f"/git/ref/heads/{default}")
            if resp.status_code == 200:
                self._json(
                    "POST",
                    "/git/refs",
                    json={
                        "ref": f"refs/heads/{self.branch}",
                        "sha": resp.json()["object"]["sha"],
                    },
                )
                return self._head()  # type: ignore[return-value]
        # Empty repository: the Git Data API refuses to work until a first
        # commit exists, and the Contents API is the only way to create it.
        self._json(
            "PUT",
            "/contents/README.md",
            json={
                "message": "Initialize OntoBricks repository",
                "content": base64.b64encode(b"# OntoBricks\n").decode("ascii"),
                "branch": self.branch,
            },
        )
        head = self._head()
        if not head:
            raise InfrastructureError(
                f"Could not initialize branch '{self.branch}' on GitHub"
            )
        return head

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list_files(self, prefix: str = "") -> Dict[str, str]:
        """Return ``{path: blob_sha}`` for every file under *prefix* on the branch."""
        head = self._head()
        if not head:
            return {}
        return self._list_tree(head[1], prefix)

    def _list_tree(self, tree_sha: str, prefix: str = "") -> Dict[str, str]:
        data = self._json("GET", f"/git/trees/{tree_sha}", params={"recursive": "1"})
        if data.get("truncated"):
            logger.warning(
                "GitHub tree listing for %s/%s is truncated", self.owner, self.repo
            )
        prefix = prefix.strip("/")
        out: Dict[str, str] = {}
        for entry in data.get("tree", []):
            if entry.get("type") != "blob":
                continue
            path = entry["path"]
            if not prefix or path == prefix or path.startswith(prefix + "/"):
                out[path] = entry["sha"]
        return out

    def read_blob(self, sha: str) -> bytes:
        data = self._json("GET", f"/git/blobs/{sha}")
        return base64.b64decode(data.get("content", ""))

    def read_file(self, path: str) -> Optional[bytes]:
        """Return the bytes of *path* on the branch, or ``None`` when absent."""
        resp = self._request("GET", f"/contents/{path}", params={"ref": self.branch})
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            self._raise(resp, f"contents/{path}")
        return base64.b64decode(resp.json().get("content", ""))

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def commit_files(
        self,
        files: Dict[str, str],
        message: str,
        *,
        replace_prefixes: Iterable[str] = (),
        author: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Write *files* (``{path: text}``) in one commit.

        Every existing file under one of *replace_prefixes* that is not in
        *files* is deleted in the same commit. Returns the new commit SHA,
        or ``None`` when the resulting tree is identical to the branch head.
        """
        prefixes = [p.strip("/") for p in replace_prefixes if p.strip("/")]
        last_error: Optional[Exception] = None
        for attempt in range(1, GITHUB_COMMIT_RETRIES + 1):
            head_sha, base_tree = self._ensure_branch()
            entries: List[Dict[str, Any]] = [
                {"path": path, "mode": "100644", "type": "blob", "content": text}
                for path, text in sorted(files.items())
            ]
            if prefixes:
                existing = self._list_tree(base_tree)
                for path in sorted(existing):
                    if path in files:
                        continue
                    if any(path == p or path.startswith(p + "/") for p in prefixes):
                        entries.append(
                            {"path": path, "mode": "100644", "type": "blob", "sha": None}
                        )
            if not entries:
                return None
            tree = self._json(
                "POST", "/git/trees", json={"base_tree": base_tree, "tree": entries}
            )
            if tree["sha"] == base_tree:
                return None
            body: Dict[str, Any] = {
                "message": message,
                "tree": tree["sha"],
                "parents": [head_sha],
            }
            if author and author.get("name") and author.get("email"):
                body["author"] = {"name": author["name"], "email": author["email"]}
            commit = self._json("POST", "/git/commits", json=body)
            resp = self._request(
                "PATCH",
                f"/git/refs/heads/{self.branch}",
                json={"sha": commit["sha"], "force": False},
            )
            if resp.status_code == 200:
                return commit["sha"]
            if resp.status_code == 422:
                last_error = InfrastructureError(
                    "GitHub branch moved during commit", detail=resp.text[:200]
                )
                logger.info(
                    "GitHub ref update not fast-forward (attempt %d/%d), retrying",
                    attempt,
                    GITHUB_COMMIT_RETRIES,
                )
                continue
            self._raise(resp, f"update ref heads/{self.branch}")
        raise last_error or InfrastructureError("GitHub commit failed")

    def tag(self, name: str, commit_sha: str) -> None:
        """Create or move the lightweight tag *name* to *commit_sha*."""
        resp = self._request(
            "POST", "/git/refs", json={"ref": f"refs/tags/{name}", "sha": commit_sha}
        )
        if resp.status_code == 201:
            return
        if resp.status_code == 422:
            self._json(
                "PATCH",
                f"/git/refs/tags/{name}",
                json={"sha": commit_sha, "force": True},
            )
            return
        self._raise(resp, f"tag {name}")

    def head_sha(self) -> Optional[str]:
        head = self._head()
        return head[0] if head else None
