"""In-memory fake of the GitHub Git Data API for GitHubClient tests."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Dict, Optional

import httpx

from back.core.github import GitHubClient


class FakeGitHub:
    """Just enough of the GitHub REST API for GitHubClient."""

    def __init__(self, files: Optional[Dict[str, str]] = None, empty: bool = False):
        self.blobs: Dict[str, str] = {}
        self.trees: Dict[str, Dict[str, str]] = {}
        self.commits: Dict[str, dict] = {}
        self.refs: Dict[str, str] = {}
        self.calls: list = []
        self.reject_ref_updates = 0
        if not empty:
            self.refs["heads/main"] = self._commit(self._tree(files or {}), [], "init")

    def _blob(self, content: str) -> str:
        sha = hashlib.sha1(content.encode()).hexdigest()
        self.blobs[sha] = content
        return sha

    def _tree(self, files: Dict[str, str]) -> str:
        entries = {p: self._blob(c) for p, c in files.items()}
        sha = hashlib.sha1(json.dumps(sorted(entries.items())).encode()).hexdigest()
        self.trees[sha] = entries
        return sha

    def _commit(self, tree: str, parents: list, message: str) -> str:
        sha = hashlib.sha1(f"{tree}{parents}{message}{len(self.commits)}".encode()).hexdigest()
        self.commits[sha] = {"tree": tree, "parents": parents, "message": message}
        return sha

    def files(self, branch: str = "main") -> Dict[str, str]:
        tree = self.commits[self.refs[f"heads/{branch}"]]["tree"]
        return {p: self.blobs[s] for p, s in self.trees[tree].items()}

    def handler(self, request: httpx.Request) -> httpx.Response:
        full = request.url.path
        if not full.rstrip("/").startswith("/repos/acme/onto"):
            return httpx.Response(
                404,
                json={"message": f"path escaped repo prefix: {full}"},
            )
        path = full.rstrip("/").removeprefix("/repos/acme/onto")
        method = request.method
        self.calls.append((method, path))
        body = json.loads(request.content) if request.content else {}

        if method == "GET" and path == "":
            return httpx.Response(
                200,
                json={
                    "full_name": "acme/onto",
                    "private": True,
                    "default_branch": "main",
                    "permissions": {"push": True},
                },
            )
        m = re.fullmatch(r"/git/ref/(heads/.+)", path)
        if method == "GET" and m:
            if not self.refs:
                return httpx.Response(409, json={"message": "Git Repository is empty."})
            sha = self.refs.get(m.group(1))
            if not sha:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json={"object": {"sha": sha}})
        m = re.fullmatch(r"/git/commits/(\w+)", path)
        if method == "GET" and m:
            return httpx.Response(200, json={"tree": {"sha": self.commits[m.group(1)]["tree"]}})
        m = re.fullmatch(r"/git/trees/(\w+)", path)
        if method == "GET" and m:
            entries = self.trees[m.group(1)]
            return httpx.Response(
                200,
                json={
                    "tree": [{"path": p, "type": "blob", "sha": s} for p, s in entries.items()],
                    "truncated": False,
                },
            )
        if method == "POST" and path == "/git/trees":
            files = {
                p: self.blobs[s] for p, s in self.trees[body["base_tree"]].items()
            }
            for entry in body["tree"]:
                if "content" in entry:
                    files[entry["path"]] = entry["content"]
                elif entry.get("sha") is None:
                    if entry["path"] not in files:
                        return httpx.Response(422, json={"message": "path not found"})
                    del files[entry["path"]]
            return httpx.Response(201, json={"sha": self._tree(files)})
        if method == "POST" and path == "/git/commits":
            sha = self._commit(body["tree"], body["parents"], body["message"])
            self.commits[sha]["author"] = body.get("author")
            return httpx.Response(201, json={"sha": sha})
        m = re.fullmatch(r"/git/refs/(heads/.+|tags/.+)", path)
        if method == "PATCH" and m:
            ref = m.group(1)
            if ref.startswith("heads/") and self.reject_ref_updates:
                self.reject_ref_updates -= 1
                # Simulate a concurrent push that moved the branch meanwhile.
                current = self.refs[ref]
                tree = self.commits[current]["tree"]
                self.refs[ref] = self._commit(tree, [current], "concurrent")
                return httpx.Response(422, json={"message": "Update is not a fast forward"})
            if ref.startswith("heads/") and self.commits[body["sha"]]["parents"] != [self.refs[ref]]:
                return httpx.Response(422, json={"message": "Update is not a fast forward"})
            self.refs[ref] = body["sha"]
            return httpx.Response(200, json={})
        if method == "POST" and path == "/git/refs":
            ref = body["ref"].removeprefix("refs/")
            if ref in self.refs:
                return httpx.Response(422, json={"message": "Reference already exists"})
            self.refs[ref] = body["sha"]
            return httpx.Response(201, json={})
        m = re.fullmatch(r"/contents/(.+)", path)
        if m and method == "GET":
            if not self.refs:
                return httpx.Response(404, json={"message": "Not Found"})
            content = self.files().get(m.group(1))
            if content is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(
                200, json={"content": base64.b64encode(content.encode()).decode()}
            )
        if m and method == "PUT":
            content = base64.b64decode(body["content"]).decode()
            self.refs[f"heads/{body['branch']}"] = self._commit(
                self._tree({m.group(1): content}), [], body["message"]
            )
            return httpx.Response(201, json={})
        m = re.fullmatch(r"/git/blobs/(\w+)", path)
        if method == "GET" and m:
            content = self.blobs[m.group(1)]
            return httpx.Response(
                200, json={"content": base64.b64encode(content.encode()).decode()}
            )
        return httpx.Response(500, json={"message": f"unhandled {method} {path}"})

    def client(self, branch: str = "main") -> GitHubClient:
        return GitHubClient(
            "tok", "acme", "onto", branch, transport=httpx.MockTransport(self.handler)
        )
