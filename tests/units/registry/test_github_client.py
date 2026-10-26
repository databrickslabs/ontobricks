"""Tests for GitHubClient against an in-memory fake of the GitHub Git Data API."""

from __future__ import annotations

import httpx
import pytest

from back.core.errors import InfrastructureError
from back.core.github import GitHubClient
from tests.fixtures.github import FakeGitHub


class TestCommitFiles:
    def test_single_atomic_commit_with_replace(self):
        gh = FakeGitHub(
            {
                "README.md": "hi",
                "domains/A/versions/V1/mapping/A.r2rml.ttl": "old",
                "domains/A/versions/V1/info.json": "{}",
                "domains/B/info.json": "keep",
            }
        )
        with gh.client() as client:
            sha = client.commit_files(
                {"domains/A/versions/V1/info.json": '{"name": "A"}'},
                "save(A/V1)",
                replace_prefixes=["domains/A/versions/V1"],
                author={"name": "Bot", "email": "bot@example.com"},
            )
        assert sha == gh.refs["heads/main"]
        assert gh.files() == {
            "README.md": "hi",
            "domains/A/versions/V1/info.json": '{"name": "A"}',
            "domains/B/info.json": "keep",
        }
        assert gh.commits[sha]["message"] == "save(A/V1)"
        assert gh.commits[sha]["author"] == {"name": "Bot", "email": "bot@example.com"}
        assert sum(1 for c in gh.calls if c == ("POST", "/git/commits")) == 1

    def test_unchanged_tree_creates_no_commit(self):
        gh = FakeGitHub({"a.json": "1"})
        head = gh.refs["heads/main"]
        with gh.client() as client:
            assert client.commit_files({"a.json": "1"}, "noop") is None
        assert gh.refs["heads/main"] == head

    def test_retries_when_branch_moves(self):
        gh = FakeGitHub({"a.json": "1"})
        gh.reject_ref_updates = 1
        with gh.client() as client:
            sha = client.commit_files({"a.json": "2"}, "update")
        assert sha == gh.refs["heads/main"]
        assert gh.files()["a.json"] == "2"

    def test_gives_up_after_retries(self):
        gh = FakeGitHub({"a.json": "1"})
        gh.reject_ref_updates = 10
        with gh.client() as client, pytest.raises(InfrastructureError):
            client.commit_files({"a.json": "2"}, "update")

    def test_initializes_empty_repository(self):
        gh = FakeGitHub(empty=True)
        with gh.client() as client:
            client.commit_files({"x.json": "{}"}, "first")
        assert gh.files()["x.json"] == "{}"
        assert "README.md" in gh.files()

    def test_creates_missing_branch_from_default(self):
        gh = FakeGitHub({"a.json": "1"})
        with gh.client("ontobricks") as client:
            client.commit_files({"b.json": "2"}, "branch")
        assert gh.files("ontobricks") == {"a.json": "1", "b.json": "2"}
        assert gh.files("main") == {"a.json": "1"}


class TestReadsAndTags:
    def test_list_and_read(self):
        gh = FakeGitHub({"domains/A/x.json": "X", "README.md": "r"})
        with gh.client() as client:
            listing = client.list_files("domains")
            assert list(listing) == ["domains/A/x.json"]
            assert client.read_blob(listing["domains/A/x.json"]) == b"X"
            assert client.read_file("README.md") == b"r"
            assert client.read_file("missing.json") is None

    def test_tag_create_then_move(self):
        gh = FakeGitHub({"a": "1"})
        with gh.client() as client:
            head = client.head_sha()
            client.tag("Sales/V1", head)
            sha = client.commit_files({"a": "2"}, "update")
            client.tag("Sales/V1", sha)
        assert gh.refs["tags/Sales/V1"] == sha

    def test_git_requests_stay_under_the_repo_prefix(self):
        seen = []

        def capture(request):
            seen.append(request.url.path)
            return httpx.Response(404, json={"message": "Not Found"})

        client = GitHubClient(
            "tok", "acme", "onto", transport=httpx.MockTransport(capture)
        )
        assert client.list_files() == {}
        assert seen
        assert all(p.rstrip("/").startswith("/repos/acme/onto") for p in seen)
        assert any("/git/ref/heads/" in p for p in seen)

    def test_get_repo_does_not_use_a_trailing_slash(self):
        seen = []

        def capture(request):
            seen.append(str(request.url))
            return httpx.Response(
                200,
                json={"full_name": "acme/onto", "permissions": {"push": True}},
            )

        client = GitHubClient(
            "tok", "acme", "onto", transport=httpx.MockTransport(capture)
        )
        assert client.get_repo()["full_name"] == "acme/onto"
        assert seen == ["https://api.github.com/repos/acme/onto"]

    def test_auth_error_is_friendly(self):
        def deny(_request):
            return httpx.Response(401, json={"message": "Bad credentials"})

        client = GitHubClient("tok", "acme", "onto", transport=httpx.MockTransport(deny))
        with pytest.raises(InfrastructureError, match="401"):
            client.get_repo()
