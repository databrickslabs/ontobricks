"""Tests for GitHubSyncService / GitHubMirror against the fake GitHub API."""

from __future__ import annotations

import copy
import json

import pytest

from back.core.errors import ConflictError
from back.objects.registry import obx_format
from back.objects.registry.gitsync import (
    DomainRepoLayout,
    GitHubConfig,
    GitHubMirror,
    GitHubSyncService,
)
from tests.fixtures.github import FakeGitHub

UUID = "a" * 32


def _doc(status: str = "DRAFT", with_mapping: bool = True) -> dict:
    return {
        "info": {"name": "Sales", "description": "d", "status": status},
        "versions": {
            "1": {
                "ontology": {
                    "name": "Sales",
                    "base_uri": "http://test.org/sales#",
                    "classes": [{"uri": "http://test.org/sales#Order", "name": "Order"}],
                    "properties": [],
                },
                "assignment": {
                    "entities": (
                        [
                            {
                                "ontology_class": "http://test.org/sales#Order",
                                "ontology_class_label": "Order",
                                "sql_query": "SELECT * FROM orders",
                                "id_column": "order_id",
                                "attribute_mappings": {},
                            }
                        ]
                        if with_mapping
                        else []
                    ),
                    "relationships": [],
                },
                "design_layout": {},
                "metadata": {},
            }
        },
    }


class FakeStore:
    def __init__(self, global_config=None):
        self.global_config = global_config
        self.audit = []

    def load_global_config(self):
        return self.global_config

    def record_change_events(self, folder, version, actor, events):
        self.audit.extend((folder, version, actor, e) for e in events)
        return True, ""


class FakeRegistry(FakeStore):
    """The slice of RegistryStore used by the sync service and the mirror."""

    cache_key = "fake"

    def __init__(self, docs, global_config=None):
        super().__init__(global_config)
        self.docs = docs  # {folder: {version: doc}}

    def read_version(self, folder, version):
        doc = self.docs.get(folder, {}).get(version)
        return (True, copy.deepcopy(doc), "") if doc else (False, {}, "missing")

    def list_versions(self, folder):
        return True, list(self.docs.get(folder, {})), ""

    def list_domain_folders(self):
        return True, list(self.docs), ""


def _cfg(**kw) -> GitHubConfig:
    base = dict(enabled=True, owner="acme", repo="onto", instance_uuid=UUID)
    base.update(kw)
    return GitHubConfig(**base)


def _sync(gh: FakeGitHub, **kw) -> GitHubSyncService:
    return GitHubSyncService(_cfg(**kw), client_factory=lambda cfg: gh.client(cfg.branch))


class TestPushVersion:
    def test_pushes_version_tree_with_turtle_files(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc()}})
        result = _sync(gh).push_version(svc, "Sales", "1", actor="alice@example.com")
        files = gh.files()
        vdir = "domains/Sales/versions/V1"
        assert f"{vdir}/ontology/Sales.owl.ttl" in files
        assert f"{vdir}/mapping/Sales.r2rml.ttl" in files
        assert f"{vdir}/manifest.json" in files
        assert json.loads(files["domains/Sales/domain.json"])["versions"] == ["1"]
        assert result["errors"] == []
        assert gh.commits[result["commit_sha"]]["message"] == (
            "save(Sales/V1): by alice@example.com"
        )

    def test_stale_r2rml_is_deleted_when_mappings_are_removed(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc()}})
        sync = _sync(gh)
        sync.push_version(svc, "Sales", "1")
        svc.docs["Sales"]["1"] = _doc(with_mapping=False)
        sync.push_version(svc, "Sales", "1")
        files = gh.files()
        assert "domains/Sales/versions/V1/mapping/Sales.r2rml.ttl" not in files
        assert "domains/Sales/versions/V1/ontology/Sales.owl.ttl" in files

    def test_repeated_push_without_change_makes_no_commit(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc()}})
        sync = _sync(gh)
        sync.push_version(svc, "Sales", "1")
        assert sync.push_version(svc, "Sales", "1")["commit_sha"] is None

    def test_published_version_is_tagged(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc(status="PUBLISHED")}})
        result = _sync(gh).push_version(svc, "Sales", "1")
        assert gh.refs["tags/Sales/V1"] == result["commit_sha"]

    def test_base_path_prefixes_everything(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc()}})
        _sync(gh, base_path="ontobricks").push_version(svc, "Sales", "1")
        assert any(p.startswith("ontobricks/domains/Sales/") for p in gh.files())

    def test_delete_domain_removes_folder(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc()}})
        sync = _sync(gh)
        sync.push_version(svc, "Sales", "1")
        sync.delete_domain("Sales")
        assert not any(p.startswith("domains/Sales") for p in gh.files())


class TestClaim:
    def test_claims_free_repository(self):
        gh = FakeGitHub({"README.md": "r"})
        _sync(gh).ensure_claim()
        claim = json.loads(gh.files()[".ontobricks/instance.json"])
        assert claim["instance_uuid"] == UUID

    def test_refuses_repository_of_another_instance(self):
        gh = FakeGitHub(
            {".ontobricks/instance.json": json.dumps({"instance_uuid": "other"})}
        )
        with pytest.raises(ConflictError):
            _sync(gh).ensure_claim()
        _sync(gh).ensure_claim(take_over=True)
        assert json.loads(gh.files()[".ontobricks/instance.json"])["instance_uuid"] == UUID

    def test_test_connection_reports_repository(self):
        gh = FakeGitHub({"README.md": "r"})
        info = _sync(gh).test_connection()
        assert info["repository"] == "acme/onto"
        assert info["private"] is True


class TestSyncAllAndImport:
    def test_sync_all_rewrites_domains_and_readme(self):
        gh = FakeGitHub({"domains/Orphan/domain.json": "{}"})
        svc = FakeRegistry(
            {"Sales": {"1": _doc(), "2": _doc()}, "Hr": {"1": _doc(with_mapping=False)}},
        )
        result = _sync(gh).sync_all(
            svc, global_config={"default_base_uri": "http://x", "warehouse_id": "w"}
        )
        files = gh.files()
        assert result["domains"] == 2 and result["versions"] == 3
        assert "domains/Orphan/domain.json" not in files
        assert "domains/Sales/versions/V2/ontology/Sales.owl.ttl" in files
        assert "| [Sales](domains/Sales) | V1, V2 |" in files["README.md"]
        assert json.loads(files[".ontobricks/global_config.json"]) == {
            "default_base_uri": "http://x"
        }

    def test_obx_envelope_round_trip(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry({"Sales": {"1": _doc()}})
        sync = _sync(gh)
        sync.push_version(svc, "Sales", "1")
        envelope = obx_format.load(sync.build_obx_envelope(exported_by="me"))
        assert envelope["source"] == "github:acme/onto@main"
        (entry,) = envelope["domains"]
        assert entry["name"] == "Sales"
        assert entry["versions"]["1"] == _doc()

    def test_list_domain_versions_does_not_read_blobs(self):
        gh = FakeGitHub({"README.md": "r"})
        svc = FakeRegistry(
            {"Sales": {"1": _doc(), "2": _doc()}, "Hr": {"1": _doc()}}
        )
        sync = _sync(gh)
        sync.sync_all(svc)
        blob_gets = [
            (m, p) for m, p in gh.calls if m == "GET" and "/git/blobs/" in p
        ]
        listed = sync.list_domain_versions()
        blob_gets_after = [
            (m, p) for m, p in gh.calls if m == "GET" and "/git/blobs/" in p
        ]
        assert listed == {"Hr": ["1"], "Sales": ["2", "1"]}
        assert blob_gets_after == blob_gets


class TestMirror:
    def _mirror(self):
        return GitHubMirror()

    def test_disabled_config_enqueues_nothing(self):
        mirror = self._mirror()
        svc = FakeRegistry({}, global_config={"github_config": {"enabled": False}})
        mirror.notify_write(svc, "Sales", "1")
        assert mirror.status()["pending"] == 0

    def test_store_without_global_config_is_ignored(self):
        mirror = self._mirror()

        class Bare:
            cache_key = "bare"

        mirror.notify_write(Bare(), "Sales", "1")
        assert mirror.status()["pending"] == 0

    def test_saves_are_coalesced(self, monkeypatch):
        mirror = self._mirror()
        monkeypatch.setattr(mirror, "_ensure_worker", lambda: None)
        svc = FakeRegistry({}, global_config={"github_config": _cfg().to_dict()})
        mirror.notify_write(svc, "Sales", "1")
        mirror.notify_write(svc, "Sales", "1")
        mirror.notify_write(svc, "Sales", "2")
        assert mirror.status()["pending"] == 2
        mirror.notify_delete_domain(svc, "Sales")
        assert mirror.status()["pending"] == 1

    def test_process_pushes_and_records_status(self, monkeypatch):
        gh = FakeGitHub({"README.md": "r"})
        monkeypatch.setattr(
            GitHubSyncService, "_default_client", classmethod(lambda cls, cfg: gh.client())
        )
        mirror = self._mirror()
        svc = FakeRegistry(
            {"Sales": {"1": _doc()}}, global_config={"github_config": _cfg().to_dict()}
        )
        from back.objects.registry.gitsync.GitHubMirror import _Job

        mirror.process(_Job("push", svc, "Sales", "1", "bob@example.com"))
        status = mirror.status()
        assert status["state"] == "ok" and status["commit_sha"]
        assert ".ontobricks/instance.json" in gh.files()

    def test_process_failure_is_recorded_not_raised(self, monkeypatch):
        def boom(cls, cfg):
            raise RuntimeError("github down")

        monkeypatch.setattr(GitHubSyncService, "_default_client", classmethod(boom))
        mirror = self._mirror()
        svc = FakeRegistry(
            {"Sales": {"1": _doc()}}, global_config={"github_config": _cfg().to_dict()}
        )
        from back.objects.registry.gitsync.GitHubMirror import _Job

        mirror.process(_Job("push", svc, "Sales", "1"))
        status = mirror.status()
        assert status["state"] == "error"
        assert "github down" in status["message"]


class TestAuditTrail:
    """Mirror pushes land in the domain change audit log (source='github')."""

    def _mirror(self, monkeypatch, gh):
        monkeypatch.setattr(
            GitHubSyncService, "_default_client", classmethod(lambda cls, cfg: gh.client())
        )
        return GitHubMirror()

    def test_push_is_audited_with_commit_link_and_tag(self, monkeypatch):
        from back.objects.registry.gitsync.GitHubMirror import _Job

        gh = FakeGitHub({"README.md": "r"})
        mirror = self._mirror(monkeypatch, gh)
        store = FakeRegistry(
            {"Sales": {"1": _doc(status="PUBLISHED")}},
            global_config={"github_config": _cfg().to_dict()},
        )
        mirror.process(_Job("push", store, "Sales", "1", "bob@example.com"))
        [(folder, version, actor, event)] = store.audit
        assert (folder, version, actor) == ("Sales", "1", "bob@example.com")
        assert event["source"] == "github" and event["action"] == "github_pushed"
        sha = event["meta"]["commit_sha"]
        assert sha and event["meta"]["commit_url"].endswith(f"/acme/onto/commit/{sha}")
        assert event["meta"]["tag"] == "Sales/V1"

    def test_unchanged_push_is_not_audited(self, monkeypatch):
        from back.objects.registry.gitsync.GitHubMirror import _Job

        gh = FakeGitHub({"README.md": "r"})
        mirror = self._mirror(monkeypatch, gh)
        store = FakeRegistry(
            {"Sales": {"1": _doc()}}, global_config={"github_config": _cfg().to_dict()}
        )
        mirror.process(_Job("push", store, "Sales", "1"))
        mirror.process(_Job("push", store, "Sales", "1"))
        assert len(store.audit) == 1

    def test_failure_is_audited(self, monkeypatch):
        from back.objects.registry.gitsync.GitHubMirror import _Job

        def boom(cls, cfg):
            raise RuntimeError("github down")

        monkeypatch.setattr(GitHubSyncService, "_default_client", classmethod(boom))
        store = FakeRegistry(
            {"Sales": {"1": _doc()}}, global_config={"github_config": _cfg().to_dict()}
        )
        GitHubMirror().process(_Job("push", store, "Sales", "1"))
        [(_, _, _, event)] = store.audit
        assert event["action"] == "github_push_failed"
        assert "github down" in event["meta"]["message"]

    def test_audit_failure_never_raises(self):
        class Broken:
            def record_change_events(self, *a):
                raise RuntimeError("db down")

        GitHubMirror.audit(Broken(), _cfg(), "Sales", "1", "", "github_pushed")

    def test_sync_all_reports_each_version(self):
        gh = FakeGitHub({"README.md": "r"})
        store = FakeRegistry({"Sales": {"1": _doc(), "2": _doc()}, "Ops": {"1": _doc()}})
        result = _sync(gh).sync_all(store)
        assert sorted((p["folder"], p["version"]) for p in result["pushed"]) == [
            ("Ops", "1"), ("Sales", "1"), ("Sales", "2")
        ]


class TestStoreHooks:
    """``GitHubMirrorHooks`` notifies after successful store writes only."""

    @staticmethod
    def _store(write_ok=True):
        from back.objects.registry.gitsync import GitHubMirrorHooks

        class Base:
            cache_key = "k"

            def write_version(self, folder, version, data):
                return write_ok, ""

            def update_version_status(self, folder, version, status):
                return True, ""

            def delete_version(self, folder, version):
                return True, ""

            def delete_domain(self, folder):
                return []

        class Store(GitHubMirrorHooks, Base):
            pass

        return Store()

    @staticmethod
    def _record(monkeypatch):
        from back.objects.registry.gitsync import github_mirror

        calls = []
        for hook in ("notify_write", "notify_delete_version", "notify_delete_domain"):
            monkeypatch.setattr(
                github_mirror, hook, lambda *a, _h=hook: calls.append((_h, *a[1:]))
            )
        return calls

    def test_writes_and_deletes_notify_the_mirror(self, monkeypatch):
        calls = self._record(monkeypatch)
        store = self._store()
        assert store.write_version("Sales", "1", "{}") == (True, "")
        store.update_version_status("Sales", "1", "PUBLISHED")
        store.delete_version("Sales", "1")
        assert store.delete_domain("Sales") == []
        assert calls == [
            ("notify_write", "Sales", "1"),
            ("notify_write", "Sales", "1"),
            ("notify_delete_version", "Sales", "1"),
        ]

    def test_failed_write_does_not_notify(self, monkeypatch):
        calls = self._record(monkeypatch)
        assert self._store(write_ok=False).write_version("Sales", "1", "{}")[0] is False
        assert calls == []

    def test_lakebase_store_carries_the_hooks(self):
        from back.objects.registry.gitsync import GitHubMirrorHooks
        from back.objects.registry.store.lakebase import LakebaseRegistryStore

        assert LakebaseRegistryStore.__mro__[1] is GitHubMirrorHooks

    def test_actor_comes_from_request_identity(self, monkeypatch):
        from back.core.databricks.request_identity import (
            RequestIdentity,
            reset_request_identity,
            set_request_identity,
        )

        mirror = GitHubMirror()
        monkeypatch.setattr(mirror, "_ensure_worker", lambda: None)
        store = FakeRegistry({}, global_config={"github_config": _cfg().to_dict()})
        token = set_request_identity(RequestIdentity(email="carol@example.com"))
        try:
            mirror.notify_write(store, "Sales", "1")
        finally:
            reset_request_identity(token)
        assert mirror._pending[("Sales", "1")].actor == "carol@example.com"


def test_push_writes_domain_readme():
    gh = FakeGitHub({"README.md": "r"})
    store = FakeRegistry({"Sales": {"1": _doc(), "2": _doc()}})
    _sync(gh).push_version(store, "Sales", "2")
    readme = gh.files()["domains/Sales/README.md"]
    assert "| V1 | [versions/V1](versions/V1) |" in readme
    assert "Summary of: V2" in readme
    assert "domains/Sales/domain.json" in gh.files()


def test_layout_helpers_are_exported():
    assert DomainRepoLayout.version_dir("Sales", "3") == "domains/Sales/versions/V3"
