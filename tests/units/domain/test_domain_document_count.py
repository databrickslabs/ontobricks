"""Domain document counts come from the Lakebase Knowledge Store."""

import importlib
from types import SimpleNamespace

from back.objects.domain import Domain


def test_count_documents_uses_store(monkeypatch):
    domain_module = importlib.import_module("back.objects.domain.Domain")
    session = SimpleNamespace(
        uc_domain_folder="sales",
        domain_folder="sales",
        current_version="1",
    )

    class _Store:
        def count_documents(self, folder, version):
            assert (folder, version) == ("sales", "1")
            return 2

    class _Svc:
        store = _Store()

    registry_module = importlib.import_module("back.objects.registry")
    monkeypatch.setattr(
        registry_module.RegistryService,
        "from_context",
        classmethod(lambda cls, _session, _settings: _Svc()),
    )

    assert Domain(session).count_documents(object()) == 2


def test_count_documents_zero_without_registry_folder():
    session = SimpleNamespace(
        uc_domain_folder="", domain_folder="", current_version="1"
    )
    assert Domain(session).count_documents(object()) == 0
