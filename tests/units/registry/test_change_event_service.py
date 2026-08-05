"""Tests for the RegistryService change-audit wrappers.

Thin delegation to the store; the store itself is mocked. Focus is on the
call shape (arguments forwarded verbatim) and the return passthrough.
"""

from unittest.mock import MagicMock

from back.objects.registry.RegistryService import RegistryService


def _svc(store):
    svc = RegistryService.__new__(RegistryService)
    svc._store = store
    return svc


def test_record_change_events_delegates():
    store = MagicMock()
    store.record_change_events.return_value = (True, "")
    events = [{"action": "class_added"}]
    ok, msg = _svc(store).record_change_events("acme", "1", "a@x.com", events)
    assert (ok, msg) == (True, "")
    store.record_change_events.assert_called_once_with("acme", "1", "a@x.com", events)


def test_list_change_events_delegates_with_defaults():
    store = MagicMock()
    store.list_change_events.return_value = [{"action": "class_added"}]
    out = _svc(store).list_change_events("acme")
    assert out == [{"action": "class_added"}]
    store.list_change_events.assert_called_once_with("acme", None, 500)


def test_list_change_events_forwards_version_and_limit():
    store = MagicMock()
    store.list_change_events.return_value = []
    _svc(store).list_change_events("acme", version="2", limit=10)
    store.list_change_events.assert_called_once_with("acme", "2", 10)
