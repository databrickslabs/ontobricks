"""Tests for the ontology/mapping change-audit trail.

Covers the three legs of the feature end-to-end at the unit level:
  1. the session-level buffer (``record_change`` / ``drain_change_log``)
     and its isolation from change-detection + persisted export;
  2. the Ontology / Mapping instrumentation that appends buffered events
     (including per-entity bulk diffs and the ``source=agent`` tag);
  3. the flush in ``Domain.save_domain_to_uc`` that writes the buffer to
     the registry once and clears it.
"""

import copy

from unittest.mock import MagicMock

from back.objects.domain import Domain
from back.objects.mapping import Mapping
from back.objects.ontology import Ontology


# ----------------------------------------------------------------------
# 1. Session buffer
# ----------------------------------------------------------------------


class TestSessionBuffer:
    def test_record_appends_event_with_timestamp(self, domain_session):
        domain_session.record_change(
            "class_added", entity_type="class",
            entity_ref="http://x#C", summary="C",
        )
        log = domain_session._data["change_log"]
        assert len(log) == 1
        assert log[0]["action"] == "class_added"
        assert log[0]["entity_ref"] == "http://x#C"
        assert log[0]["source"] == "user"
        assert log[0]["ts"]  # ISO timestamp captured at edit time

    def test_drain_returns_and_clears(self, domain_session):
        domain_session.record_change("class_added")
        domain_session.record_change("class_removed")
        drained = domain_session.drain_change_log()
        assert [e["action"] for e in drained] == ["class_added", "class_removed"]
        assert domain_session._data["change_log"] == []

    def test_buffer_excluded_from_export(self, domain_session):
        domain_session.record_change("class_added")
        exported = domain_session.export_for_save()
        assert "change_log" not in exported
        version = exported["versions"][next(iter(exported["versions"]))]
        assert "change_log" not in version

    def test_buffer_does_not_trip_change_detection(self, domain_session):
        # Baseline snapshot with no ontology/mapping edits.
        domain_session._rebake_snapshots()
        before = domain_session._config_snapshot()
        domain_session.record_change("class_added")
        # The audit buffer must not change the config hash used to stamp
        # last_update / ontology_changed.
        assert domain_session._config_snapshot() == before


# ----------------------------------------------------------------------
# 2. Ontology instrumentation
# ----------------------------------------------------------------------


class TestOntologyInstrumentation:
    def test_add_class_buffers_event(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        Ontology(ps).add_class(
            {"name": "Product", "uri": "http://test.org/ontology#Product"}
        )
        log = ps._data["change_log"]
        assert log[-1]["action"] == "class_added"
        assert log[-1]["entity_ref"] == "http://test.org/ontology#Product"

    def test_update_class_buffers_event(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        Ontology(ps).update_class(
            {"uri": "http://test.org/ontology#Customer", "name": "Client"}
        )
        assert ps._data["change_log"][-1]["action"] == "class_updated"

    def test_delete_class_buffers_event(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        Ontology(ps).delete_class_by_uri("http://test.org/ontology#Customer")
        assert ps._data["change_log"][-1]["action"] == "class_removed"

    def test_bulk_save_emits_per_entity_diff(
        self, domain_session, sample_ontology_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        base = ps.info.get("name", "")
        cfg = copy.deepcopy(sample_ontology_config)
        # Add one class, remove another (drop first existing class).
        cfg["classes"] = cfg["classes"][1:] + [
            {"name": "Product", "uri": "http://test.org/ontology#Product"}
        ]
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})
        actions = [e["action"] for e in ps._data["change_log"]]
        assert "class_added" in actions
        assert "class_removed" in actions

    def test_agent_changes_tagged_source_agent(
        self, domain_session, sample_ontology_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        Ontology(ps).apply_agent_ontology_changes(
            classes=list(ps.get_classes()) + [
                {"name": "Robot", "uri": "http://test.org/ontology#Robot"}
            ],
            properties=list(ps.get_properties()),
            prune_orphan_mappings=False,
        )
        added = [e for e in ps._data["change_log"] if e["action"] == "class_added"]
        assert added and all(e["source"] == "agent" for e in added)


# ----------------------------------------------------------------------
# 2b. Mapping instrumentation
# ----------------------------------------------------------------------


class TestMappingInstrumentation:
    def test_add_entity_mapping_buffers_added(self, domain_session):
        ps = domain_session
        ps.assignment["entities"] = []
        Mapping(ps).add_or_update_entity_mapping(
            {"ontology_class": "http://x#Customer", "table_name": "customers"}
        )
        assert ps._data["change_log"][-1]["action"] == "mapping_entity_added"

    def test_update_entity_mapping_buffers_updated(self, domain_session):
        ps = domain_session
        ps.assignment["entities"] = [
            {"ontology_class": "http://x#Customer", "table_name": "old"}
        ]
        Mapping(ps).add_or_update_entity_mapping(
            {"ontology_class": "http://x#Customer", "table_name": "new"}
        )
        assert ps._data["change_log"][-1]["action"] == "mapping_entity_updated"

    def test_delete_entity_mapping_buffers_removed(self, domain_session):
        ps = domain_session
        ps.assignment["entities"] = [
            {"ontology_class": "http://x#Customer", "table_name": "t"}
        ]
        Mapping(ps).delete_entity_mapping("http://x#Customer")
        assert ps._data["change_log"][-1]["action"] == "mapping_entity_removed"

    def test_reset_mapping_buffers_event(self, domain_session):
        ps = domain_session
        ps.assignment["entities"] = [{"ontology_class": "http://x#C"}]
        Mapping(ps).reset_mapping()
        assert ps._data["change_log"][-1]["action"] == "mapping_reset"


# ----------------------------------------------------------------------
# 3. Flush on save-to-registry
# ----------------------------------------------------------------------


class TestFlushOnSave:
    def _domain(self):
        domain = MagicMock()
        domain.info = {"name": "Acme"}
        domain.current_version = "1"
        domain.domain_folder = "acme"
        domain.settings = {"registry": {"catalog": "c", "schema": "s", "volume": "v"}}
        domain.export_for_save = MagicMock(return_value={"info": {}})
        domain.clear_change_flags = MagicMock()
        domain.save = MagicMock()
        return domain

    def _svc(self):
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.get_version_status.return_value = "DRAFT"
        svc.write_version.return_value = (True, "")
        svc.record_change_events.return_value = (True, "")
        # No other editor holds the single-editor lock (realistic default;
        # the store returns None when the lock is free).
        svc.store.get_edit_lock.return_value = None
        return svc

    def test_flushes_buffer_to_registry(self):
        domain = self._domain()
        events = [{"action": "class_added", "source": "user"}]
        domain.drain_change_log = MagicMock(return_value=events)
        svc = self._svc()
        Domain(domain).save_domain_to_uc(svc, actor_email="alice@acme.com")
        svc.record_change_events.assert_called_once_with(
            "acme", "1", "alice@acme.com", events
        )

    def test_no_flush_when_buffer_empty(self):
        domain = self._domain()
        domain.drain_change_log = MagicMock(return_value=[])
        svc = self._svc()
        Domain(domain).save_domain_to_uc(svc)
        svc.record_change_events.assert_not_called()

    def test_audit_failure_does_not_break_save(self):
        domain = self._domain()
        domain.drain_change_log = MagicMock(return_value=[{"action": "x"}])
        svc = self._svc()
        svc.record_change_events.side_effect = RuntimeError("audit down")
        result = Domain(domain).save_domain_to_uc(svc)
        assert result["success"] is True
