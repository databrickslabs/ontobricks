"""Unit tests for graph read-query bounds (:mod:`back.core.query_limits`).

Pins the resolution order (admin override > env var > built-in default) and the
clamping that keeps a misconfiguration from disabling the guard. These bounds
are what stop a broad Graph Chat traversal from freezing the app.
"""

import back.core.query_limits as ql


def _reset_overrides():
    ql.set_graph_query_timeout_override(None)
    ql.set_graph_chat_result_cap_override(None)


class TestGraphQueryTimeout:
    def test_builtin_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S", raising=False)
        _reset_overrides()
        assert ql.get_graph_query_timeout_s() == ql.DEFAULT_GRAPH_QUERY_TIMEOUT_S

    def test_env_var_used_when_no_override(self, monkeypatch):
        _reset_overrides()
        monkeypatch.setenv("ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S", "90")
        assert ql.get_graph_query_timeout_s() == 90

    def test_admin_override_beats_env(self, monkeypatch):
        monkeypatch.setenv("ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S", "90")
        ql.set_graph_query_timeout_override(120)
        try:
            assert ql.get_graph_query_timeout_s() == 120
        finally:
            _reset_overrides()

    def test_override_clamped_to_bounds(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S", raising=False)
        try:
            ql.set_graph_query_timeout_override(10_000_000)
            assert ql.get_graph_query_timeout_s() == ql._MAX_TIMEOUT_S
            ql.set_graph_query_timeout_override(1)
            assert ql.get_graph_query_timeout_s() == ql._MIN_TIMEOUT_S
        finally:
            _reset_overrides()

    def test_zero_override_clears(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S", raising=False)
        ql.set_graph_query_timeout_override(120)
        ql.set_graph_query_timeout_override(0)
        try:
            assert ql.get_graph_query_timeout_s() == ql.DEFAULT_GRAPH_QUERY_TIMEOUT_S
        finally:
            _reset_overrides()

    def test_non_integer_env_ignored(self, monkeypatch):
        _reset_overrides()
        monkeypatch.setenv("ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S", "not-a-number")
        assert ql.get_graph_query_timeout_s() == ql.DEFAULT_GRAPH_QUERY_TIMEOUT_S


class TestGraphChatResultCap:
    def test_builtin_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_GRAPH_CHAT_RESULT_CAP", raising=False)
        _reset_overrides()
        assert ql.get_graph_chat_result_cap() == ql.DEFAULT_GRAPH_CHAT_RESULT_CAP

    def test_admin_override_beats_env(self, monkeypatch):
        monkeypatch.setenv("ONTOBRICKS_GRAPH_CHAT_RESULT_CAP", "5000")
        ql.set_graph_chat_result_cap_override(2500)
        try:
            assert ql.get_graph_chat_result_cap() == 2500
        finally:
            _reset_overrides()

    def test_override_clamped_to_bounds(self, monkeypatch):
        monkeypatch.delenv("ONTOBRICKS_GRAPH_CHAT_RESULT_CAP", raising=False)
        try:
            ql.set_graph_chat_result_cap_override(10_000_000)
            assert ql.get_graph_chat_result_cap() == ql._MAX_RESULT_CAP
            ql.set_graph_chat_result_cap_override(1)
            assert ql.get_graph_chat_result_cap() == ql._MIN_RESULT_CAP
        finally:
            _reset_overrides()
