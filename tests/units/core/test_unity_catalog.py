"""Tests for UnityCatalog (Databricks Unity Catalog metadata)."""

import importlib
import pytest
import requests
from unittest.mock import MagicMock, Mock, patch

_unity_catalog_mod = importlib.import_module("back.core.databricks.uc.UnityCatalog")

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.uc import UnityCatalog
from back.core.errors import ValidationError


def _make_sql_mocks(mock_connect, *, fetchall=None, fetchone=None):
    """Wire sql.connect context manager and cursor; set fetchall/fetchone on cursor."""
    mock_cursor = MagicMock()
    if fetchall is not None:
        mock_cursor.fetchall.return_value = fetchall
    if fetchone is not None:
        mock_cursor.fetchone.return_value = fetchone
    mock_conn = MagicMock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    return mock_cursor


@pytest.fixture
def clean_databricks_env(monkeypatch):
    """Avoid env-driven host/token/oauth when constructing DatabricksAuth in tests."""
    for key in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_APP_PORT",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def auth_with_warehouse(clean_databricks_env):
    return DatabricksAuth(
        host="https://test.cloud.databricks.com",
        token="test-pat",
        warehouse_id="warehouse-123",
    )


@pytest.fixture
def auth_no_warehouse(clean_databricks_env):
    return DatabricksAuth(
        host="https://test.cloud.databricks.com",
        token="test-pat",
        warehouse_id="",
    )


class TestGetCatalogs:
    def test_raises_value_error_without_warehouse_id(self, auth_no_warehouse):
        uc = UnityCatalog(auth_no_warehouse)
        with pytest.raises(ValidationError, match="SQL Warehouse ID is required"):
            uc.get_catalogs()

    @patch("databricks.sql.connect")
    def test_returns_catalog_names_on_success(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect, fetchall=[["main"], ["samples"], ["hive_metastore"]]
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_catalogs()
        assert out == ["main", "samples", "hive_metastore"]
        mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")


class TestGetSchemas:
    def test_raises_value_error_without_warehouse_id(self, auth_no_warehouse):
        uc = UnityCatalog(auth_no_warehouse)
        with pytest.raises(ValidationError, match="SQL Warehouse ID is required"):
            uc.get_schemas("main")

    @patch("databricks.sql.connect")
    def test_returns_schema_names(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect, fetchall=[["default"], ["information_schema"]]
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_schemas("main")
        assert out == ["default", "information_schema"]
        mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS IN `main`")


class TestGetTables:
    @patch("databricks.sql.connect")
    def test_returns_table_names_on_success(self, mock_connect, auth_with_warehouse):
        # SHOW TABLES rows: database, tableName, isTemporary
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchall=[["sch", "t1", "false"], ["sch", "t2", "false"]],
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_tables("cat", "sch")
        assert out == ["t1", "t2"]
        mock_cursor.execute.assert_called_once_with("SHOW TABLES IN `cat`.`sch`")

    @patch(
        "databricks.sql.connect",
        side_effect=RuntimeError("warehouse down"),
    )
    def test_returns_empty_list_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_tables("cat", "sch") == []


class TestListFunctions:
    # Input parameters and RETURNS TABLE result columns live in two different
    # information_schema views, so they are read by two separate statements.
    _ROUTINES = [
        ["recompute_risk", "STRING", "Recompute the risk score", "entity_id"],
        ["risk_history", "TABLE_TYPE", None, "days,entity_id"],
        ["no_args", "STRING", "", ""],
    ]
    _RETURN_COLUMNS = [
        ["risk_history", "as_of", "DATE"],
        ["risk_history", "score", "DOUBLE"],
    ]

    def _cursor(self, mock_connect, *, routines=None, return_columns=None):
        cursor = _make_sql_mocks(mock_connect)
        cursor.fetchall.side_effect = [
            self._ROUTINES if routines is None else routines,
            self._RETURN_COLUMNS if return_columns is None else return_columns,
        ]
        return cursor

    @patch("databricks.sql.connect")
    def test_returns_functions_with_param_metadata(
        self, mock_connect, auth_with_warehouse
    ):
        cursor = self._cursor(mock_connect)
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.list_functions("main", "ops")

        assert out == [
            {
                "name": "recompute_risk",
                "full_name": "main.ops.recompute_risk",
                "comment": "Recompute the risk score",
                "input_params": ["entity_id"],
                "param_count": 1,
                "returns_table": False,
                "return_type": "STRING",
                "return_columns": [],
            },
            {
                "name": "risk_history",
                "full_name": "main.ops.risk_history",
                "comment": "",
                "input_params": ["days", "entity_id"],
                "param_count": 2,
                "returns_table": True,
                "return_type": "TABLE",
                "return_columns": [
                    {"name": "as_of", "data_type": "DATE"},
                    {"name": "score", "data_type": "DOUBLE"},
                ],
            },
            {
                "name": "no_args",
                "full_name": "main.ops.no_args",
                "comment": "",
                "input_params": [],
                "param_count": 0,
                "returns_table": False,
                "return_type": "STRING",
                "return_columns": [],
            },
        ]
        call_sql, call_params = cursor.execute.call_args_list[0][0]
        # Parameter listing must come from information_schema: the REST
        # /functions collection endpoint does not populate input_params.
        assert "`main`.information_schema.routines" in call_sql
        assert "`main`.information_schema.parameters" in call_sql
        assert "p.parameter_mode   = 'IN'" in call_sql
        assert call_params == ("ops",)

    @patch("databricks.sql.connect")
    def test_return_columns_come_from_routine_columns_not_parameters(
        self, mock_connect, auth_with_warehouse
    ):
        """Databricks lists only the declared arguments in ``parameters``, so a
        table function contributes no row there for what it returns."""
        cursor = self._cursor(mock_connect)
        UnityCatalog(auth_with_warehouse).list_functions("main", "ops")

        call_sql, call_params = cursor.execute.call_args_list[1][0]
        assert "`main`.information_schema.routine_columns" in call_sql
        assert "information_schema.parameters" not in call_sql
        assert "ORDER BY r.routine_name, c.ordinal_position" in call_sql
        assert call_params == ("ops",)

    @patch("databricks.sql.connect")
    def test_unreadable_return_columns_do_not_cost_the_function_list(
        self, mock_connect, auth_with_warehouse
    ):
        """The action picker only needs the parameter count, so a metastore
        that cannot answer the second query must still yield the functions."""
        cursor = _make_sql_mocks(mock_connect)
        cursor.fetchall.side_effect = [self._ROUTINES]
        cursor.execute.side_effect = [None, RuntimeError("no such column")]

        out = UnityCatalog(auth_with_warehouse).list_functions("main", "ops")

        assert [f["name"] for f in out] == [
            "recompute_risk",
            "risk_history",
            "no_args",
        ]
        assert out[1]["return_columns"] == []

    @patch("databricks.sql.connect", side_effect=RuntimeError("boom"))
    def test_returns_empty_list_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.list_functions("main", "ops") == []

    def test_rejects_invalid_schema_identifier(self, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.list_functions("main", "ops; DROP TABLE x") == []


class TestGetTableColumns:
    @patch("databricks.sql.connect")
    def test_returns_list_of_dicts(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchall=[
                ["id", "bigint", "pk"],
                ["name", "string", None],
                ["x", "int", ""],
            ],
        )
        uc = UnityCatalog(auth_with_warehouse)
        cols = uc.get_table_columns("cat", "sch", "tbl")
        assert cols == [
            {"name": "id", "type": "bigint", "comment": "pk"},
            {"name": "name", "type": "string", "comment": ""},
            {"name": "x", "type": "int", "comment": ""},
        ]
        mock_cursor.execute.assert_called_once_with("DESCRIBE `cat`.`sch`.`tbl`")

    @patch(
        "databricks.sql.connect",
        side_effect=Exception("no table"),
    )
    def test_returns_empty_list_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_columns("c", "s", "t") == []


class TestGetTableComment:
    @patch("databricks.sql.connect")
    def test_returns_comment_string(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchone=["my table comment"],
        )
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_comment("cat", "sch", "tbl") == "my table comment"
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        call_sql = call_args[0][0]
        assert "information_schema.tables" in call_sql
        assert "`cat`.information_schema.tables" in call_sql
        # Databricks SQL Connector requires qmark placeholders, not pyformat %s.
        assert "%s" not in call_sql
        assert call_sql.count("?") == 3
        assert call_args[0][1] == ("cat", "sch", "tbl")

    @patch("databricks.sql.connect")
    def test_returns_empty_when_no_row(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(mock_connect)
        mock_cursor.fetchone.return_value = None
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_comment("cat", "sch", "tbl") == ""

    @patch(
        "databricks.sql.connect",
        side_effect=Exception("timeout"),
    )
    def test_returns_empty_string_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.get_table_comment("c", "s", "t") == ""


class TestGetVolumesSql:
    def test_raises_value_error_without_warehouse_id(self, auth_no_warehouse):
        uc = UnityCatalog(auth_no_warehouse)
        with pytest.raises(ValidationError, match="SQL Warehouse ID is required"):
            uc.get_volumes("main", "default")

    @patch("databricks.sql.connect")
    def test_returns_volume_names(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(
            mock_connect,
            fetchall=[["sch", "vol_a"], ["sch", "vol_b"]],
        )
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.get_volumes("main", "default")
        assert out == ["vol_a", "vol_b"]
        mock_cursor.execute.assert_called_once_with("SHOW VOLUMES IN `main`.`default`")


class TestListVolumesRest:
    @patch.object(_unity_catalog_mod.requests, "get")
    def test_returns_names_from_rest_api(self, mock_get, auth_with_warehouse):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "volumes": [
                {"name": "v1", "full_name": "main.default.v1"},
                {"name": "v2"},
            ]
        }
        mock_get.return_value = mock_resp

        uc = UnityCatalog(auth_with_warehouse)
        out = uc.list_volumes("main", "default")
        assert out == ["v1", "v2"]
        mock_get.assert_called_once()
        call_kw = mock_get.call_args
        assert "unity-catalog/volumes" in call_kw[0][0]
        assert call_kw[1]["params"] == {
            "catalog_name": "main",
            "schema_name": "default",
        }

    def test_returns_empty_when_no_auth(self, clean_databricks_env):
        auth = DatabricksAuth(
            host="https://test.cloud.databricks.com",
            token="",
            warehouse_id="wh",
        )
        uc = UnityCatalog(auth)
        assert uc.list_volumes("main", "default") == []

    def test_returns_empty_when_no_host(self, clean_databricks_env):
        auth = DatabricksAuth(
            host="https://test.cloud.databricks.com",
            token="pat",
            warehouse_id="wh",
        )
        auth.host = ""
        uc = UnityCatalog(auth)
        assert uc.list_volumes("main", "default") == []

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_returns_empty_on_http_error(self, mock_get, auth_with_warehouse):
        mock_get.side_effect = Exception("401 Unauthorized")
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.list_volumes("main", "default") == []


class TestCreateVolumeRest:
    @patch.object(_unity_catalog_mod.requests, "post")
    def test_returns_true_on_success(self, mock_post, auth_with_warehouse):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = Mock()
        mock_post.return_value = mock_resp

        uc = UnityCatalog(auth_with_warehouse)
        assert uc.create_volume("main", "default", "my_vol") is True
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert "unity-catalog/volumes" in args[0]
        assert kwargs["json"]["name"] == "my_vol"
        assert kwargs["json"]["volume_type"] == "MANAGED"

    @patch.object(_unity_catalog_mod.requests, "post")
    def test_returns_false_on_error(self, mock_post, auth_with_warehouse):
        mock_post.side_effect = Exception("conflict")
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.create_volume("main", "default", "x") is False

    def test_returns_false_when_no_auth(self, clean_databricks_env):
        auth = DatabricksAuth(
            host="https://test.cloud.databricks.com",
            token="",
            warehouse_id="wh",
        )
        uc = UnityCatalog(auth)
        assert uc.create_volume("main", "default", "v") is False


class TestGetEffectiveSchemaPermissions:
    @patch.object(_unity_catalog_mod.requests, "get")
    def test_effective_schema_calls_endpoint_with_exact_principal_param(
        self, mock_get, auth_with_warehouse
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {"privilege_assignments": []}
        mock_get.return_value = mock_resp

        out = UnityCatalog(auth_with_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )

        assert out == {"accessible": True, "assignments": [], "error": None}
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert args[0].endswith(
            "/api/2.1/unity-catalog/effective-permissions/SCHEMA/main.graph"
        )
        assert kwargs["params"] == {"principal": "app-client-id"}
        assert kwargs["timeout"] == 10

    @patch.object(_unity_catalog_mod.requests, "get")
    @patch.object(_unity_catalog_mod, "validate_uc_identifier")
    def test_effective_schema_validates_and_url_encodes_path_segments(
        self, mock_validate_identifier, mock_get, auth_with_warehouse
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'{"privilege_assignments":[]}'
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {"privilege_assignments": []}
        mock_get.return_value = mock_resp
        mock_validate_identifier.side_effect = ["main", "graph/ops?takeover=1"]

        UnityCatalog(auth_with_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )

        args, kwargs = mock_get.call_args
        assert args[0].endswith(
            "/api/2.1/unity-catalog/effective-permissions/SCHEMA/main.graph%2Fops%3Ftakeover%3D1"
        )
        assert kwargs["params"] == {"principal": "app-client-id"}
        assert mock_validate_identifier.call_args_list[0].kwargs == {"role": "catalog"}
        assert mock_validate_identifier.call_args_list[1].kwargs == {"role": "schema"}

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_effective_schema_normalizes_privileges_and_filters_by_principal(
        self, mock_get, auth_with_warehouse
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "privilege_assignments": [
                {
                    "principal": "some-group",
                    "privileges": [
                        {
                            "privilege": "USE_SCHEMA",
                            "inherited_from_name": "main.graph",
                        }
                    ],
                },
                {
                    "principal": "app-client-id",
                    "privileges": [
                        {"privilege": "USE_CATALOG", "inherited_from_name": "main"},
                        {"privilege": "SELECT"},
                        "ALL_PRIVILEGES",
                    ],
                },
            ]
        }
        mock_get.return_value = mock_resp

        out = UnityCatalog(auth_with_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )

        assert out == {
            "accessible": True,
            "assignments": [
                {"privilege": "USE CATALOG", "inherited_from": "main"},
                {"privilege": "SELECT", "inherited_from": ""},
                {"privilege": "ALL PRIVILEGES", "inherited_from": ""},
            ],
            "error": None,
        }

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_effective_schema_keeps_principal_less_assignments(
        self, mock_get, auth_with_warehouse
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'{"privilege_assignments":[]}'
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {
            "privilege_assignments": [
                {"privileges": [{"privilege": "USE_SCHEMA"}]},
                {"principal": "other-principal", "privileges": [{"privilege": "SELECT"}]},
            ]
        }
        mock_get.return_value = mock_resp

        out = UnityCatalog(auth_with_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )
        assert out == {
            "accessible": True,
            "assignments": [{"privilege": "USE SCHEMA", "inherited_from": ""}],
            "error": None,
        }

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_effective_schema_does_not_require_warehouse(
        self, mock_get, auth_no_warehouse
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'{"privilege_assignments":[]}'
        mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = {"privilege_assignments": []}
        mock_get.return_value = mock_resp

        out = UnityCatalog(auth_no_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )
        assert out == {"accessible": True, "assignments": [], "error": None}

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_effective_schema_handles_empty_200_content(self, mock_get, auth_with_warehouse):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b""
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        out = UnityCatalog(auth_with_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )
        assert out == {"accessible": True, "assignments": [], "error": None}
        mock_resp.json.assert_not_called()

    @patch.object(_unity_catalog_mod.requests, "get")
    @pytest.mark.parametrize(
        "status_code,expected_error",
        [
            (404, "Schema not found in Unity Catalog"),
            (
                403,
                "Insufficient privileges to inspect effective schema permissions",
            ),
        ],
    )
    def test_effective_schema_returns_diagnostic_on_404_or_403(
        self, mock_get, auth_with_warehouse, status_code, expected_error
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_get.return_value = mock_resp

        out = UnityCatalog(auth_with_warehouse).get_effective_schema_permissions(
            "main", "graph", "app-client-id"
        )

        assert out == {"accessible": False, "assignments": [], "error": expected_error}

    @patch.object(_unity_catalog_mod.requests, "get")
    def test_effective_schema_raises_on_non_diagnostic_request_error(
        self, mock_get, auth_with_warehouse
    ):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.HTTPError("boom")
        mock_get.return_value = mock_resp

        uc = UnityCatalog(auth_with_warehouse)
        with pytest.raises(requests.HTTPError, match="boom"):
            uc.get_effective_schema_permissions("main", "graph", "app-client-id")


class TestUnityCatalogSqlInjectionGuards:
    @pytest.mark.parametrize(
        "method,args",
        [
            ("get_schemas", ("main; DROP TABLE x--",)),
            ("get_tables", ("cat", "sch'; DROP--")),
            ("get_table_columns", ("cat", "sch", "tbl;drop")),
            ("get_table_comment", ("cat", "sch", "tbl;")),
            ("get_volumes", ("main", "default;")),
            ("probe_schema_has_tables", ("cat", "sch;")),
            ("check_table_select_permission", ("cat", "sch", "tbl;")),
            ("get_effective_schema_permissions", ("cat", "sch;", "spn")),
        ],
    )
    def test_rejects_invalid_identifiers(self, auth_with_warehouse, method, args):
        uc = UnityCatalog(auth_with_warehouse)
        with pytest.raises(ValidationError, match="Invalid UC"):
            getattr(uc, method)(*args)


class TestProbeSchemaHasTables:
    @patch("databricks.sql.connect")
    def test_returns_count_with_parameterized_query(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(mock_connect, fetchone=[3])
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.probe_schema_has_tables("my_cat", "my_sch") == 3
        mock_cursor.execute.assert_called_once()
        call_sql, params = mock_cursor.execute.call_args[0]
        assert "`my_cat`.information_schema.tables" in call_sql
        assert "table_schema = ?" in call_sql
        assert "%s" not in call_sql
        assert params == ("my_sch",)

    @patch("databricks.sql.connect", side_effect=RuntimeError("denied"))
    def test_returns_minus_one_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        assert uc.probe_schema_has_tables("cat", "sch") == -1


class TestCheckTableSelectPermission:
    @patch("databricks.sql.connect")
    def test_uses_quoted_fqn(self, mock_connect, auth_with_warehouse):
        mock_cursor = _make_sql_mocks(mock_connect)
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.check_table_select_permission("cat", "sch", "tbl")
        assert out == {"can_select": True, "error": None}
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM `cat`.`sch`.`tbl` LIMIT 0"
        )

    @patch("databricks.sql.connect", side_effect=RuntimeError("no select"))
    def test_returns_false_on_error(self, _mock_connect, auth_with_warehouse):
        uc = UnityCatalog(auth_with_warehouse)
        out = uc.check_table_select_permission("cat", "sch", "tbl")
        assert out["can_select"] is False
        assert "no select" in out["error"]
