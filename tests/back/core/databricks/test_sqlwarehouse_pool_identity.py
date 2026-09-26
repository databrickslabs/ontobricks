from back.core.databricks.SQLWarehouse import SQLWarehouse
from back.core.databricks.DatabricksAuth import DatabricksAuth


def _auth(token):
    a = DatabricksAuth.__new__(DatabricksAuth)
    a.token = token
    a.client_id = ""
    a.client_secret = ""
    a.is_app_mode = False
    a._cli_config = None
    return a


def test_identity_key_differs_per_token():
    w1 = SQLWarehouse(_auth("tokenA"))
    w2 = SQLWarehouse(_auth("tokenB"))
    assert w1._identity_key() != w2._identity_key()


def test_identity_key_stable_for_same_token():
    w1 = SQLWarehouse(_auth("tokenA"))
    w2 = SQLWarehouse(_auth("tokenA"))
    assert w1._identity_key() == w2._identity_key()


def test_empty_token_is_service_principal_bucket():
    w = SQLWarehouse(_auth(""))
    assert w._identity_key() == "sp"


def test_separate_pools_per_identity():
    w = SQLWarehouse(_auth("tokenA"))
    pool_a = w._pool_for_identity()
    # Same instance, same token -> same pool object reused.
    assert w._pool_for_identity() is pool_a
