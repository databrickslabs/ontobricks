from back.core.databricks.request_identity import (
    RequestIdentity,
    get_request_identity,
    set_request_identity,
    reset_request_identity,
)


def test_default_identity_is_empty():
    ident = get_request_identity()
    assert ident.email == ""
    assert ident.user_token == ""
    assert ident.domain_role == ""


def test_set_and_reset_identity():
    token = set_request_identity(
        RequestIdentity(
            email="a@b.c",
            user_token="tok",
            app_role="app_user",
            domain_role="viewer",
        )
    )
    try:
        ident = get_request_identity()
        assert ident.email == "a@b.c"
        assert ident.user_token == "tok"
        assert ident.domain_role == "viewer"
    finally:
        reset_request_identity(token)
    assert get_request_identity().user_token == ""
