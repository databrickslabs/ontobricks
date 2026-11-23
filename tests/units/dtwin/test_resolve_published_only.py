"""External reads resolve only PUBLISHED versions (D4 of the certified-graph SPEC)."""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.objects.digitaltwin import DigitalTwin

pytestmark = pytest.mark.unit


def _resolve(svc, *, domain=None, **kwargs):
    domain = domain if domain is not None else MagicMock(domain_folder="")
    with ExitStack() as stack:
        get_domain = stack.enter_context(
            patch("back.objects.digitaltwin.DigitalTwin.get_domain")
        )
        get_domain.return_value = domain
        stack.enter_context(
            patch.object(
                DigitalTwin,
                "resolve_registry",
                return_value={"catalog": "c", "schema": "s", "volume": "v"},
            )
        )
        stack.enter_context(
            patch.object(DigitalTwin, "uc_from_domain", return_value=MagicMock())
        )
        stack.enter_context(
            patch("back.objects.registry.RegistryService", return_value=svc)
        )
        name = kwargs.pop("domain_name", "mydom")
        return DigitalTwin.resolve_domain(
            name,
            MagicMock(),
            MagicMock(),
            read_only=True,
            published_only=True,
            **kwargs,
        )


def test_the_latest_published_version_is_served():
    svc = MagicMock()
    svc.load_published_domain_data_cached.return_value = (True, {"info": {}}, "3", "")

    domain = _resolve(svc)

    svc.load_published_domain_data_cached.assert_called_once_with("mydom")
    svc.load_latest_domain_data.assert_not_called()
    domain.import_from_file.assert_called_once_with({"info": {}}, version="3")


def test_a_context_without_published_version_is_not_found():
    svc = MagicMock()
    svc.load_published_domain_data_cached.return_value = (
        False,
        {},
        "",
        'No PUBLISHED version available for context "mydom"',
    )

    with pytest.raises(NotFoundError, match="PUBLISHED"):
        _resolve(svc)


def test_an_explicit_published_version_is_served():
    svc = MagicMock()
    svc.read_version.return_value = (True, {"info": {"status": "PUBLISHED"}}, "")

    domain = _resolve(svc, domain_version="2")

    svc.read_version.assert_called_once_with("mydom", "2")
    assert domain.import_from_file.call_args.kwargs["version"] == "2"


@pytest.mark.parametrize("status", ["DRAFT", "IN-REVIEW", None])
def test_an_explicit_unpublished_version_is_not_found(status):
    svc = MagicMock()
    svc.read_version.return_value = (True, {"info": {"status": status}}, "")

    with pytest.raises(NotFoundError, match="not published"):
        _resolve(svc, domain_version="2")


def test_without_a_name_the_session_context_is_resolved_to_its_published_version():
    svc = MagicMock()
    svc.load_published_domain_data_cached.return_value = (True, {"info": {}}, "3", "")
    session = MagicMock(domain_folder="sessiondom")

    _resolve(svc, domain=session, domain_name=None)

    svc.load_published_domain_data_cached.assert_called_once_with("sessiondom")


def test_without_a_name_and_no_session_context_a_name_is_required():
    with pytest.raises(ValidationError, match="domain_name"):
        _resolve(MagicMock(), domain_name=None)
