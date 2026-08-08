"""Unit tests for domain name format helpers."""

import pytest

from back.objects.session import is_valid_domain_name, sanitize_domain_folder


@pytest.mark.parametrize(
    "name,expected",
    [
        ("PatientCare", True),
        ("SupplyChain", True),
        ("Acme360", True),
        ("A", True),
        ("NewDomain", True),
        ("", False),
        ("acme", False),
        ("My Domain", False),
        ("WRFM - Shell", False),
        ("Patient_Care", False),
        ("Patient-Care", False),
        ("Patient.Care", False),
        ("123Acme", False),
        ("Acme Sales!", False),
    ],
)
def test_is_valid_domain_name(name, expected):
    assert is_valid_domain_name(name) is expected


def test_sanitize_domain_folder_still_handles_legacy_names():
    assert sanitize_domain_folder("WRFM - Shell") == "wrfm___shell"
