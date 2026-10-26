"""Raised for any hard failure that aborts the Lakebase graph provisioning flow."""

from __future__ import annotations


class ProvisionError(Exception):
    """Raised for any hard failure that should abort the provision flow."""
