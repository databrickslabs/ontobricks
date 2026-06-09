"""Persona-based UAT suite for OntoBricks.

A job-function persona layer on top of the existing feature-organized
``tests/e2e`` Playwright suite. Each persona is an archetype mapped onto one
of the four RBAC roles (admin > builder > editor > viewer) enforced by
``PermissionMiddleware``. Offline runs drive personas through the prod-safe
test-auth seam (``ONTOBRICKS_TEST_AUTH=1``); a curated live-smoke subset runs
the real Databricks journeys against a deployed app.

See ``README.md`` in this package for the persona model, coverage matrix, and
how to run the suite offline vs live.
"""
