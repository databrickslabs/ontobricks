"""Regression: Mapping-PGE session extras stay bounded across repeated runs."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from back.objects.mapping.Mapping import Mapping, _MAX_MAPPING_RUN_LOG_ENTRIES


@pytest.mark.unit
def test_mapping_run_log_is_capped_on_save(tmp_path: Path):
    session_id = "0123456789abcdef0123456789abcdef"
    session_path = tmp_path / session_id
    seed_log = [{"i": i} for i in range(_MAX_MAPPING_RUN_LOG_ENTRIES - 10)]
    session_path.write_text(
        json.dumps(
            {
                "domain_data": {
                    "assignment": {
                        "entities": [],
                        "relationships": [],
                        "mapping_run_log": seed_log,
                    },
                    "domain": {},
                }
            }
        )
    )

    settings = MagicMock()
    settings.session_dir = str(tmp_path)

    with patch("back.objects.mapping.Mapping.get_settings", return_value=settings):
        Mapping.save_mappings_to_session(
            session_id,
            None,
            [],
            [],
            mapping_run_log=[{"i": i} for i in range(50)],
        )

    data = json.loads(session_path.read_text())
    run_log = data["domain_data"]["assignment"]["mapping_run_log"]
    assert len(run_log) == _MAX_MAPPING_RUN_LOG_ENTRIES
    # Newest entries retained.
    assert run_log[-1]["i"] == 49


@pytest.mark.unit
def test_save_mappings_to_session_rejects_traversal_id(tmp_path: Path):
    """A traversal-shaped session id must not touch the filesystem.

    This encodes the path-sink defence in Mapping.save_mappings_to_session:
    ``is_valid_session_id`` guards the call before any filesystem operation
    is performed, so a hostile id cannot overwrite files outside session_dir.
    """
    traversal_id = "../evil"
    evil_path = tmp_path.parent / "evil"

    settings = MagicMock()
    settings.session_dir = str(tmp_path)

    with patch("back.objects.mapping.Mapping.get_settings", return_value=settings):
        Mapping.save_mappings_to_session(
            traversal_id,
            None,
            [{"entity": "X"}],
            [],
        )

    assert not evil_path.exists(), "traversal id must not create files outside session_dir"
    assert list(tmp_path.iterdir()) == [], "no file must be created inside session_dir either"
