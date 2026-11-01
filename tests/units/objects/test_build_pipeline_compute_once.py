"""The build reads the mapped triples once and resets the inferred companion.

``_BuildPipeline`` keeps one-line hooks; the behaviour lives in
``TwinBuildPostMaterialize``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from back.objects.digitaltwin.build import TwinBuildPostMaterialize
from tests.units.objects.test_build_pipeline_materialize import _bare_pipeline

pytestmark = pytest.mark.unit


class TestPersistLastBuildHook:
    def test_the_pipeline_delegates_to_the_collaborator(self):
        pipe = _bare_pipeline()

        with patch.object(
            TwinBuildPostMaterialize, "persist_last_build_to_registry"
        ) as persist:
            pipe._persist_last_build_to_registry()

        persist.assert_called_once_with(pipe)
