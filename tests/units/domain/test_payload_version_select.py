"""Loaded domain version must pin graph/object slice selection."""

from back.objects.domain.payload import select_loaded_version


class TestSelectLoadedVersion:
    def test_integer_request_matches_string_keys(self):
        """JSON ``version: 1`` must not fall through to lexicographic latest."""
        versions = {"1": {}, "2": {}}
        assert select_loaded_version(versions, requested=1) == "1"

    def test_string_request_is_honoured(self):
        assert select_loaded_version({"1": {}, "2": {}}, requested="1") == "1"

    def test_unspecified_picks_numeric_latest_not_lexicographic(self):
        assert select_loaded_version({"1": {}, "10": {}, "2": {}}) == "10"

    def test_requested_version_missing_from_document_is_still_pinned(self):
        """FQNs must stay on the loaded version even if that slice is absent."""
        assert select_loaded_version({"2": {}}, requested="1") == "1"
