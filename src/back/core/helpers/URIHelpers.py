import re


class URIHelpers:
    @staticmethod
    def is_uri(value: str) -> bool:
        """Return ``True`` if *value* looks like an absolute HTTP(S) URI."""
        return bool(value) and (value.startswith("http://") or value.startswith("https://"))

    @staticmethod
    def extract_local_name(uri: str) -> str:
        """Extract the local name from a URI (the part after ``#`` or the last ``/``).

        This is the canonical implementation used across the codebase.
        """
        if not uri:
            return ""
        s = str(uri)
        if "#" in s:
            return s.rsplit("#", 1)[-1]
        return s.rsplit("/", 1)[-1]

    @staticmethod
    def safe_identifier(name: str, prefix: str = "") -> str:
        """Convert *name* into a valid Python / SQL / GraphQL identifier.

        Replaces non-alphanumeric characters with underscores. If the result
        starts with a digit, *prefix* (default ``"_"``) is prepended.
        """
        safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        if safe and safe[0].isdigit():
            safe = f"{prefix or '_'}{safe}"
        return safe or "_unnamed"
