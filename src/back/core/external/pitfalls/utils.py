"""Utility functions for ontology pitfall detection.

Vendored from https://github.com/D2KLab/Ontology-Pitfalls-Detector (Apache-2.0).
"""
from __future__ import annotations

from functools import reduce
from operator import concat
from typing import Callable, Iterable, List, Sequence, TypeVar

T = TypeVar("T")


def camel_case_split(text: str) -> List[str]:
    if not text:
        return []

    words = [[text[0]]]
    for char in text[1:]:
        if words[-1][-1].islower() and char.isupper():
            words.append([char])
        else:
            words[-1].append(char)

    return ["".join(word) for word in words]


def flatten(values: Iterable[Iterable[T]]) -> List[T]:
    values_list = [list(v) for v in values]
    if not values_list:
        return []
    return reduce(concat, values_list)


def extract_label(uri: object, clean: bool = False) -> str:
    label = str(uri).split("#")[-1]
    if clean:
        return " ".join(camel_case_split(label))
    return label


def normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in str(name) if ch.isalnum())


def ensure_nltk_resource(resource_path: str, download_name: str) -> None:
    import nltk  # optional dep — only needed for semantic checks

    try:
        nltk.data.find(resource_path)
    except LookupError:
        nltk.download(download_name, quiet=True)


def normalize_pattern_id(raw_id: str) -> str:
    token = str(raw_id).strip().upper().rstrip(".")
    if not token:
        raise ValueError("Pattern identifier cannot be empty.")

    if token == "ALL":
        return token

    if token.startswith("P"):
        token = token[1:]

    if not token.isdigit():
        raise ValueError(f"Invalid pattern identifier: {raw_id}")

    return f"P{int(token)}"


def _pattern_sort_key(pattern_id: str) -> tuple:
    token = str(pattern_id).strip().upper().rstrip(".")
    if token.startswith("P"):
        token = token[1:]

    parts = token.split(".")
    if any(not part.isdigit() for part in parts):
        raise ValueError(f"Invalid pattern identifier: {pattern_id}")

    return tuple(int(part) for part in parts)


def sort_pattern_ids(pattern_ids: Sequence[str]) -> List[str]:
    return sorted(pattern_ids, key=_pattern_sort_key)


def parse_pattern_selection(
    patterns: Sequence[str] | None,
    available_patterns: Sequence[str],
    normalizer: Callable[[str], str] = normalize_pattern_id,
) -> List[str]:
    available_normalized = [normalizer(pattern) for pattern in available_patterns]
    available_set = set(available_normalized)

    if not patterns:
        return sort_pattern_ids(available_normalized)

    raw_tokens: List[str] = []
    for pattern in patterns:
        raw_tokens.extend(token.strip() for token in str(pattern).split(",") if token.strip())

    if not raw_tokens:
        return sort_pattern_ids(available_normalized)

    normalized = [normalizer(token) for token in raw_tokens]
    if "ALL" in normalized:
        return sort_pattern_ids(available_normalized)

    selected: List[str] = []
    for pattern_id in normalized:
        if pattern_id not in available_set:
            options = ", ".join(sort_pattern_ids(available_normalized))
            raise ValueError(f"Unknown pattern '{pattern_id}'. Available: {options}")

        if pattern_id not in selected:
            selected.append(pattern_id)

    return selected
