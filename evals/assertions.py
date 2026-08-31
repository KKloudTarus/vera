"""Pure assertion operator semantics shared by evaluation and report validation."""

from __future__ import annotations

from typing import Any


def _collection_contains(observed: Any, expected: Any) -> bool:
    if isinstance(observed, str) and isinstance(expected, str):
        return expected.casefold() in observed.casefold()
    if isinstance(observed, (list, tuple, set)):
        return any(item == expected or _collection_contains(item, expected) for item in observed)
    if isinstance(observed, dict):
        return any(
            value == expected or _collection_contains(value, expected)
            for value in observed.values()
        )
    return False


def assertion_passes(
    operator: str,
    target: str,
    observed: Any,
    expected: Any,
    *,
    observed_present: bool,
    expected_present: bool,
) -> bool:
    try:
        if operator == "absent":
            return not observed_present or observed is None
        if operator == "exists":
            return observed_present and observed is not None and expected is True
        if not observed_present or not expected_present:
            return False
        if operator == "eq":
            return observed == expected
        if operator == "neq":
            return observed != expected
        if operator == "contains":
            return _collection_contains(observed, expected)
        if operator == "not_contains":
            return not _collection_contains(observed, expected)
        if operator == "all":
            return isinstance(expected, list) and all(
                _collection_contains(observed, item) for item in expected
            )
        if operator == "none":
            return isinstance(expected, list) and all(
                not _collection_contains(observed, item) for item in expected
            )
        if operator in {"gte", "lte", "lt", "gt"}:
            if (
                isinstance(observed, bool)
                or isinstance(expected, bool)
                or not isinstance(observed, (int, float))
                or not isinstance(expected, (int, float))
            ):
                return False
            if operator == "gte":
                return observed >= expected
            if operator == "lte":
                return observed <= expected
            if operator == "lt":
                return observed < expected
            return observed > expected
        if operator == "unchanged":
            if isinstance(observed, bool):
                return observed is True and expected is True
            if isinstance(observed, dict) and {"before", "after"} <= set(observed):
                return observed["before"] == observed["after"] and expected is True
            return False
        if operator == "equivalent":
            return observed is True if expected is True else observed == expected
    except (TypeError, ValueError):
        return False
    return False
