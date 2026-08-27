"""Identifier helpers."""

from __future__ import annotations

import pytest

from vera.shared.ids import deterministic_id, uuid7


def test_deterministic_id_is_stable_for_same_input() -> None:
    assert deterministic_id("confluence:page:1:v1") == deterministic_id("confluence:page:1:v1")


def test_deterministic_id_differs_for_different_input() -> None:
    assert deterministic_id("source-a") != deterministic_id("source-b")


def test_deterministic_id_parts_do_not_collide() -> None:
    assert deterministic_id("a", "bc") != deterministic_id("ab", "c")


def test_deterministic_id_requires_parts() -> None:
    with pytest.raises(ValueError):
        deterministic_id()


def test_uuid7_reports_version_7() -> None:
    assert uuid7().version == 7
