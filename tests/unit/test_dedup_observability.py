"""Semantic dedup is on by default and its outcomes are observable."""

from __future__ import annotations

from vera.config.settings import MemorySettings
from vera.observability.metrics import record_entity_resolution, render_latest


def test_semantic_dedup_is_enabled_by_default() -> None:
    assert MemorySettings().semantic_dedup_enabled is True


def test_entity_resolution_outcomes_are_counted() -> None:
    record_entity_resolution("linked_judge")
    payload = render_latest()[0]
    body = payload.decode() if isinstance(payload, bytes) else str(payload)
    assert "vera_entity_resolution_total" in body
    assert 'outcome="linked_judge"' in body
