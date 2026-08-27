"""note_backpressure flags and counts a pending backlog over the threshold."""

from __future__ import annotations

from vera.observability.metrics import note_backpressure, render_latest


def test_over_threshold_is_flagged() -> None:
    assert note_backpressure({"pending": 1500}, 1000) is True


def test_at_or_under_threshold_is_not_flagged() -> None:
    assert note_backpressure({"pending": 1000}, 1000) is False
    assert note_backpressure({"pending": 0}, 1000) is False


def test_zero_threshold_disables_the_check() -> None:
    assert note_backpressure({"pending": 10_000}, 0) is False


def test_crossings_are_counted_in_the_metric() -> None:
    note_backpressure({"pending": 2000}, 1000)
    payload = render_latest()[0]
    body = payload.decode() if isinstance(payload, bytes) else str(payload)
    assert "vera_queue_backpressure_events_total" in body
