"""build_sync_registrations turns connector specs into registrations, skipping bad ones."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from vera.bootstrap import Container
from vera.entrypoints.worker.main import build_sync_registrations
from vera.shared.ids import uuid7


def _container(specs: list[dict[str, Any]]) -> Container:
    settings = SimpleNamespace(connectors=SimpleNamespace(specs=specs))
    return cast("Container", SimpleNamespace(settings=settings))


def test_good_specs_register_and_bad_ones_are_skipped() -> None:
    good = {
        "kind": "filesystem",
        "root": "/srv/docs",
        "source_id": str(uuid7()),
        "group_id": "p:demo",
        "interval_s": 600,
    }
    missing_source = {"kind": "filesystem", "root": "/srv/x", "group_id": "p:demo"}
    missing_secret = {
        "kind": "jira",
        "base_url": "https://j",
        "project_key": "ENG",
        "token_env": "VERA_DEFINITELY_UNSET_TOKEN",
        "source_id": str(uuid7()),
        "group_id": "p:demo",
    }
    unknown_kind = {"kind": "nope", "source_id": str(uuid7()), "group_id": "p:demo"}

    registrations = build_sync_registrations(
        _container([good, missing_source, missing_secret, unknown_kind])
    )
    assert len(registrations) == 1  # only the well-formed filesystem spec survives
    assert registrations[0].group_id == "p:demo"
    assert registrations[0].interval_s == 600.0
    assert registrations[0].connector.kind == "filesystem"


def test_no_specs_yields_no_registrations() -> None:
    assert build_sync_registrations(_container([])) == []
