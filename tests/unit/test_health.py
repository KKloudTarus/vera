"""Liveness endpoint. Runs the app lifespan without needing a real database."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vera import __version__
from vera.entrypoints.api.main import app
from vera.entrypoints.api.routers import health


def test_live_returns_ok() -> None:
    with TestClient(app) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_build_returns_baked_provenance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    metadata = tmp_path / "build-metadata.json"
    metadata.write_text(json.dumps({"git_sha": "a" * 40, "git_dirty": False}), encoding="utf-8")
    monkeypatch.setattr(health, "BUILD_METADATA_PATH", metadata)

    with TestClient(app) as client:
        response = client.get("/health/build")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service_version": __version__,
        "git_sha": "a" * 40,
        "git_dirty": False,
    }


def test_build_fails_closed_without_baked_provenance(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(health, "BUILD_METADATA_PATH", tmp_path / "missing.json")

    with TestClient(app) as client:
        response = client.get("/health/build")

    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"
