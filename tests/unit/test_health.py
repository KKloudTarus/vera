"""Liveness endpoint. Runs the app lifespan without needing a real database."""

from __future__ import annotations

from fastapi.testclient import TestClient

from vera.entrypoints.api.main import app


def test_live_returns_ok() -> None:
    with TestClient(app) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
