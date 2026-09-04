from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any, cast

import yaml

from vera import __version__
from vera.entrypoints.api.main import create_app


def test_package_chart_and_default_image_versions_match() -> None:
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    chart = cast(
        dict[str, Any],
        yaml.safe_load((root / "deploy/helm/vera/Chart.yaml").read_text(encoding="utf-8")),
    )
    values = cast(
        dict[str, Any],
        yaml.safe_load((root / "deploy/helm/vera/values.yaml").read_text(encoding="utf-8")),
    )

    assert project["project"]["version"] == __version__
    assert chart["version"] == __version__
    assert chart["appVersion"] == __version__
    assert "tag" not in values["image"]
    assert values["image"]["digest"].startswith("sha256:")
    for name in (
        "run.local.json",
        "run.nightly.local.json",
        "run.release.local.json",
        "run.weekly.local.json",
    ):
        config = cast(
            dict[str, Any],
            json.loads((root / "evals" / name).read_text(encoding="utf-8")),
        )
        assert config["service_version"] == __version__
    assert create_app().version == __version__
