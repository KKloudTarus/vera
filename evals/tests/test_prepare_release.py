from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from evals.prepare_release import ReleasePreparationError, prepare_release_config
from vera import __version__

IMAGE_DIGEST = "sha256:" + "b" * 64


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def _template() -> dict[str, Any]:
    return {
        "profile": "release",
        "git_sha": "0" * 40,
        "git_dirty": False,
        "run_context": {"evaluation_scope": {"id": "release-template"}},
    }


def test_prepare_release_config_binds_image_revision_and_scope(tmp_path: Path) -> None:
    template = tmp_path / "template.json"
    metadata = tmp_path / "metadata.json"
    output = tmp_path / "generated" / "release.json"
    _write_json(template, _template())
    _write_json(metadata, {"git_sha": "a" * 40, "git_dirty": False})

    prepare_release_config(
        template,
        output,
        metadata_path=metadata,
        scope_id="release-a-20260902",
        app_image_digest=IMAGE_DIGEST,
    )

    generated = cast(dict[str, Any], json.loads(output.read_text(encoding="utf-8")))
    assert generated["git_sha"] == "a" * 40
    assert generated["git_dirty"] is False
    assert generated["service_version"] == __version__
    assert generated["app_image_digest"] == IMAGE_DIGEST
    run_context = cast(dict[str, Any], generated["run_context"])
    scope = cast(dict[str, Any], run_context["evaluation_scope"])
    assert scope["id"] == "release-a-20260902"


def test_prepare_release_config_rejects_dirty_image(tmp_path: Path) -> None:
    template = tmp_path / "template.json"
    metadata = tmp_path / "metadata.json"
    _write_json(template, _template())
    _write_json(metadata, {"git_sha": "a" * 40, "git_dirty": True})

    with pytest.raises(ReleasePreparationError, match="clean source tree"):
        prepare_release_config(
            template,
            tmp_path / "release.json",
            metadata_path=metadata,
            scope_id="release-a",
            app_image_digest=IMAGE_DIGEST,
        )


def test_prepare_release_config_does_not_overwrite_artifact(tmp_path: Path) -> None:
    template = tmp_path / "template.json"
    metadata = tmp_path / "metadata.json"
    output = tmp_path / "release.json"
    _write_json(template, _template())
    _write_json(metadata, {"git_sha": "a" * 40, "git_dirty": False})
    output.write_text("preserve", encoding="utf-8")

    with pytest.raises(ReleasePreparationError, match="already exists"):
        prepare_release_config(
            template,
            output,
            metadata_path=metadata,
            scope_id="release-a",
            app_image_digest=IMAGE_DIGEST,
        )

    assert output.read_text(encoding="utf-8") == "preserve"


def test_prepare_release_config_rejects_placeholder_image_digest(tmp_path: Path) -> None:
    template = tmp_path / "template.json"
    metadata = tmp_path / "metadata.json"
    _write_json(template, _template())
    _write_json(metadata, {"git_sha": "a" * 40, "git_dirty": False})

    with pytest.raises(ReleasePreparationError, match="missing or invalid"):
        prepare_release_config(
            template,
            tmp_path / "release.json",
            metadata_path=metadata,
            scope_id="release-a",
            app_image_digest="sha256:" + "0" * 64,
        )
