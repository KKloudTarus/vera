"""Prepare an immutable release run config from evaluator image metadata."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, cast

from vera import __version__
from vera.build_metadata import BuildMetadataError, load_build_metadata

_DEFAULT_BUILD_METADATA_PATH = Path("/workspace/build-metadata.json")
_SAFE_SCOPE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


class ReleasePreparationError(ValueError):
    """A release config cannot be bound to the running evaluator image."""


def prepare_release_config(
    template_path: Path,
    output_path: Path,
    *,
    metadata_path: Path = _DEFAULT_BUILD_METADATA_PATH,
    scope_id: str | None = None,
) -> Path:
    metadata = load_build_metadata(metadata_path)
    if metadata.git_dirty:
        raise ReleasePreparationError("release images must be built from a clean source tree")

    selected_scope = scope_id or os.environ.get("VERA_EVAL_SCOPE_ID")
    if selected_scope is None or _SAFE_SCOPE_ID.fullmatch(selected_scope) is None:
        raise ReleasePreparationError("VERA_EVAL_SCOPE_ID is missing or unsafe")

    try:
        raw = json.loads(template_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleasePreparationError("release config template is unavailable or invalid") from exc
    if not isinstance(raw, dict):
        raise ReleasePreparationError("release config template must be an object")
    config = cast(dict[str, Any], raw)
    if config.get("profile") != "release":
        raise ReleasePreparationError("release config template must select the release profile")

    run_context = config.get("run_context")
    if not isinstance(run_context, dict):
        raise ReleasePreparationError("release config template must define run_context")
    typed_run_context = cast(dict[str, Any], run_context)
    evaluation_scope = typed_run_context.get("evaluation_scope")
    if not isinstance(evaluation_scope, dict):
        raise ReleasePreparationError("release config template must define evaluation_scope")

    config["git_sha"] = metadata.git_sha
    config["git_dirty"] = metadata.git_dirty
    config["service_version"] = __version__
    cast(dict[str, Any], evaluation_scope)["id"] = selected_scope
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output_path.open("x", encoding="utf-8") as output:
            json.dump(config, output, ensure_ascii=True, indent=2)
            output.write("\n")
    except FileExistsError as exc:
        raise ReleasePreparationError(f"release config already exists: {output_path}") from exc
    return output_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("template", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--metadata", type=Path, default=_DEFAULT_BUILD_METADATA_PATH)
    parser.add_argument("--scope-id")
    return parser


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    try:
        output = prepare_release_config(
            args.template,
            args.output,
            metadata_path=args.metadata,
            scope_id=args.scope_id,
        )
    except (BuildMetadataError, ReleasePreparationError) as exc:
        parser.error(str(exc))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
