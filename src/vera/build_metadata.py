"""Read immutable source provenance baked into a VERA image."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

_GIT_SHA = re.compile(r"[0-9a-f]{40}")


class BuildMetadataError(ValueError):
    """Image build metadata is missing or malformed."""


@dataclass(frozen=True, slots=True)
class BuildMetadata:
    git_sha: str
    git_dirty: bool

    def as_dict(self) -> dict[str, str | bool]:
        return {"git_sha": self.git_sha, "git_dirty": self.git_dirty}


def parse_build_metadata(value: Any) -> BuildMetadata:
    if not isinstance(value, dict):
        raise BuildMetadataError("build metadata must be an object")
    metadata = cast(dict[str, Any], value)
    git_sha = metadata.get("git_sha")
    git_dirty = metadata.get("git_dirty")
    if not isinstance(git_sha, str) or _GIT_SHA.fullmatch(git_sha) is None:
        raise BuildMetadataError("build metadata git_sha must be a 40-character lowercase SHA")
    if not isinstance(git_dirty, bool):
        raise BuildMetadataError("build metadata git_dirty must be a boolean")
    return BuildMetadata(git_sha=git_sha, git_dirty=git_dirty)


def load_build_metadata(path: Path) -> BuildMetadata:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BuildMetadataError("build metadata is unavailable") from exc
    return parse_build_metadata(value)
