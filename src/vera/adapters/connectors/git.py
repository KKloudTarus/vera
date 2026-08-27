"""Git connector: commits in a repository become text artifacts.

Incremental via the last-seen commit sha in the cursor (``<last_sha>..HEAD``). The
external id is the stable ``git:<repo>:<sha>``, so a commit is ingested once. The git
invocation is injectable, so the mapping is testable without a real repository.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

from vera.adapters.connectors.base import parse_iso
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict

GitRunner = Callable[[list[str]], Awaitable[str]]

_UNIT = "\x1f"  # ASCII unit separator: safe field delimiter in git pretty-format
_FORMAT = f"%H{_UNIT}%an{_UNIT}%aI{_UNIT}%s"


async def _run_git(args: list[str]) -> str:
    process = await asyncio.create_subprocess_exec(
        "git", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {stderr.decode().strip()}")
    return stdout.decode()


class GitConnector:
    def __init__(self, repo_path: str, *, runner: GitRunner | None = None) -> None:
        self._repo_path = repo_path
        self._repo_name = Path(repo_path).name
        self._runner = runner or _run_git

    @property
    def kind(self) -> str:
        return "git"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        last_sha = str(cursor["last_sha"]) if cursor and cursor.get("last_sha") else None
        args = ["-C", self._repo_path, "log", "--no-merges", f"--pretty=format:{_FORMAT}"]
        if last_sha:
            args.append(f"{last_sha}..HEAD")
        output = await self._runner(args)

        records: list[ConnectorRecord] = []
        newest = last_sha
        for line in output.splitlines():
            if not line.strip():
                continue
            sha, author, when, subject = line.split(_UNIT, 3)
            records.append(
                ConnectorRecord(
                    external_id=f"git:{self._repo_name}:{sha}",
                    title=subject,
                    body=f"{subject} (by {author})",
                    knowledge_type="text",
                    metadata={"sha": sha, "author": author, "repo": self._repo_name},
                    reference_time=parse_iso(when),
                )
            )
            if newest == last_sha:
                newest = sha  # git log lists newest first

        next_cursor: JsonDict = {"last_sha": newest} if newest else {}
        return ConnectorBatch(records=tuple(records), next_cursor=next_cursor)
