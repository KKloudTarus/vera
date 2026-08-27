"""Filesystem connector: text and markdown documents in a directory tree.

Reads matching files recursively (docs, runbooks, a repository's markdown), so a local
checkout becomes a knowledge source. Incremental via a modified-time watermark; the
external id is the path relative to the root, so a changed file updates in place.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict

_DEFAULT_PATTERNS = ("*.md", "*.mdx", "*.txt", "*.rst")


class FilesystemConnector:
    def __init__(
        self,
        root: str,
        *,
        patterns: tuple[str, ...] = _DEFAULT_PATTERNS,
        max_bytes: int = 200_000,
    ) -> None:
        self._root = Path(root)
        self._patterns = patterns
        self._max_bytes = max_bytes

    @property
    def kind(self) -> str:
        return "filesystem"

    def _paths(self) -> list[Path]:
        found: set[Path] = set()
        for pattern in self._patterns:
            found.update(p for p in self._root.rglob(pattern) if p.is_file())
        return sorted(found)

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        since = float(cursor["since_mtime"]) if cursor and cursor.get("since_mtime") else 0.0
        records: list[ConnectorRecord] = []
        latest = since
        for path in self._paths():
            mtime = path.stat().st_mtime
            if mtime <= since:
                continue
            body = path.read_text(encoding="utf-8", errors="replace")[: self._max_bytes]
            rel = path.relative_to(self._root).as_posix()
            records.append(
                ConnectorRecord(
                    external_id=f"fs:{rel}",
                    title=rel,
                    body=body,
                    knowledge_type="text",
                    metadata={"path": rel},
                    reference_time=datetime.fromtimestamp(mtime, tz=UTC),
                )
            )
            latest = max(latest, mtime)

        return ConnectorBatch(records=tuple(records), next_cursor={"since_mtime": latest})
