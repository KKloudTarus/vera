"""PDF connector: documents in a directory become text artifacts.

Incremental via a modified-time watermark in the cursor, so only new or changed files
are re-read. The external id is the stable file name. Text extraction is injectable;
the default lazily uses ``pypdf`` so the connector carries no hard dependency and stays
testable with a plain-text extractor.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict

Extractor = Callable[[Path], str]


def _default_extract(path: Path) -> str:
    # pypdf has no type stubs and is an optional, lazily-imported dependency, so its
    # attributes read as Unknown; funnel it through Any and extract page text as strings.
    from pypdf import PdfReader  # type: ignore[import-untyped]

    reader: Any = PdfReader(str(path))  # pyright: ignore[reportUnknownVariableType]
    pages: list[Any] = list(reader.pages)  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
    return "\n".join(str(page.extract_text() or "") for page in pages)


class PdfConnector:
    def __init__(
        self, directory: str, *, glob: str = "*.pdf", extract: Extractor | None = None
    ) -> None:
        self._directory = Path(directory)
        self._glob = glob
        self._extract = extract or _default_extract

    @property
    def kind(self) -> str:
        return "pdf"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        since = float(cursor["since_mtime"]) if cursor and cursor.get("since_mtime") else 0.0
        records: list[ConnectorRecord] = []
        latest = since
        for path in sorted(self._directory.glob(self._glob)):
            mtime = path.stat().st_mtime
            if mtime <= since:
                continue
            records.append(
                ConnectorRecord(
                    external_id=f"pdf:{path.name}",
                    title=path.stem,
                    body=self._extract(path),
                    knowledge_type="text",
                    metadata={"filename": path.name},
                    reference_time=datetime.fromtimestamp(mtime, tz=UTC),
                )
            )
            latest = max(latest, mtime)

        next_cursor: JsonDict = {"since_mtime": latest}
        return ConnectorBatch(records=tuple(records), next_cursor=next_cursor)
