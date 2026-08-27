"""Evaluate retrieval quality against a golden set, as a CI gate or a manual check.

The golden file is JSON: ``{"group_ids": [...], "k": 5, "min_hit_rate": 0.8,
"cases": [{"query": "...", "expected": ["substring", ...]}, ...]}``. Each query is run
through the real ranker over the given scopes; the command prints hit@k and MRR and exits
non-zero if the hit rate is below ``min_hit_rate``, so a regression in ranking or dedup
fails the build.

    python -m vera.entrypoints.retrieval_eval golden.json
"""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass

from vera.application.queries.retrieval_eval import score
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.bootstrap import build_container, dispose_container, refresh_rerank_weights
from vera.config.settings import get_settings
from vera.observability import configure_logging
from vera.shared.types import GroupId


@dataclass(frozen=True, slots=True)
class _Spec:
    group_ids: tuple[str, ...]
    k: int
    min_hit_rate: float
    cases: list[tuple[str, list[str]]]


def _load_spec(path: str) -> _Spec:
    with open(path, encoding="utf-8") as handle:
        raw = json.load(handle)
    cases = [(str(c["query"]), [str(e) for e in c["expected"]]) for c in raw["cases"]]
    return _Spec(
        group_ids=tuple(str(g) for g in raw["group_ids"]),
        k=int(raw.get("k", 5)),
        min_hit_rate=float(raw.get("min_hit_rate", 0.0)),
        cases=cases,
    )


async def _run(spec: _Spec) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    group_ids = tuple(GroupId(g) for g in spec.group_ids)

    container = build_container(settings)
    try:
        await refresh_rerank_weights(container)
        handler = SearchMemoryHandler(
            container.memory, container.retrieval_read, weights=container.rerank_weights
        )
        per_case: list[tuple[list[str], list[str]]] = []
        for query, expected in spec.cases:
            hits = await handler.handle(SearchMemory(text=query, group_ids=group_ids, limit=spec.k))
            per_case.append(([h.fact for h in hits], expected))
    finally:
        await dispose_container(container)

    report = score(per_case, k=spec.k)
    print(  # operator-facing
        f"cases={report.cases} hit@{spec.k}={report.hits_at_k}/{report.cases} "
        f"({report.hit_rate:.2%}) mrr={report.mrr:.3f}"
    )
    if report.hit_rate < spec.min_hit_rate:
        raise SystemExit(f"hit rate {report.hit_rate:.2%} below required {spec.min_hit_rate:.2%}")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m vera.entrypoints.retrieval_eval <golden.json>")
    asyncio.run(_run(_load_spec(sys.argv[1])))


if __name__ == "__main__":
    main()
