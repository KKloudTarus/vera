"""Measure semantic dedup on labeled entity-name pairs before trusting it on real data.

Reads a JSON file of ``[left, right, same]`` triples (same is a bool: do the two names
denote the same real-world entity?), embeds the names with the configured model, and
reports the precision and recall of a cosine threshold across a sweep, plus, when a key
is set, how the LLM judge decides the same pairs. Use it to pick a threshold and to
confirm the judge is accurate on your entities before enabling dedup in production.

    python -m vera.entrypoints.dedup_eval labeled_pairs.json
"""

from __future__ import annotations

import asyncio
import json
import sys

from vera.application.curation.dedup_benchmark import benchmark_names
from vera.config.settings import get_settings
from vera.observability import configure_logging

_THRESHOLDS = [round(0.50 + 0.02 * i, 2) for i in range(26)]  # 0.50 .. 1.00


def _load_pairs(path: str) -> list[tuple[str, str, bool]]:
    with open(path, encoding="utf-8") as handle:
        raw = json.load(handle)
    return [(str(a), str(b), bool(same)) for a, b, same in raw]


async def _run(path: str) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    if settings.memory.embedder != "openai" or settings.memory.openai_api_key is None:
        raise SystemExit("configure an OpenAI embedder (VERA_MEMORY__EMBEDDER=openai) to evaluate")

    pairs = _load_pairs(path)
    from vera.adapters.graph import build_embedder

    embedder = build_embedder(settings)
    results = await benchmark_names(embedder, pairs, _THRESHOLDS)  # type: ignore[arg-type]

    print(f"cosine sweep over {len(pairs)} labeled pairs:")  # operator-facing
    print(f"{'threshold':>9}  {'precision':>9}  {'recall':>7}  {'f1':>5}")
    for r in results:
        print(f"{r.threshold:>9.2f}  {r.precision:>9.3f}  {r.recall:>7.3f}  {r.f1:>5.3f}")
    best = max(results, key=lambda r: r.f1)
    print(f"best f1={best.f1:.3f} at threshold={best.threshold:.2f}")

    key = settings.memory.openai_api_key.get_secret_value()
    from vera.adapters.curation.entity_judge import LlmEntityResolutionJudge

    judge = LlmEntityResolutionJudge(api_key=key, model=settings.memory.llm_model)
    correct = 0
    for left, right, same in pairs:
        verdict = await judge.same_entity(name=left, entity_type="Entity", candidates=[right])
        if (verdict == right) == same:
            correct += 1
    print(f"judge agreement with labels: {correct}/{len(pairs)}")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m vera.entrypoints.dedup_eval <labeled_pairs.json>")
    asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    main()
