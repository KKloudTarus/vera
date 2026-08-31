"""Generate the deterministic JSONL corpus declared by evals/fixtures/load.json."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

GENERATOR_VERSION = "1.1"


def canonical_line(value: dict[str, Any]) -> bytes:
    serialized = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return f"{serialized}\n".encode()


def fact(scope_index: int, fact_index: int, seed: int, facts_per_scope: int) -> dict[str, Any]:
    service = f"service-{scope_index:02d}-{fact_index:04d}"
    variant = (fact_index + seed) % 5
    if variant == 0:
        triple = {
            "subject": service,
            "predicate": "RUNS_ON",
            "object": f"cluster-{scope_index:02d}-{fact_index % 10:02d}",
        }
    elif variant in {1, 2}:
        triple = {
            "subject": service,
            "predicate": "DEPENDS_ON",
            "object": (f"service-{scope_index:02d}-{(fact_index + variant) % facts_per_scope:04d}"),
        }
    elif variant == 3:
        triple = {
            "subject": f"team-{scope_index:02d}-{fact_index % 20:02d}",
            "predicate": "OWNS",
            "object": service,
        }
    else:
        triple = {
            "subject": service,
            "predicate": "DEPLOYED_TO",
            "object": f"environment-{scope_index:02d}-{fact_index % 4:02d}",
        }
    return {
        "record_type": "fact",
        "fact_id": f"f-{scope_index:02d}-{fact_index:04d}",
        "scope_key": f"scope-{scope_index:02d}",
        "source_event_time": f"2026-06-{1 + fact_index % 28:02d}T00:00:00Z",
        "triple": triple,
    }


def query_for(item: dict[str, Any]) -> str:
    triple = item["triple"]
    predicate = triple["predicate"]
    if predicate == "RUNS_ON":
        return f"Where does {triple['subject']} run?"
    if predicate == "DEPENDS_ON":
        return f"What does {triple['subject']} depend on?"
    if predicate == "OWNS":
        return f"What does {triple['subject']} own?"
    return f"Where is {triple['subject']} deployed?"


def records(
    *, scopes: int, facts_per_scope: int, queries: int, seed: int
) -> Iterator[dict[str, Any]]:
    for scope_index in range(scopes):
        for fact_index in range(facts_per_scope):
            yield fact(scope_index, fact_index, seed, facts_per_scope)
    for query_index in range(queries):
        if query_index % 10 == 0:
            yield {
                "record_type": "query",
                "query_id": f"q-{query_index:04d}",
                "text": f"Unknown canary question {query_index}",
                "relevance": {},
            }
            continue
        scope_index = query_index % scopes
        fact_index = (query_index * 37 + seed) % facts_per_scope
        target = fact(scope_index, fact_index, seed, facts_per_scope)
        yield {
            "record_type": "query",
            "query_id": f"q-{query_index:04d}",
            "text": query_for(target),
            "relevance": {target["fact_id"]: 3},
        }


def generate(
    *, output: Path | None, scopes: int, facts_per_scope: int, queries: int, seed: int
) -> str:
    digest = hashlib.sha256()
    handle = None
    if output is not None:
        if not output.parent.is_dir():
            raise ValueError(f"output parent does not exist: {output.parent}")
        handle = output.open("wb")
    try:
        for record in records(
            scopes=scopes,
            facts_per_scope=facts_per_scope,
            queries=queries,
            seed=seed,
        ):
            line = canonical_line(record)
            digest.update(line)
            if handle is not None:
                handle.write(line)
    finally:
        if handle is not None:
            handle.close()
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scopes", type=int, default=20)
    parser.add_argument("--facts-per-scope", type=int, default=200)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20260828)
    args = parser.parse_args()
    if args.scopes < 1 or args.facts_per_scope < 1 or args.queries < 1:
        parser.error("scopes, facts-per-scope, and queries must be positive")
    digest = generate(
        output=args.output,
        scopes=args.scopes,
        facts_per_scope=args.facts_per_scope,
        queries=args.queries,
        seed=args.seed,
    )
    print(f"generator={GENERATOR_VERSION} sha256={digest}")


if __name__ == "__main__":
    main()
