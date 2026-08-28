"""Benchmark harness for the Knowledge Fabric retrieval path (Phase 8).

Seeds a throwaway group with N facts and times context assembly over it, reporting p50/p95/p99
context-pack latency. This produces measured numbers in your environment; VERA claims no
scalability figures without a run (section 20). It cleans up the group afterward.

    python -m vera.entrypoints.benchmark_fabric [--facts N] [--queries M]
"""

from __future__ import annotations

import argparse
import asyncio
import time

from sqlalchemy import text

from vera.adapters.persistence.repositories.canonical import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import SqlAlchemyFactRepository
from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyCodeIndex,
    SqlAlchemyFactCandidateSource,
    SqlAlchemyPassageIndex,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.retrieval import ContextAssembler
from vera.bootstrap import build_container, dispose_container
from vera.config.settings import get_settings
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Fact, FactLifecycle, ObjectType
from vera.observability import configure_logging, get_logger
from vera.shared.ids import uuid7

log = get_logger(__name__)

_OBJECTS = ["eks", "ecs", "fargate", "gke", "aks", "postgres", "valkey", "kafka", "s3", "lambda"]


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[idx]


async def _seed(sessionmaker: object, group: str, facts: int) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:  # type: ignore[arg-type]
        await uow.use_tenant(group)
        canonical = SqlAlchemyCanonicalEntityRepository(uow.session)
        repo = SqlAlchemyFactRepository(uow.session)
        for i in range(facts):
            entity = await canonical.create(
                group_id=group, entity_type="Service", canonical_name=f"service-{i}", aliases=[]
            )
            obj = _OBJECTS[i % len(_OBJECTS)]
            await repo.upsert(
                Fact(
                    id=uuid7(),
                    group_id=group,
                    fact_key=fabric.fact_key(
                        scope=group,
                        subject_entity_id=entity.id,
                        predicate="RUNS_ON",
                        object_scalar=obj,
                    ),
                    slot_key=fabric.slot_key(
                        scope=group, subject_entity_id=entity.id, predicate="RUNS_ON"
                    ),
                    subject_entity_id=entity.id,
                    predicate="RUNS_ON",
                    object_type=ObjectType.SCALAR,
                    normalized_object=fabric.normalize_object(object_scalar=obj),
                    object_scalar=obj,
                    lifecycle_state=FactLifecycle.ACTIVE,
                    authority=1.0,
                    confidence=0.9,
                )
            )
        await uow.commit()


async def _run(facts: int, queries: int) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    group = f"p:bench-{uuid7().hex[:12]}"
    try:
        await _seed(container.sessionmaker, group, facts)
        assembler = ContextAssembler(
            facts=SqlAlchemyFactCandidateSource(container.sessionmaker),
            passages=SqlAlchemyPassageIndex(container.sessionmaker),
            code=SqlAlchemyCodeIndex(container.sessionmaker),
        )
        latencies_ms: list[float] = []
        for i in range(queries):
            obj = _OBJECTS[i % len(_OBJECTS)]
            start = time.perf_counter()
            await assembler.assemble(query=f"service {obj}", group_id=group, limit=10)
            latencies_ms.append((time.perf_counter() - start) * 1000.0)
        log.info(
            "benchmark.context_pack_latency_ms",
            facts=facts,
            queries=queries,
            p50=round(_percentile(latencies_ms, 50), 2),
            p95=round(_percentile(latencies_ms, 95), 2),
            p99=round(_percentile(latencies_ms, 99), 2),
        )
    finally:
        async with container.sessionmaker() as session, session.begin():
            await session.execute(text("SET LOCAL ROLE vera_app"))
            await session.execute(
                text("SELECT set_config('vera.group_id', :g, true)"), {"g": group}
            )
            await session.execute(text("DELETE FROM facts WHERE group_id = :g"), {"g": group})
            await session.execute(
                text("DELETE FROM canonical_entities WHERE group_id = :g"), {"g": group}
            )
        await dispose_container(container)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Knowledge Fabric retrieval latency")
    parser.add_argument("--facts", type=int, default=1000)
    parser.add_argument("--queries", type=int, default=100)
    args = parser.parse_args()
    asyncio.run(_run(args.facts, args.queries))


if __name__ == "__main__":
    main()
