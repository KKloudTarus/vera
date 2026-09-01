from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest

from vera.bootstrap import Container
from vera.config.settings import get_settings
from vera.entrypoints.knowledge import service as knowledge_service
from vera.entrypoints.knowledge.service import (
    KnowledgeService,
    active_embedding_version,
    active_retrieval_index_version,
)


def _container(*, threshold: float = 0.35, top_n: int = 20) -> Container:
    base = get_settings()
    memory = base.memory.model_copy(
        update={
            "vector_search_enabled": True,
            "embedder": "voyage",
            "embedding_model": "wrong-default-model",
            "embedding_dim": 1536,
        }
    )
    voyage = base.voyage.model_copy(
        update={
            "embedding_model": "voyage-4-lite",
            "embedding_dim": 1024,
            "rerank_model": "rerank-2.5",
        }
    )
    rerank = base.rerank.model_copy(
        update={
            "cross_encoder_enabled": True,
            "cross_encoder_provider": "voyage",
            "cross_encoder_min_score": threshold,
            "cross_encoder_top_n": top_n,
        }
    )
    settings = base.model_copy(update={"memory": memory, "voyage": voyage, "rerank": rerank})
    return cast(
        "Container",
        SimpleNamespace(settings=settings, embedder=object(), reranker=object()),
    )


def test_voyage_snapshot_metadata_uses_the_active_embedding() -> None:
    assert active_embedding_version(_container()) == {
        "provider": "voyage",
        "model": "voyage-4-lite",
        "model_version": "1",
        "dimension": 1024,
    }


def test_snapshot_retrieval_version_pins_the_reranker_threshold() -> None:
    assert active_retrieval_index_version(
        _container(threshold=0.35)
    ) != active_retrieval_index_version(_container(threshold=0.5))
    assert active_retrieval_index_version(_container(top_n=20)) != active_retrieval_index_version(
        _container(top_n=50)
    )


@pytest.mark.asyncio
async def test_failed_production_search_records_latency(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[dict[str, float | int]] = []

    class _Scopes:
        async def resolve(self, _principal_id: object) -> object:
            return SimpleNamespace(group_ids=["p:demo"], personal_group_id=None)

    class _Assembler:
        async def assemble(self, **_values: object) -> object:
            raise TimeoutError

    service = KnowledgeService.__new__(KnowledgeService)
    service._scopes = _Scopes()  # type: ignore[assignment]
    service._assembler = _Assembler()  # type: ignore[assignment]
    monkeypatch.setattr(
        knowledge_service, "record_search", lambda **values: observed.append(values)
    )

    with pytest.raises(TimeoutError):
        await service.search(uuid4(), query="x", project="p:demo")

    assert len(observed) == 1
    assert observed[0]["duration_s"] >= 0
    assert observed[0]["hits"] == 0
