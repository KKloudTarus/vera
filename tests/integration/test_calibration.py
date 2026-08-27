"""Rerank calibration against the live database: log feedback with a signal vector,
read it back, and calibrate weights from it.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories import SqlAlchemyRetrievalReadModel
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.queries.calibration import CalibrationService
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_HL = 30 * 24 * 3600.0


async def _record(
    sessionmaker: async_sessionmaker[AsyncSession],
    group: str,
    *,
    ref: str,
    signal: str,
    authority: float,
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        await uow.feedback.record(
            group_id=group,
            principal_id=None,
            query="q",
            result_ref=ref,
            signal=signal,
            signals={"authority": authority, "relevance": 0.5},
        )
        await uow.commit()


async def test_calibrates_authority_weight_from_logged_feedback(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    # Up votes landed on high-authority results, down votes on low-authority ones.
    await _record(sessionmaker, group, ref="a", signal="up", authority=1.0)
    await _record(sessionmaker, group, ref="b", signal="up", authority=0.9)
    await _record(sessionmaker, group, ref="c", signal="down", authority=0.1)
    await _record(sessionmaker, group, ref="d", signal="down", authority=0.0)

    read_model = SqlAlchemyRetrievalReadModel(sessionmaker)
    samples = await read_model.calibration_samples(group_ids=[group])
    assert len(samples) == 4

    weights = await CalibrationService(read_model).calibrate(group_ids=[group], half_life_s=_HL)
    assert weights.authority > 0.9  # authority separated helpful from unhelpful
    assert weights.relevance == 0.0  # relevance was constant, earns nothing

    assert group in await read_model.feedback_groups()
