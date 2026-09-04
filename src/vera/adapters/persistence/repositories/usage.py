"""Durable sink for LLM usage events: one row per provider call in ``llm_usage``.

Writes in its own short transaction so metering never rides on the caller's unit of
work (a cost row must not be rolled back with a failed ingest, and must not hold the
ingest transaction open). Cost per episode or per query is then a simple aggregate.
"""

from __future__ import annotations

import math

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.ops import LlmUsageRow
from vera.observability.cost import UsageEvent


class SqlAlchemyUsageSink:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        record_enabled: bool = True,
    ) -> None:
        self._session_factory = session_factory
        self._record_enabled = record_enabled

    async def record(self, event: UsageEvent) -> None:
        if not self._record_enabled:
            return
        async with self._session_factory() as session, session.begin():
            session.add(
                LlmUsageRow(
                    model=event.model,
                    operation=event.operation,
                    request_kind=event.request_kind,
                    group_id=event.group_id,
                    ref=event.ref,
                    prompt_tokens=event.prompt_tokens,
                    completion_tokens=event.completion_tokens,
                    cost_usd=event.cost_usd,
                    cost_complete=event.cost_complete,
                )
            )

    async def fence_provider_attempt(self, job_id: str, claim_token: str) -> None:
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                text(
                    "UPDATE ingestion_jobs SET provider_retry_fenced=true "
                    "WHERE id=:job_id AND claim_token=:claim_token "
                    "AND status='inflight' RETURNING id"
                ),
                {"job_id": job_id, "claim_token": claim_token},
            )
            if result.scalar_one_or_none() is None:
                raise RuntimeError("provider attempt has no inflight ingestion job")

    async def initialize_provider_budget(
        self,
        action_key: str,
        max_cost_usd: float,
        *,
        run_key: str,
        run_max_cost_usd: float,
    ) -> None:
        if not action_key or len(action_key) > 512:
            raise ValueError("provider budget action key is invalid")
        if not run_key or len(run_key) > 256:
            raise ValueError("provider budget run key is invalid")
        if not math.isfinite(max_cost_usd) or max_cost_usd <= 0:
            raise ValueError("provider budget maximum must be finite and positive")
        if not math.isfinite(run_max_cost_usd) or run_max_cost_usd <= 0:
            raise ValueError("provider run budget maximum must be finite and positive")
        if max_cost_usd > run_max_cost_usd:
            raise ValueError("provider action budget exceeds its run budget")
        async with self._session_factory() as session, session.begin():
            run_created = await session.scalar(
                text(
                    "INSERT INTO provider_run_budget_reservations "
                    "(run_key, max_cost_usd, reserved_cost_usd) VALUES (:key, :maximum, 0) "
                    "ON CONFLICT (run_key) DO NOTHING RETURNING run_key"
                ),
                {"key": run_key, "maximum": run_max_cost_usd},
            )
            if run_created is None:
                run_maximum = await session.scalar(
                    text(
                        "SELECT max_cost_usd FROM provider_run_budget_reservations "
                        "WHERE run_key=:key"
                    ),
                    {"key": run_key},
                )
                if run_maximum is None or float(run_maximum) != run_max_cost_usd:
                    raise RuntimeError("provider run budget conflicts with existing run")
            created = await session.scalar(
                text(
                    "INSERT INTO provider_budget_reservations "
                    "(action_key, run_key, max_cost_usd, reserved_cost_usd) "
                    "VALUES (:key, :run_key, :maximum, 0) "
                    "ON CONFLICT (action_key) DO NOTHING RETURNING action_key"
                ),
                {"key": action_key, "run_key": run_key, "maximum": max_cost_usd},
            )
            if created is None:
                existing = (
                    await session.execute(
                        text(
                            "SELECT run_key, max_cost_usd FROM provider_budget_reservations "
                            "WHERE action_key=:key"
                        ),
                        {"key": action_key},
                    )
                ).one_or_none()
                if (
                    existing is None
                    or str(existing.run_key) != run_key
                    or float(existing.max_cost_usd) != max_cost_usd
                ):
                    raise RuntimeError("provider budget reservation conflicts with existing action")

    async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool:
        if not action_key or len(action_key) > 512:
            raise ValueError("provider budget action key is invalid")
        if not math.isfinite(max_cost_usd) or max_cost_usd <= 0:
            raise ValueError("provider budget reservation must be finite and positive")
        async with self._session_factory() as session, session.begin():
            budgets = (
                await session.execute(
                    text(
                        "SELECT action_budget.reserved_cost_usd AS action_reserved, "
                        "action_budget.max_cost_usd AS action_maximum, "
                        "run_budget.run_key AS run_key, "
                        "run_budget.reserved_cost_usd AS run_reserved, "
                        "run_budget.max_cost_usd AS run_maximum "
                        "FROM provider_budget_reservations AS action_budget "
                        "JOIN provider_run_budget_reservations AS run_budget "
                        "ON run_budget.run_key=action_budget.run_key "
                        "WHERE action_budget.action_key=:key "
                        "FOR UPDATE OF run_budget, action_budget"
                    ),
                    {"key": action_key},
                )
            ).one_or_none()
            if budgets is None:
                return False
            if float(budgets.action_reserved) + max_cost_usd > float(
                budgets.action_maximum
            ) or float(budgets.run_reserved) + max_cost_usd > float(budgets.run_maximum):
                return False
            await session.execute(
                text(
                    "UPDATE provider_run_budget_reservations "
                    "SET reserved_cost_usd=reserved_cost_usd + :cost WHERE run_key=:run_key"
                ),
                {"run_key": str(budgets.run_key), "cost": max_cost_usd},
            )
            await session.execute(
                text(
                    "UPDATE provider_budget_reservations "
                    "SET reserved_cost_usd=reserved_cost_usd + :cost WHERE action_key=:key"
                ),
                {"key": action_key, "cost": max_cost_usd},
            )
            return True

    async def settle_provider_budget(
        self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
    ) -> bool:
        if not action_key or len(action_key) > 512:
            raise ValueError("provider budget action key is invalid")
        if not math.isfinite(reserved_cost_usd) or reserved_cost_usd <= 0:
            raise ValueError("provider settled reservation must be finite and positive")
        if (
            not math.isfinite(actual_cost_usd)
            or actual_cost_usd < 0
            or actual_cost_usd > reserved_cost_usd
        ):
            raise ValueError("provider settled cost must fit its reservation")
        released_cost_usd = reserved_cost_usd - actual_cost_usd
        async with self._session_factory() as session, session.begin():
            budgets = (
                await session.execute(
                    text(
                        "SELECT action_budget.reserved_cost_usd AS action_reserved, "
                        "run_budget.run_key AS run_key, "
                        "run_budget.reserved_cost_usd AS run_reserved "
                        "FROM provider_budget_reservations AS action_budget "
                        "JOIN provider_run_budget_reservations AS run_budget "
                        "ON run_budget.run_key=action_budget.run_key "
                        "WHERE action_budget.action_key=:key "
                        "FOR UPDATE OF run_budget, action_budget"
                    ),
                    {"key": action_key},
                )
            ).one_or_none()
            if (
                budgets is None
                or float(budgets.action_reserved) < reserved_cost_usd
                or float(budgets.run_reserved) < reserved_cost_usd
            ):
                return False
            if released_cost_usd == 0:
                return True
            await session.execute(
                text(
                    "UPDATE provider_run_budget_reservations "
                    "SET reserved_cost_usd=reserved_cost_usd - :cost WHERE run_key=:run_key"
                ),
                {"run_key": str(budgets.run_key), "cost": released_cost_usd},
            )
            await session.execute(
                text(
                    "UPDATE provider_budget_reservations "
                    "SET reserved_cost_usd=reserved_cost_usd - :cost WHERE action_key=:key"
                ),
                {"key": action_key, "cost": released_cost_usd},
            )
            return True

    async def total_cost_for_group(self, group_id: str) -> float:
        async with self._session_factory() as session:
            total = await session.scalar(
                select(func.coalesce(func.sum(LlmUsageRow.cost_usd), 0.0)).where(
                    LlmUsageRow.group_id == group_id
                )
            )
        return float(total or 0.0)

    async def total_cost_for_ref(self, ref: str) -> float:
        async with self._session_factory() as session:
            total = await session.scalar(
                select(func.coalesce(func.sum(LlmUsageRow.cost_usd), 0.0)).where(
                    LlmUsageRow.ref == ref
                )
            )
        return float(total or 0.0)
