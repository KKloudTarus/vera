from __future__ import annotations

from typing import get_args

from fastapi.params import Depends

from vera.entrypoints.api.deps import UnitOfWorkDep


def test_identity_uow_commits_before_response_is_sent() -> None:
    dependency = next(item for item in get_args(UnitOfWorkDep) if isinstance(item, Depends))

    assert dependency.scope == "function"
