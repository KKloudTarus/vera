"""Alembic migration environment (async).

The database URL comes from VERA settings, not from alembic.ini, so migrations use
the same configuration as the application. Importing the models module registers
every table on Base.metadata for autogenerate.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from vera.adapters.persistence import base as persistence_base
from vera.adapters.persistence import models as _models  # noqa: F401  (registers tables)
from vera.config.settings import get_settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = persistence_base.Base.metadata


def _database_url() -> str:
    return str(get_settings().db.dsn)


def _include_object(
    obj: object, name: str | None, type_: str, reflected: bool, compare_to: object
) -> bool:
    # Never auto-drop a reflected table that is not modeled. Declarative partitions
    # (audit_events_default, retrieval_feedback_default) live in the database but not
    # in Base.metadata; without this, autogenerate would try to drop them.
    if type_ == "table" and reflected and compare_to is None:
        return name in target_metadata.tables
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_object=_include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        include_object=_include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _database_url()
    connectable = async_engine_from_config(configuration, prefix="sqlalchemy.")
    async with connectable.connect() as connection:
        await connection.run_sync(_do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
