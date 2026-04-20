"""Test fixtures.

Spins up an ephemeral PostgreSQL via testcontainers, applies Alembic
migrations once per session, and truncates tables between tests.

The container is started at conftest import time (BEFORE any ``from app.*``
import happens in test modules), because ``app.database.db`` builds the
SQLAlchemy engine eagerly at module load.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING

import pytest
from testcontainers.postgres import PostgresContainer

if TYPE_CHECKING:
    from httpx import AsyncClient

# ---------------------------------------------------------------------------
# Module-level setup: start Postgres + set env vars BEFORE app modules import.
# ---------------------------------------------------------------------------
_pg = PostgresContainer("postgres:16-alpine", driver="asyncpg")
_pg.start()

os.environ["POSTGRES_HOST"] = _pg.get_container_host_ip()
os.environ["POSTGRES_PORT"] = str(_pg.get_exposed_port(5432))
os.environ["POSTGRES_USER"] = _pg.username
os.environ["POSTGRES_PASSWORD"] = _pg.password
os.environ["POSTGRES_DB"] = _pg.dbname
os.environ["API_KEY"] = "test-api-key"

from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402

command.upgrade(Config("alembic.ini"), "head")

# Replace the production engine with one using NullPool — connections are
# created/closed per call so they never cross asyncio event loops between tests.
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402

import app.database as _db_pkg  # noqa: E402
import app.database.db as _db_mod  # noqa: E402

from app.config import get_settings  # noqa: E402

_test_engine = create_async_engine(
    _db_mod.build_database_url(get_settings()), poolclass=NullPool
)
_test_factory = async_sessionmaker(
    _test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)
_db_mod.engine = _test_engine
_db_mod.async_session_factory = _test_factory
_db_pkg.async_session_factory = _test_factory
_db_pkg.engine = _test_engine


@pytest.fixture(scope="session", autouse=True)
def _stop_container() -> Iterator[None]:
    yield
    _pg.stop()


@pytest.fixture(autouse=True)
async def _truncate_tables() -> AsyncIterator[None]:
    yield
    from sqlalchemy import text

    from app.database import async_session_factory

    async with async_session_factory() as session:
        await session.execute(text("TRUNCATE payments, outbox RESTART IDENTITY"))
        await session.commit()


@pytest.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    from app.database import async_session_factory

    async with async_session_factory() as session:
        yield session


@pytest.fixture
async def api_client() -> AsyncIterator["AsyncClient"]:
    from httpx import ASGITransport, AsyncClient

    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-api-key"},
    ) as client:
        yield client
