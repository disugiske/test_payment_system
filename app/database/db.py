from collections.abc import AsyncIterator

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import Settings, get_settings


def build_database_url(
    config_settings: Settings,
) -> URL:
    """Build a SQLAlchemy URL from application settings.

    Uses ``sqlalchemy.URL.create`` so credentials with special characters
    are escaped correctly and the password is never logged in repr.
    """

    return URL.create(
        drivername=config_settings.driver,
        username=config_settings.postgres_user,
        password=config_settings.postgres_password.get_secret_value(),
        host=config_settings.postgres_host,
        port=config_settings.postgres_port,
        database=config_settings.postgres_db,
    )


settings = get_settings()

connect_args = {
    "server_settings": {
        "application_name": settings.service_name,
    },
    "statement_cache_size": 0,
}


engine = create_async_engine(
    build_database_url(settings),
    pool_pre_ping=True,
    pool_size=settings.min_pool_size,
    max_overflow=settings.max_pool_size,
    pool_recycle=settings.pool_recycle,
    connect_args=connect_args,
    echo=settings.debug,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    pass


async def get_session() -> AsyncIterator[AsyncSession]:
    async with async_session_factory() as session:
        yield session
