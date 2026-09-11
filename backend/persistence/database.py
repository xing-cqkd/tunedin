import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from backend.config import reload_settings
from backend.persistence._sqlite import configure_sqlite_fk
from backend.persistence.models import Base

# XIN-42: sourced from the centralized settings object; the DATABASE_URL
# environment variable name is unchanged. Settings are rebuilt (not read
# from cache) so importlib.reload() after a setenv picks up the new value,
# preserving the historical import-time binding behavior.
DATABASE_URL = reload_settings().database_url

# Create Async Engine
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

# Enable Foreign Key enforcement for SQLite
if DATABASE_URL.startswith("sqlite"):
    configure_sqlite_fk(engine)

# Session Factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

async def init_db():
    """Bring the app database schema up to date via Alembic migrations.

    Databases created by the old create_all path (tables exist but no
    alembic_version table) are stamped at head instead of migrated, since
    they already match the current models.
    """
    from alembic import command as alembic_command
    from alembic.config import Config

    cfg = Config(str(Path(__file__).resolve().parent.parent / "alembic.ini"))

    async with engine.begin() as conn:
        has_version_table = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).has_table("alembic_version")
        )
        has_app_tables = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).has_table("feeds")
        )

    def _run_migrations():
        if has_app_tables and not has_version_table:
            alembic_command.stamp(cfg, "head")
        else:
            alembic_command.upgrade(cfg, "head")

    # Alembic's env.py drives its own event loop, so run it in a thread.
    await asyncio.to_thread(_run_migrations)


async def create_all_tables():
    """Create all tables directly from models (tests and throwaway DBs only).

    Application startup should use init_db(), which runs Alembic migrations.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager yielding a session on the app database."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


get_db = session_scope
"""Alias kept for the conventional FastAPI dependency name.

Identical to :func:`session_scope`; nothing in the codebase currently uses
it (the configured-backend dependency lives in ``settings.get_db``), but the
name is kept so external FastAPI ``Depends(get_db)`` usage keeps working.
"""
