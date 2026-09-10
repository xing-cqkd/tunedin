import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from backend.persistence._sqlite import configure_sqlite_fk
from backend.persistence.models.base import Base

# Database file path located directly in the ingestion directory
INGESTION_DB_PATH = Path(__file__).parent / "simple.db"
DATABASE_URL = os.getenv(
    "INGESTION_DATABASE_URL",
    f"sqlite+aiosqlite:///{INGESTION_DB_PATH.resolve()}",
)

# Create Async SQLAlchemy Engine
engine: AsyncEngine = create_async_engine(
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


async def init_db() -> None:
    """Asynchronously creates all tables in the ingestion simple.db database."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency / generator providing an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for scoping a database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
