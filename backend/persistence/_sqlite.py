"""Shared SQLite engine helpers for the persistence layer (Linear: XIN-133).

SQLite does not enforce foreign keys unless ``PRAGMA foreign_keys=ON`` is
set on every connection, so every engine bound to a SQLite database must
register the connect listener below. It used to be copy-pasted into
``persistence/database.py``, ``ingestion/simple_db.py``, and
``migrate_data.SqlAlchemyBackend.from_url``; this module holds the single
copy.
"""

from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine


def configure_sqlite_fk(engine: AsyncEngine) -> None:
    """Enable ``PRAGMA foreign_keys=ON`` on every new SQLite connection.

    Call sites guard on the URL (``url.startswith("sqlite")``) so non-SQLite
    engines never get the listener.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
