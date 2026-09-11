"""Shared SQLite engine helpers for the persistence layer (Linear: XIN-133).

SQLite does not enforce foreign keys unless ``PRAGMA foreign_keys=ON`` is
set on every connection, so every engine bound to a SQLite database must
register the connect listener below. It used to be copy-pasted into
``persistence/database.py``, ``ingestion/simple_db.py``, and
``migrate_data.SqlAlchemyBackend.from_url``; this module holds the single
copy.

XIN-58: the same listener also sets WAL journal mode and a busy timeout.
Concurrent workers (e.g. the crawler's N sync workers) each open their
own connection to the same SQLite file, and SQLite allows only a single
writer — without WAL + busy_timeout the workers intermittently hit
``sqlite3.OperationalError: database is locked``. WAL lets readers
proceed while a writer holds the lock; the busy timeout makes a blocked
writer wait up to 5s instead of raising immediately.
"""

from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine


def configure_sqlite_fk(engine: AsyncEngine) -> None:
    """Enable the SQLite pragmas on every new connection.

    ``foreign_keys=ON`` (FK enforcement), ``journal_mode=WAL``
    (concurrent reader/writer access to one file), ``busy_timeout=5000``
    (wait up to 5s on a locked database instead of raising immediately).

    Call sites guard on the URL (``url.startswith("sqlite")``) so non-SQLite
    engines never get the listener.

    (Kept the historical ``configure_sqlite_fk`` name: the same listener
    is referenced from ``database.py``, ``simple_db.py``, and
    ``migrate_data.py``, and it still configures the FK pragma too.)
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()
