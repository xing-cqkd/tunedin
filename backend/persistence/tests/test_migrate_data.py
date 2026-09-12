"""Tests for backend.migrate_data (cross-backend data migration CLI)."""

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from backend.migrate_data import (
    SameBackendError,
    SqlAlchemyBackend,
    _normalize_url,
    assert_distinct_backends,
    get_backend,
    main,
    migrate,
    reconcile_tables,
    resolve_url,
)
from backend.persistence.dynamodb.migrate_adapter import DynamoDBBackend
from backend.persistence.models import (
    Episode,
    EpisodeTag,
    Feed,
    Insight,
    Tag,
    User,
    UserEpisodeProgress,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent


def _url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path.resolve()}"


async def _seed_source(url: str) -> None:
    """Seed a source DB: feed -> episodes -> (tags, insights, progress)."""
    backend = SqlAlchemyBackend.from_url("seed", url)
    await backend.init()
    async with backend._session_factory() as s:
        feed = Feed(rss_url="https://example.com/feed.xml", title="Test Feed")
        s.add(feed)
        await s.flush()

        ep1 = Episode(
            feed_id=feed.feed_id,
            guid="ep-1",
            title="Episode One",
            audio_url="https://example.com/1.mp3",
        )
        ep2 = Episode(
            feed_id=feed.feed_id,
            guid="ep-2",
            title="Episode Two",
            audio_url="https://example.com/2.mp3",
        )
        s.add_all([ep1, ep2])
        await s.flush()

        tag = Tag(name="tech", category="topic")
        s.add(tag)
        await s.flush()
        s.add(EpisodeTag(episode_id=ep1.episode_id, tag_id=tag.tag_id))
        s.add(Insight(episode_id=ep1.episode_id, title="Key insight"))

        user = User(email="user@example.com")
        s.add(user)
        await s.flush()
        s.add(
            UserEpisodeProgress(
                user_id=user.user_id,
                episode_id=ep2.episode_id,
                position_seconds=42,
                completed=True,
            )
        )
        await s.commit()
    await backend.close()


async def _counts(url: str) -> dict:
    backend = SqlAlchemyBackend.from_url("probe", url)
    out = {}
    try:
        for t in backend.table_names:
            out[t] = len(await backend.read_table(t))
    finally:
        await backend.close()
    return out


@pytest.mark.asyncio
async def test_migrate_copies_all_tables(tmp_path):
    src, dst = _url(tmp_path / "src.db"), _url(tmp_path / "dst.db")
    await _seed_source(src)

    report = await migrate(
        SqlAlchemyBackend.from_url("simple", src),
        SqlAlchemyBackend.from_url("app", dst),
    )
    by_table = {r.table: r for r in report}

    assert by_table["feeds"].copied_rows == 1
    assert by_table["episodes"].copied_rows == 2
    assert by_table["tags"].copied_rows == 1
    assert by_table["episode_tags"].copied_rows == 1
    assert by_table["insights"].copied_rows == 1
    assert by_table["users"].copied_rows == 1
    assert by_table["user_episode_progress"].copied_rows == 1

    # Values survive the round trip.
    dst_backend = SqlAlchemyBackend.from_url("check", dst)
    try:
        feeds = await dst_backend.read_table("feeds")
        assert feeds[0]["title"] == "Test Feed"
        episodes = await dst_backend.read_table("episodes")
        assert {e["audio_url"] for e in episodes} == {
            "https://example.com/1.mp3",
            "https://example.com/2.mp3",
        }
    finally:
        await dst_backend.close()


@pytest.mark.asyncio
async def test_migrate_is_idempotent(tmp_path):
    src, dst = _url(tmp_path / "src.db"), _url(tmp_path / "dst.db")
    await _seed_source(src)

    await migrate(SqlAlchemyBackend.from_url("s", src), SqlAlchemyBackend.from_url("t", dst))
    first = await _counts(dst)
    await migrate(SqlAlchemyBackend.from_url("s", src), SqlAlchemyBackend.from_url("t", dst))
    second = await _counts(dst)

    assert first == second
    assert second["feeds"] == 1
    assert second["episodes"] == 2


@pytest.mark.asyncio
async def test_dry_run_writes_nothing(tmp_path):
    src = _url(tmp_path / "src.db")
    dst_path = tmp_path / "dst.db"
    await _seed_source(src)

    report = await migrate(
        SqlAlchemyBackend.from_url("s", src),
        SqlAlchemyBackend.from_url("t", _url(dst_path)),
        dry_run=True,
    )

    assert not dst_path.exists()  # target never initialized
    assert all(r.copied_rows == 0 for r in report)
    assert sum(r.source_rows for r in report) == 8  # 1+2+1+1+1+1+1


@pytest.mark.asyncio
async def test_same_backend_refused(tmp_path):
    url = _url(tmp_path / "same.db")
    with pytest.raises(SameBackendError):
        await migrate(
            SqlAlchemyBackend.from_url("s", url), SqlAlchemyBackend.from_url("t", url)
        )


def test_cli_refuses_same_backend_name():
    assert main(["--source", "simple", "--target", "simple"]) == 2


def test_fk_ordering_parents_before_children():
    tables = SqlAlchemyBackend.from_url("x", "sqlite+aiosqlite:///:memory:").table_names

    def before(a, b):
        assert tables.index(a) < tables.index(b), f"{a} must come before {b}"

    before("feeds", "episodes")
    before("episodes", "insights")
    before("episodes", "episode_tags")
    before("tags", "episode_tags")
    before("episodes", "user_episode_progress")
    before("users", "user_episode_progress")
    before("users", "curated_playlists")
    before("curated_playlists", "playlist_episodes")
    before("episodes", "playlist_episodes")


def test_resolve_url_honors_env(monkeypatch):
    monkeypatch.setenv("INGESTION_DATABASE_URL", "sqlite+aiosqlite:////tmp/custom.db")
    assert resolve_url("simple") == "sqlite+aiosqlite:////tmp/custom.db"
    monkeypatch.delenv("INGESTION_DATABASE_URL")
    assert resolve_url("simple").endswith("backend/ingestion/simple.db")
    assert resolve_url("app") == "sqlite+aiosqlite:///./tunedin.db"


def test_resolve_url_dynamodb_raises_value_error():
    # "dynamodb" is in _VALID_BACKENDS but has no URL; it must raise the
    # documented ValueError, not fall through to a KeyError on
    # _BACKEND_MODULES (XIN-132).
    with pytest.raises(ValueError, match="no database URL"):
        resolve_url("dynamodb")


def test_resolve_url_unknown_backend_raises_value_error():
    with pytest.raises(ValueError, match="Unknown backend"):
        resolve_url("nope")


@pytest.mark.asyncio
async def test_write_rows_empty_list_short_circuits(tmp_path):
    backend = SqlAlchemyBackend.from_url("s", _url(tmp_path / "empty.db"))
    try:
        assert await backend.write_rows("feeds", []) == 0
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_write_rows_flushes_in_chunks(tmp_path):
    # More rows than _WRITE_FLUSH_EVERY so the periodic flush+evict path
    # runs at least once (XIN-132 write-side OOM fix); all rows must still
    # land, upserted by primary key.
    from backend.migrate_data import _WRITE_FLUSH_EVERY

    backend = SqlAlchemyBackend.from_url("s", _url(tmp_path / "chunk.db"))
    try:
        await backend.init()
        n = _WRITE_FLUSH_EVERY + 5
        rows = [
            {
                "feed_id": uuid4(),
                "rss_url": f"https://example.com/chunk/{i}.xml",
                "title": f"Chunk {i}",
            }
            for i in range(n)
        ]
        assert await backend.write_rows("feeds", rows) == n
        assert await backend.count_rows("feeds") == n
        titles = {r["title"] for r in await backend.read_table("feeds")}
        assert len(titles) == n
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_count_rows_matches_read_table(tmp_path):
    src = _url(tmp_path / "src.db")
    await _seed_source(src)
    backend = SqlAlchemyBackend.from_url("s", src)
    try:
        assert await backend.count_rows("feeds") == 1
        assert await backend.count_rows("episodes") == 2
        assert await backend.count_rows("tags") == 1
    finally:
        await backend.close()


def test_dynamodb_identity_differs_across_endpoints():
    # Same region/table but different endpoints (moto-local vs real AWS)
    # must NOT share an identity, or the same-database guard misfires
    # (XIN-132).
    local = DynamoDBBackend(
        table_name="t", region_name="us-east-1", endpoint_url="http://localhost:8000"
    )
    aws = DynamoDBBackend(table_name="t", region_name="us-east-1")
    other = DynamoDBBackend(
        table_name="t", region_name="us-east-1", endpoint_url="http://other:8000"
    )
    assert local.identity != aws.identity
    assert local.identity != other.identity
    assert aws.identity != other.identity
    same = DynamoDBBackend(
        table_name="t", region_name="us-east-1", endpoint_url="http://localhost:8000"
    )
    assert same.identity == local.identity


def _run_cli(args, env_overrides, cwd):
    env = dict(os.environ)
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-m", "backend.migrate_data", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.asyncio
async def test_cli_end_to_end(tmp_path):
    """Full CLI via subprocess: exercises the real module-import backend path."""
    src_path, dst_path = tmp_path / "src.db", tmp_path / "dst.db"
    await _seed_source(_url(src_path))
    env = {
        "INGESTION_DATABASE_URL": _url(src_path),
        "DATABASE_URL": _url(dst_path),
    }

    proc = _run_cli(["--source", "simple", "--target", "app"], env, REPO_ROOT)
    assert proc.returncode == 0, proc.stderr
    assert "MIGRATED" in proc.stdout
    assert "feeds" in proc.stdout

    assert await _counts(_url(src_path)) == await _counts(_url(dst_path))


@pytest.mark.asyncio
async def test_cli_dry_run_end_to_end(tmp_path):
    src_path, dst_path = tmp_path / "src.db", tmp_path / "dst.db"
    await _seed_source(_url(src_path))
    env = {
        "INGESTION_DATABASE_URL": _url(src_path),
        "DATABASE_URL": _url(dst_path),
    }

    proc = _run_cli(["--source", "simple", "--target", "app", "--dry-run"], env, REPO_ROOT)
    assert proc.returncode == 0, proc.stderr
    assert "DRY RUN" in proc.stdout
    assert not dst_path.exists()


def test_cli_same_backend_exits_nonzero(tmp_path):
    db_path = tmp_path / "db.db"
    env = {
        "INGESTION_DATABASE_URL": _url(db_path),
        "DATABASE_URL": _url(db_path),
    }
    proc = _run_cli(["--source", "simple", "--target", "simple"], env, REPO_ROOT)
    assert proc.returncode == 2
    assert "refusing" in proc.stderr


def test_get_backend_rejects_unknown():
    with pytest.raises(ValueError):
        get_backend("nope")


def test_get_backend_dynamodb_uses_env_config(monkeypatch):
    monkeypatch.setenv("DATABASE_DYNAMODB_TABLE_NAME", "mytable")
    monkeypatch.setenv("DATABASE_DYNAMODB_REGION", "eu-west-1")
    monkeypatch.setenv("DATABASE_DYNAMODB_ENDPOINT_URL", "http://localhost:8000")
    backend = get_backend("dynamodb")
    assert backend.name == "dynamodb"
    assert backend._table_name == "mytable"
    assert backend._region_name == "eu-west-1"
    assert backend._endpoint_url == "http://localhost:8000"
    assert backend.identity == "dynamodb:eu-west-1:mytable:http://localhost:8000"


def test_get_backend_dynamodb_defaults(monkeypatch):
    for var in (
        "DATABASE_DYNAMODB_TABLE_NAME",
        "DATABASE_DYNAMODB_REGION",
        "DATABASE_DYNAMODB_ENDPOINT_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    backend = get_backend("dynamodb")
    assert backend._table_name == "tunedin"
    assert backend._region_name == "us-east-1"
    assert backend._endpoint_url is None


class _SubsetBackend(SqlAlchemyBackend):
    """Target backend exposing only a subset of tables."""

    @property
    def table_names(self):
        return ["feeds", "episodes"]


@pytest.mark.asyncio
async def test_skipped_tables_warned_and_reported(tmp_path, capsys):
    src = _url(tmp_path / "src.db")
    dst = _url(tmp_path / "dst.db")
    await _seed_source(src)

    report = await migrate(
        SqlAlchemyBackend.from_url("s", src),
        _SubsetBackend.from_url("t", dst),
    )
    by_table = {r.table: r for r in report}

    assert by_table["feeds"].skipped is False
    assert by_table["feeds"].copied_rows == 1
    assert by_table["tags"].skipped is True
    assert by_table["tags"].copied_rows == 0
    # Skipped tables get their count from a count-only read -- the rows are
    # never loaded (XIN-132 nit: skip check moved before read_table).
    assert by_table["tags"].source_rows == 1
    assert by_table["insights"].skipped is True
    assert by_table["insights"].source_rows == 1

    err = capsys.readouterr().err
    assert "skipping table 'tags'" in err
    assert "skipping table 'insights'" in err
    assert "skipping table 'episodes'" not in err
    assert "skipping table 'feeds'" not in err


def test_assert_distinct_backends(tmp_path):
    a = SqlAlchemyBackend.from_url("a", _url(tmp_path / "a.db"))
    b = SqlAlchemyBackend.from_url("b", _url(tmp_path / "b.db"))
    assert_distinct_backends(a, b)  # distinct: no raise
    same = SqlAlchemyBackend.from_url("c", _url(tmp_path / "a.db"))
    with pytest.raises(SameBackendError, match="refusing"):
        assert_distinct_backends(a, same)


def test_reconcile_tables_partitions_and_orders():
    common, source_only, target_only = reconcile_tables(
        ["feeds", "episodes", "tags", "ghost"],
        ["tags", "feeds", "episodes", "leftover_b", "leftover_a"],
    )
    # common/source_only keep source order; target_only is sorted.
    assert common == ["feeds", "episodes", "tags"]
    assert source_only == ["ghost"]
    assert target_only == ["leftover_a", "leftover_b"]


def test_reconcile_tables_identical_sets():
    common, source_only, target_only = reconcile_tables(["a", "b"], ["b", "a"])
    assert (common, source_only, target_only) == (["a", "b"], [], [])


@pytest.mark.asyncio
async def test_migrate_same_backend_error_message_names_both(tmp_path, monkeypatch):
    url = _url(tmp_path / "same.db")
    with pytest.raises(SameBackendError, match="refusing"):
        await migrate(
            SqlAlchemyBackend.from_url("s", url), SqlAlchemyBackend.from_url("t", url)
        )
    monkeypatch.chdir(tmp_path)
    relative = _normalize_url("sqlite+aiosqlite:///./same.db")
    absolute = _normalize_url(f"sqlite+aiosqlite:///{tmp_path}/same.db")
    assert relative == absolute
    assert relative.endswith("/same.db")
    # Non-sqlite URLs pass through untouched.
    assert _normalize_url("postgresql://u@h/db") == "postgresql://u@h/db"
    assert _normalize_url("sqlite+aiosqlite:///:memory:") == (
        "sqlite+aiosqlite:///:memory:"
    )


@pytest.mark.asyncio
async def test_same_database_relative_vs_absolute_spelling_refused(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    relative_url = "sqlite+aiosqlite:///./same.db"
    absolute_url = f"sqlite+aiosqlite:///{tmp_path / 'same.db'}"
    assert relative_url != absolute_url  # different spellings, same file
    with pytest.raises(SameBackendError):
        await migrate(
            SqlAlchemyBackend.from_url("s", relative_url),
            SqlAlchemyBackend.from_url("t", absolute_url),
        )


@pytest.mark.asyncio
async def test_read_table_chunks_streams_in_bounded_chunks(tmp_path):
    """read_table_chunks yields chunk_size-bounded chunks covering every
    row — the read-side OOM fix for hundred-thousand-row tables."""
    url = _url(tmp_path / "src.db")
    await _seed_source(url)
    backend = SqlAlchemyBackend.from_url("seed", url)
    try:
        chunks = [
            c
            async for c in backend.read_table_chunks("episodes", chunk_size=1)
        ]
        assert len(chunks) == 2
        assert all(len(c) == 1 for c in chunks)
        titles = {r["title"] for c in chunks for r in c}
        assert titles == {"Episode One", "Episode Two"}

        # A single chunk covers a small table; rows match read_table.
        single = [
            c async for c in backend.read_table_chunks("feeds", chunk_size=5000)
        ]
        assert len(single) == 1
        assert single[0] == await backend.read_table("feeds")
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_migrate_writes_in_chunks(tmp_path):
    """migrate streams the source: write_rows is called per chunk and the
    report counts still reconcile."""
    src, dst = _url(tmp_path / "src.db"), _url(tmp_path / "dst.db")
    await _seed_source(src)
    source = SqlAlchemyBackend.from_url("simple", src)
    target = SqlAlchemyBackend.from_url("app", dst)
    calls: list[tuple[str, int]] = []
    real_write = target.write_rows

    async def counting_write(table_name, rows):
        calls.append((table_name, len(rows)))
        return await real_write(table_name, rows)

    target.write_rows = counting_write
    try:
        report = await migrate(source, target)
    finally:
        await source.close()
        await target.close()

    by_table = {r.table: r for r in report}
    assert by_table["episodes"].copied_rows == 2
    assert by_table["episodes"].source_rows == 2
    ep_calls = [n for t, n in calls if t == "episodes"]
    assert sum(ep_calls) == 2
    assert all(n <= 5000 for n in ep_calls)
