"""Tests for backend.migrate_data (cross-backend data migration CLI)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from backend.migrate_data import (
    SameBackendError,
    SqlAlchemyBackend,
    _normalize_url,
    get_backend,
    main,
    migrate,
    resolve_url,
)
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
    assert backend.identity == "dynamodb:eu-west-1:mytable"


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
    assert by_table["insights"].skipped is True

    err = capsys.readouterr().err
    assert "skipping table 'tags'" in err
    assert "skipping table 'insights'" in err
    assert "skipping table 'episodes'" not in err
    assert "skipping table 'feeds'" not in err


def test_normalize_url_unifies_sqlite_spellings(tmp_path, monkeypatch):
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
