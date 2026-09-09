"""Protocol tests for backend/persistence/repositories.py (XIN-84).

These tests pin the Store/repository ABC surface: the exact method set per
aggregate, each method's signature (parameter names, kinds, defaults), and the
requirement that every abstract method carries a docstring stating its
ordering/limit semantics. They exist to catch signature drift as the concrete
stores (SQLAlchemyStore, DynamoDBStore) are built against this protocol.
"""

import inspect

import pytest

from backend.persistence import repositories as repos


# ---------------------------------------------------------------------------
# Expected surface: class -> {method: [(param_name, kind, default), ...]}
# ``default`` is the sentinel _NODEFAULT when the parameter has no default.
# ---------------------------------------------------------------------------

_NODEFAULT = object()

_KWONLY = inspect.Parameter.KEYWORD_ONLY
_POS_OR_KW = inspect.Parameter.POSITIONAL_OR_KEYWORD


def _sig(*params):
    # params: (name,) | (name, kind) | (name, kind, default)
    out = []
    for p in params:
        name = p[0]
        kind = p[1] if len(p) > 1 else _POS_OR_KW
        default = p[2] if len(p) > 2 else _NODEFAULT
        out.append((name, kind, default))
    return out


EXPECTED = {
    "FeedRepository": {
        "get_by_id": _sig(("feed_id",)),
        "get_by_rss_url": _sig(("rss_url",)),
        "list_by_statuses": _sig(("statuses",), ("limit", _KWONLY, None)),
        "list_error_due_retry": _sig(("cutoff",), ("max_attempts",)),
        "save": _sig(("feed",)),
        "count_by_status": _sig(("status",)),
        "count_by_statuses": _sig(("statuses",)),
        "count_all": _sig(),
        "list_all": _sig(("limit", _KWONLY, None)),
    },
    "EpisodeRepository": {
        "get_by_id": _sig(("episode_id",)),
        "list_guids_by_feed": _sig(("feed_id",)),
        "list_episodes_by_feed": _sig(("feed_id",)),
        "list_unprocessed": _sig(
            ("feed_id", _KWONLY, None), ("limit", _KWONLY, 50)
        ),
        "save": _sig(("episode",)),
        "save_many": _sig(("episodes",)),
        "mark_processed": _sig(("episode_id",), ("processed", _POS_OR_KW, True)),
        "count_all": _sig(),
        "count_unprocessed": _sig(),
    },
    "InsightRepository": {
        "list_by_episode": _sig(("episode_id",)),
        "save": _sig(("insight",)),
        "save_many": _sig(("insights",)),
    },
    "TagRepository": {
        "get_by_name_category": _sig(("name",), ("category",)),
        "get_or_create": _sig(("name",), ("category",)),
        "add_episode_tag": _sig(("episode_id",), ("tag_id",)),
        "list_tags_for_episode": _sig(("episode_id",)),
    },
    "UserRepository": {
        "get_by_id": _sig(("user_id",)),
        "get_by_email": _sig(("email",)),
        "save": _sig(("user",)),
    },
    "PlaylistRepository": {
        "list_by_user": _sig(("user_id",)),
        "get_by_id": _sig(("playlist_id",)),
        "save": _sig(("playlist",)),
        "add_episode": _sig(("playlist_id",), ("episode_id",), ("position",)),
        "list_episodes": _sig(("playlist_id",)),
    },
    "ProgressRepository": {
        "get": _sig(("user_id",), ("episode_id",)),
        "save": _sig(("progress",)),
    },
    "TaskLogRepository": {
        "save": _sig(("task_log",)),
        "list_by_type_status": _sig(
            ("task_type",), ("status",), ("limit", _POS_OR_KW, None)
        ),
        "update_status": _sig(
            ("task_log_id",), ("status",), ("error_message", _POS_OR_KW, None)
        ),
    },
}

STORE_PROPERTIES = (
    "feeds",
    "episodes",
    "insights",
    "tags",
    "users",
    "playlists",
    "progress",
    "task_logs",
)

STORE_METHODS = {
    "commit": _sig(),
    "rollback": _sig(),
    "close": _sig(),
}

REPO_CLASS_BY_PROPERTY = {
    "feeds": "FeedRepository",
    "episodes": "EpisodeRepository",
    "insights": "InsightRepository",
    "tags": "TagRepository",
    "users": "UserRepository",
    "playlists": "PlaylistRepository",
    "progress": "ProgressRepository",
    "task_logs": "TaskLogRepository",
}


def _check_signature(fn, expected_params):
    sig = inspect.signature(fn)
    actual = [
        (p.name, p.kind, _NODEFAULT if p.default is inspect.Parameter.empty else p.default)
        for p in sig.parameters.values()
        if p.name != "self"
    ]
    assert actual == expected_params, (
        f"{fn.__qualname__}: signature drift.\n"
        f"  expected: {expected_params}\n"
        f"  actual:   {actual}"
    )


# ---------------------------------------------------------------------------
# Surface tests
# ---------------------------------------------------------------------------


def test_all_expected_classes_exist():
    for name in list(EXPECTED) + ["Store"]:
        assert hasattr(repos, name), f"backend.persistence.repositories.{name} missing"
        assert inspect.isclass(getattr(repos, name))


def test_repository_method_sets_match_catalog_exactly():
    """No missing methods, and no invented ones beyond the plan catalog."""
    for class_name, methods in EXPECTED.items():
        cls = getattr(repos, class_name)
        actual = set(cls.__abstractmethods__)
        assert actual == set(methods), (
            f"{class_name}: method set drift.\n"
            f"  missing: {sorted(set(methods) - actual)}\n"
            f"  extra:   {sorted(set(actual) - set(methods))}"
        )


def test_repository_method_signatures():
    for class_name, methods in EXPECTED.items():
        cls = getattr(repos, class_name)
        for method_name, expected_params in methods.items():
            fn = getattr(cls, method_name)
            assert inspect.iscoroutinefunction(fn), (
                f"{class_name}.{method_name} must be async"
            )
            _check_signature(fn, expected_params)


def test_every_abstract_method_has_docstring():
    for class_name, methods in EXPECTED.items():
        cls = getattr(repos, class_name)
        for method_name in methods:
            doc = getattr(cls, method_name).__doc__
            assert doc and doc.strip(), (
                f"{class_name}.{method_name} must document its "
                "ordering/limit semantics"
            )
    for method_name in STORE_METHODS:
        doc = getattr(repos.Store, method_name).__doc__
        assert doc and doc.strip(), f"Store.{method_name} must have a docstring"


def test_abcs_cannot_be_instantiated():
    for class_name in list(EXPECTED) + ["Store"]:
        with pytest.raises(TypeError):
            getattr(repos, class_name)()


def test_store_exposes_all_repository_properties():
    props = {
        name for name, value in inspect.getmembers(repos.Store)
        if isinstance(value, property)
    }
    assert set(STORE_PROPERTIES) <= props
    for name in ("commit", "rollback", "close"):
        assert name in repos.Store.__abstractmethods__


def test_store_is_async_context_manager():
    assert inspect.iscoroutinefunction(repos.Store.__aenter__)
    assert inspect.iscoroutinefunction(repos.Store.__aexit__)


# ---------------------------------------------------------------------------
# Stub implementation: proves the ABCs are implementable and exercises the
# async context-manager commit/rollback behavior defined on Store.
# ---------------------------------------------------------------------------


def _make_repo_stub(repo_cls):
    namespace = {}
    for name in repo_cls.__abstractmethods__:

        async def _method(self, *args, **kwargs):
            return None

        _method.__name__ = name
        namespace[name] = _method
    return type(f"{repo_cls.__name__}Stub", (repo_cls,), namespace)


def _make_store_stub():
    async def _commit(self):
        self.committed += 1

    async def _rollback(self):
        self.rolled_back += 1

    async def _close(self):
        return None

    def _sync_init(self):
        self._repos = {
            prop: _make_repo_stub(getattr(repos, cls_name))()
            for prop, cls_name in REPO_CLASS_BY_PROPERTY.items()
        }
        self.committed = 0
        self.rolled_back = 0

    namespace = {
        "__init__": _sync_init,
        "commit": _commit,
        "rollback": _rollback,
        "close": _close,
    }
    for prop in STORE_PROPERTIES:
        namespace[prop] = property(
            lambda self, _p=prop: self._repos[_p]
        )
    return type("StoreStub", (repos.Store,), namespace)


async def test_stub_store_implements_protocol():
    store = _make_store_stub()()
    assert store.__abstractmethods__ == frozenset()
    for prop, cls_name in REPO_CLASS_BY_PROPERTY.items():
        repo = getattr(store, prop)
        assert isinstance(repo, getattr(repos, cls_name)), prop


async def test_store_context_manager_commits_on_clean_exit():
    store = _make_store_stub()()
    async with store as s:
        assert s is store
    assert getattr(store, "committed", 0) == 1
    assert getattr(store, "rolled_back", 0) == 0


async def test_store_context_manager_rolls_back_on_exception():
    store = _make_store_stub()()

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with store:
            raise Boom()
    assert getattr(store, "committed", 0) == 0
    assert getattr(store, "rolled_back", 0) == 1
