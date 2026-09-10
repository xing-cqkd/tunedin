"""Unit tests for the DynamoDB key builders (Linear: XIN-89)."""

import hashlib
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from backend.persistence.dynamodb import keys
from backend.persistence.dynamodb.keys import (
    MISSING_TS_MIN,
    episode_keys,
    episode_tag_link_keys,
    feed_keys,
    guid_marker_keys,
    insight_keys,
    iso_timestamp,
    normalize_email,
    playlist_episode_link_keys,
    playlist_keys,
    progress_keys,
    sanitize_str,
    sha256_hex,
    tag_keys,
    tag_natural_key_hash,
    task_log_keys,
    user_keys,
    uuid_from_str,
    uuid_str,
)

AWARE = datetime(2026, 9, 9, 14, 41, 0, tzinfo=timezone.utc)
NAIVE = datetime(2026, 9, 9, 14, 41, 0)  # same instant, no tzinfo


class TestIsoTimestamp:
    def test_aware_datetime_encoded_verbatim(self):
        assert iso_timestamp(AWARE) == "2026-09-09T14:41:00+00:00"

    def test_naive_datetime_normalized_to_utc(self):
        # The critical correctness rule: a naive timestamp must never sort
        # after its aware equivalent.
        assert iso_timestamp(NAIVE) == "2026-09-09T14:41:00+00:00"

    def test_naive_never_sorts_after_aware(self):
        assert iso_timestamp(NAIVE) <= iso_timestamp(AWARE)

    def test_none_uses_min_sentinel_by_default(self):
        assert iso_timestamp(None) == MISSING_TS_MIN
        assert MISSING_TS_MIN < "2026-01-01T00:00:00+00:00"

    def test_non_utc_offset_normalized_to_utc(self):
        # A +02:00 aware datetime must encode as the same instant in UTC so
        # the same timestamp always sorts the same way regardless of the
        # offset it was constructed with.
        tz_plus2 = timezone(timedelta(hours=2))
        dt = datetime(2026, 9, 9, 16, 41, 0, tzinfo=tz_plus2)
        assert iso_timestamp(dt) == "2026-09-09T14:41:00+00:00"


class TestSanitizeStr:
    def test_empty_string_becomes_none(self):
        assert sanitize_str("") is None

    def test_none_stays_none(self):
        assert sanitize_str(None) is None

    def test_non_empty_preserved(self):
        assert sanitize_str("hello") == "hello"


class TestUuidHelpers:
    def test_round_trip(self):
        uid = uuid4()
        assert uuid_from_str(uuid_str(uid)) == uid

    def test_str_form(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        assert uuid_str(uid) == "12345678-1234-5678-1234-567812345678"


class TestSha256Hex:
    def test_matches_hashlib(self):
        assert sha256_hex("abc") == hashlib.sha256(b"abc").hexdigest()

    def test_is_64_hex_chars(self):
        h = sha256_hex("https://example.com/feed.xml")
        assert len(h) == 64 and all(c in "0123456789abcdef" for c in h)


class TestFeedKeys:
    def test_main_keys(self):
        fid = uuid4()
        k = feed_keys(
            fid,
            rss_url="https://example.com/feed.xml",
            sync_status="pending",
            created_at=AWARE,
        )
        assert k["pk"] == f"FEED#{fid}"
        assert k["sk"] == "META"

    def test_gsi1_rss_url_hashed(self):
        url = "https://example.com/feed.xml"
        k = feed_keys(uuid4(), rss_url=url, sync_status="pending",
                      created_at=AWARE)
        assert k["gsi1pk"] == f"URL#{hashlib.sha256(url.encode()).hexdigest()}"
        assert k["gsi1sk"] == "META"

    def test_gsi2_status_scan(self):
        k = feed_keys(uuid4(), rss_url="u", sync_status="error",
                      created_at=AWARE)
        assert k["gsi2pk"] == "STATUS#error"
        assert k["gsi2sk"] == "2026-09-09T14:41:00+00:00"


class TestEpisodeKeys:
    def test_main_keys(self):
        eid, fid = uuid4(), uuid4()
        k = episode_keys(eid, feed_id=fid, published_at=AWARE, processed=True)
        assert k["pk"] == f"FEED#{fid}"
        assert k["sk"] == f"EP#2026-09-09T14:41:00+00:00#{eid}"
        assert k["gsi1pk"] == f"EP#{eid}"
        assert k["gsi1sk"] == "META"

    def test_unprocessed_gets_sparse_gsi3(self):
        eid, fid = uuid4(), uuid4()
        k = episode_keys(eid, feed_id=fid, published_at=AWARE, processed=False)
        assert k["gsi3pk"] == "UNPROCESSED"
        assert k["gsi3sk"] == f"EP#2026-09-09T14:41:00+00:00#{fid}#{eid}"

    def test_processed_has_no_gsi3(self):
        k = episode_keys(uuid4(), feed_id=uuid4(), published_at=AWARE,
                         processed=True)
        assert "gsi3pk" not in k and "gsi3sk" not in k

    def test_missing_published_at_uses_min_sentinel(self):
        # Nulls-last under a descending scan: the sentinel sorts last.
        eid = uuid4()
        k = episode_keys(eid, feed_id=uuid4(), published_at=None,
                         processed=False)
        assert k["sk"] == f"EP#{MISSING_TS_MIN}#{eid}"
        assert MISSING_TS_MIN in k["gsi3sk"]


class TestGuidMarkerKeys:
    def test_keys(self):
        fid = uuid4()
        k = guid_marker_keys(fid, "episode-guid-123")
        assert k == {"pk": f"FEED#{fid}", "sk": "GUID#episode-guid-123"}
        # No GSI attributes: markers are addressed directly by pk/sk.
        assert "gsi1pk" not in k


class TestInsightKeys:
    def test_keys(self):
        iid, eid = uuid4(), uuid4()
        k = insight_keys(iid, episode_id=eid, created_at=AWARE)
        assert k["pk"] == f"EP#{eid}"
        assert k["sk"] == f"INSIGHT#2026-09-09T14:41:00+00:00#{iid}"


class TestLinkKeys:
    def test_episode_tag_link(self):
        eid, tid = uuid4(), uuid4()
        k = episode_tag_link_keys(eid, tid)
        assert k == {"pk": f"EP#{eid}", "sk": f"TAGLINK#{tid}"}

    def test_playlist_episode_link(self):
        pid, eid = uuid4(), uuid4()
        k = playlist_episode_link_keys(pid, eid)
        assert k == {"pk": f"PL#{pid}", "sk": f"PLEP#{eid}"}

    def test_progress(self):
        uid, eid = uuid4(), uuid4()
        k = progress_keys(uid, eid)
        assert k == {"pk": f"USER#{uid}", "sk": f"PROG#{eid}"}


class TestTagKeys:
    def test_main_keys(self):
        tid = uuid4()
        k = tag_keys(tid, name="Sci-Fi", category="genre")
        assert k["pk"] == f"TAG#{tid}"
        assert k["sk"] == "META"

    def test_natural_key_hashed_and_case_insensitive(self):
        k1 = tag_keys(uuid4(), name="Sci-Fi", category="Genre")
        k2 = tag_keys(uuid4(), name="sci-fi", category="genre")
        assert k1["gsi1pk"] == k2["gsi1pk"]
        assert k1["gsi1pk"].startswith("TAGNAME#")
        assert k1["gsi1sk"] == "META"

    def test_none_category_handled(self):
        k = tag_keys(uuid4(), name="x", category=None)
        assert k["gsi1pk"].startswith("TAGNAME#")


class TestTagNaturalKeyHash:
    def test_exact_case_distinguished(self):
        # The claim key must be case-SENSITIVE (XIN-124): "Foo" and "FOO"
        # are distinct claims, matching the SQL unique constraint.
        h1 = tag_natural_key_hash("Foo", "topic")
        h2 = tag_natural_key_hash("FOO", "topic")
        assert h1 != h2
        assert h1.startswith("TAGNAME#")

    def test_differs_from_lowercased_lookup_key(self):
        # The gsi1 lookup key stays case-insensitive; the claim hash must
        # not be the same normalization.
        lookup = tag_keys(uuid4(), name="Foo", category="Topic")["gsi1pk"]
        claim_hash = tag_natural_key_hash("Foo", "Topic")
        assert lookup != claim_hash

    def test_none_category_handled(self):
        h = tag_natural_key_hash("x", None)
        assert h.startswith("TAGNAME#")


class TestUserKeys:
    def test_main_keys(self):
        uid = uuid4()
        k = user_keys(uid, email="User@Example.com ")
        assert k["pk"] == f"USER#{uid}"
        assert k["sk"] == "META"

    def test_email_hashed_and_normalized(self):
        k1 = user_keys(uuid4(), email="User@Example.com ")
        k2 = user_keys(uuid4(), email="user@example.com")
        assert k1["gsi1pk"] == k2["gsi1pk"]
        expected = f"EMAIL#{hashlib.sha256(b'user@example.com').hexdigest()}"
        assert k1["gsi1pk"] == expected

    def test_normalize_email(self):
        assert normalize_email("  FOO@Bar.COM\n") == "foo@bar.com"


class TestPlaylistKeys:
    def test_keys(self):
        pid, uid = uuid4(), uuid4()
        k = playlist_keys(pid, user_id=uid, created_at=AWARE)
        assert k["pk"] == f"USER#{uid}"
        assert k["sk"] == f"PL#2026-09-09T14:41:00+00:00#{pid}"
        assert k["gsi1pk"] == f"PL#{pid}"
        assert k["gsi1sk"] == "META"


class TestTaskLogKeys:
    def test_keys(self):
        tid = uuid4()
        k = task_log_keys(tid, task_type="sync", status="pending",
                          created_at=AWARE)
        assert k["pk"] == f"TASK#{tid}"
        assert k["sk"] == "META"
        assert k["gsi2pk"] == "TASKTYPE#sync#pending"
        assert k["gsi2sk"] == "2026-09-09T14:41:00+00:00"
