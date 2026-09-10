"""Tests for backend.insights.episode_type (XIN-134)."""

from backend.insights.episode_type import (
    BONUS,
    FULL,
    PREVIEW,
    REBROADCAST,
    TRAILER,
    classify_episode_type,
    is_trailer_like,
)


def test_metadata_trailer_wins():
    assert classify_episode_type("A normal title", 3600, episode_type_meta="trailer") == TRAILER


def test_metadata_bonus_wins():
    assert classify_episode_type("A normal title", 3600, episode_type_meta="bonus") == BONUS


def test_short_trailer_title():
    assert classify_episode_type("Show Trailer", 90) == TRAILER


def test_trailer_title_unknown_duration_trusts_title():
    assert classify_episode_type("Season 2 Trailer") == TRAILER


def test_episode_about_trailers_is_full():
    # 60-minute substantive episode about movie trailers, not a trailer.
    assert classify_episode_type("Movie Trailer Breakdown", 3600) == FULL


def test_trailer_with_promo_notes():
    assert (
        classify_episode_type(
            "Coming soon", 1500, notes="Subscribe now and don't miss the premiere!"
        )
        == TRAILER
    )


def test_preview_title():
    assert classify_episode_type("Preview: next week's episode", 120) == PREVIEW


def test_bonus_episode_label():
    assert classify_episode_type("Bonus Episode: holiday mailbag", 2700) == BONUS


def test_bonus_in_brackets():
    assert classify_episode_type("Holiday mailbag (Bonus)", 2700) == BONUS


def test_incidental_bonus_word_is_full():
    # "The Bonus Army" is a history episode; bare "bonus" is not a label.
    assert classify_episode_type("The Bonus Army", 3600) == FULL


def test_rebroadcast_patterns():
    assert classify_episode_type("From the vault: our 2019 interview", 3600) == REBROADCAST
    assert classify_episode_type("Best of 2025", 3600) == REBROADCAST
    assert classify_episode_type("Encore: the pilot episode", 1800) == REBROADCAST


def test_short_daily_brief_is_full():
    # Under 5 minutes alone is not enough: daily news briefs are full episodes.
    assert classify_episode_type("Morning news briefing", 150) == FULL


def test_plain_episode_is_full():
    assert classify_episode_type("Why do empires collapse?", 3200) == FULL


def test_none_inputs_default_full():
    assert classify_episode_type(None) == FULL


def test_is_trailer_like():
    assert is_trailer_like("Show Trailer", 90)
    assert is_trailer_like("Bonus Episode: mailbag", 2700)
    assert is_trailer_like("Preview: next week", 120)
    assert not is_trailer_like("Why do empires collapse?", 3200)
    assert not is_trailer_like("Morning news briefing", 150)
