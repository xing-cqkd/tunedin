from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Set


@dataclass
class Podcast:
    """
    Provider-agnostic domain model representing a discovered podcast show.

    Core fields are provider-neutral. The generic ``provider`` /
    ``provider_id`` / ``external_url`` triple records where the show was
    discovered (e.g. provider="itunes", provider_id=<Apple collection id>,
    external_url=<Apple Podcasts page>) without naming any provider in the
    domain type itself. Provider-specific parsing lives in the provider
    module (see ``backend/ingestion/itunes.py::podcast_from_itunes``);
    a new provider adds a new parser, not a new field here.
    """
    title: str
    feed_url: str
    author: Optional[str] = None
    description: Optional[str] = None
    artwork_url: Optional[str] = None
    primary_genre: Optional[str] = None
    genres: List[str] = field(default_factory=list)
    episode_count: Optional[int] = None
    country: Optional[str] = None
    language: Optional[str] = None
    website_url: Optional[str] = None
    release_date: Optional[datetime] = None
    provider: str = "itunes"
    provider_id: Optional[str] = None
    # Provider-neutral link to the show's page on the discovery provider's
    # site (e.g. the Apple Podcasts page for provider="itunes").
    external_url: Optional[str] = None


@dataclass
class PodcastSearchResult:
    """Represents the standardized result of a podcast discovery search."""
    query: str
    count: int
    podcasts: List[Podcast] = field(default_factory=list)
    provider: str = "itunes"


# Backward compatibility alias
ITunesPodcast = Podcast


@dataclass
class ParsedFeedMetadata:
    """Represents podcast feed level metadata parsed from RSS/XML."""
    title: str
    rss_url: str
    author: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[str] = None
    category: Optional[str] = None
    link: Optional[str] = None
    language: Optional[str] = None
    website_url: Optional[str] = None
    feed_type: Optional[str] = None          # episodic vs serial
    podcast_guid: Optional[str] = None
    explicit: Optional[bool] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None


@dataclass
class ParsedEpisode:
    """Represents an individual podcast episode parsed from an RSS enclosure/item."""
    guid: str
    title: str
    audio_url: str
    duration: Optional[int] = None           # Duration in total seconds
    published_at: Optional[datetime] = None  # Normalized to timezone-aware UTC datetime
    summary: Optional[str] = None
    content_html: Optional[str] = None       # Full HTML shownotes (<content:encoded>)
    enclosure_type: Optional[str] = None
    link: Optional[str] = None
    transcript_url: Optional[str] = None     # Podcasting 2.0 transcript (<podcast:transcript>)
    chapters_url: Optional[str] = None       # Podcasting 2.0 chapters (<podcast:chapters>)
    image_url: Optional[str] = None          # Episode-level cover artwork
    episode_type: Optional[str] = "full"     # full, trailer, bonus (<itunes:episodeType>)
    episode_number: Optional[int] = None     # <itunes:episode>
    season_number: Optional[int] = None      # <itunes:season>
    explicit: Optional[bool] = None          # <itunes:explicit>


@dataclass
class FeedParseResult:
    """Result of parsing an RSS feed, with metadata and filtered new episodes."""
    metadata: ParsedFeedMetadata
    episodes: List[ParsedEpisode] = field(default_factory=list)
    total_feed_episodes: int = 0
    is_not_modified: bool = False
