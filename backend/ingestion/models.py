from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set


@dataclass
class Podcast:
    """
    Provider-agnostic domain model representing a discovered podcast show.
    Core fields are provider-neutral, with provider-specific attributes
    (such as iTunes / Apple Podcasts, Podcast Index, Spotify) stored as optional fields.
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

    # Provider-specific metadata (iTunes / Apple Podcasts)
    itunes_id: Optional[int] = None
    itunes_url: Optional[str] = None

    @classmethod
    def from_itunes(cls, data: Dict[str, Any]) -> "Podcast":
        """Factory method to construct a Podcast instance from iTunes Search/Lookup API JSON."""
        collection_id = data.get("collectionId") or data.get("trackId")
        title = data.get("collectionName") or data.get("trackName") or "Untitled Show"
        feed_url = data.get("feedUrl", "")
        author = data.get("artistName")
        artwork_url = data.get("artworkUrl600") or data.get("artworkUrl100")
        primary_genre = data.get("primaryGenreName")
        genres = data.get("genres", [])
        if isinstance(genres, list):
            genres_list = [str(g) for g in genres]
        else:
            genres_list = [str(genres)] if genres else []

        episode_count = data.get("trackCount")
        country = data.get("country")
        itunes_url = data.get("collectionViewUrl") or data.get("trackViewUrl")

        release_date = None
        raw_date = data.get("releaseDate")
        if raw_date:
            try:
                from dateutil import parser as dt_parser
                release_date = dt_parser.parse(raw_date)
            except Exception:
                pass

        return cls(
            title=title,
            feed_url=feed_url,
            author=author,
            artwork_url=artwork_url,
            primary_genre=primary_genre,
            genres=genres_list,
            episode_count=episode_count,
            country=country,
            release_date=release_date,
            provider="itunes",
            provider_id=str(collection_id) if collection_id is not None else None,
            itunes_id=int(collection_id) if collection_id is not None else None,
            itunes_url=itunes_url,
        )


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
