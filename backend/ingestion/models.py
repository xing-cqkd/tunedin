from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Set


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
    enclosure_type: Optional[str] = None
    link: Optional[str] = None


@dataclass
class FeedParseResult:
    """Result of parsing an RSS feed, with metadata and filtered new episodes."""
    metadata: ParsedFeedMetadata
    episodes: List[ParsedEpisode] = field(default_factory=list)
    total_feed_episodes: int = 0
    is_not_modified: bool = False
