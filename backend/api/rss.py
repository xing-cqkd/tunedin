"""RSS 2.0 + iTunes feed rendering for published curated playlists (XIN-98).

iTunes tag fallback policy (all choices documented here; the endpoint never
fails to render for missing metadata):
  * ``itunes:author`` (channel): the ``User`` model has no display name, so
    this falls back to the static string ``"TuneIn curator"`` rather than
    leaking the curator's email local-part into a shareable feed. A future
    ``User.display_name`` column is the real fix (noted for XIN-99).
  * ``itunes:summary`` (channel): playlist description, else playlist title.
  * ``itunes:image`` (channel): first entry episode with an ``image_url``;
    omitted entirely when no episode has artwork.
  * ``itunes:explicit`` (channel): ``"yes"`` if ANY episode is flagged
    explicit, else ``"no"`` (conservative: one explicit episode taints the
    feed so clients filter correctly).
  * ``itunes:duration`` (item): ``episode.duration`` seconds rendered as
    ``H:MM:SS``; omitted when unknown.
  * ``itunes:explicit`` (item): ``"yes"``/``"no"`` from ``episode.explicit``;
    ``None`` (unknown) is treated as ``"no"`` and documented here — the
    channel-level tag still guards clients when any episode IS explicit.
  * ``itunes:summary`` / ``itunes:image`` (item): episode summary / artwork;
    omitted when absent.
  * ``<description>`` (item): always carries the episode's ORIGINAL publish
    date (``"Originally published: <RFC-2822>."``) because ``<pubDate>`` is
    reserved for the added-to-playlist date per the product spec.
  * ``<enclosure>``: points at the publisher's ORIGINAL ``audio_url``
    (redirect, never proxy — v1 content-rights decision).
    ``length="0"`` because the byte length is unknown without fetching the
    audio (HEAD checks are XIN-101's job); ``type`` is guessed from the URL
    extension with an ``audio/mpeg`` fallback.
  * ``<guid isPermaLink="false">``: ``tunedin:{playlist_id}:{episode_id}`` —
    per-playlist, so the same episode in two feeds is distinct to clients.
    The original episode GUID rides along in ``<tunedin:sourceGuid>`` for
    future smart-client dedupe (omitted when the episode has no GUID).
"""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime
from os.path import splitext
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

if TYPE_CHECKING:  # model classes are attribute bags here; no runtime import
    from backend.persistence.models import CuratedPlaylist
    from backend.persistence.repositories import PlaylistEpisodeEntry

ITUNES_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"
TUNEDIN_NS = "https://tunedin.app/ns/1.0"

ET.register_namespace("itunes", ITUNES_NS)
ET.register_namespace("tunedin", TUNEDIN_NS)

_FALLBACK_AUTHOR = "TuneIn curator"

_AUDIO_TYPES = {
    ".mp3": "audio/mpeg",
    ".m4a": "audio/x-m4a",
    ".mp4": "audio/mp4",
    ".m4b": "audio/x-m4b",
    ".ogg": "audio/ogg",
    ".oga": "audio/ogg",
    ".opus": "audio/ogg",
    ".wav": "audio/wav",
    ".aac": "audio/aac",
}


def ensure_aware(dt: datetime | None) -> datetime | None:
    """Normalize a datetime to tz-aware UTC (SQLite returns naive)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def rfc2822(dt: datetime) -> str:
    """Format a datetime as an RFC-2822 date (RSS ``pubDate``)."""
    return format_datetime(ensure_aware(dt))


def _enclosure_type(audio_url: str) -> str:
    path = urlsplit(audio_url).path.lower()
    return _AUDIO_TYPES.get(splitext(path)[1], "audio/mpeg")


def _itunes_duration(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


def _sub(parent: ET.Element, tag: str, text: str | None) -> None:
    if text:
        ET.SubElement(parent, tag).text = text


def build_rss(
    playlist: CuratedPlaylist,
    entries: list[PlaylistEpisodeEntry],    *,
    page_url: str,
    feed_url: str,
) -> bytes:
    """Render the RSS 2.0 + iTunes feed for a published playlist.

    ``entries`` must already be in curator position order (the repository
    guarantees ``position`` ascending, ``episode_id`` ascending on ties).
    Returns the UTF-8 XML document bytes.
    """
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = playlist.title
    ET.SubElement(channel, "link").text = page_url
    description = playlist.description or (
        f"{playlist.title} — a curated podcast feed from TuneIn"
    )
    ET.SubElement(channel, "description").text = description
    ET.SubElement(channel, "language").text = "en-us"

    # iTunes channel tags (fallbacks documented in the module docstring).
    ET.SubElement(channel, f"{{{ITUNES_NS}}}author").text = _FALLBACK_AUTHOR
    ET.SubElement(channel, f"{{{ITUNES_NS}}}summary").text = (
        playlist.description or playlist.title
    )
    channel_explicit = any(
        e.episode.explicit for e in entries if e.episode.explicit is not None
    )
    ET.SubElement(channel, f"{{{ITUNES_NS}}}explicit").text = (
        "yes" if channel_explicit else "no"
    )
    for entry in entries:
        if entry.episode.image_url:
            ET.SubElement(
                channel, f"{{{ITUNES_NS}}}image", href=entry.episode.image_url
            )
            break

    for entry in entries:
        episode: Episode = entry.episode
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = episode.title
        ET.SubElement(item, "link").text = page_url
        original_pub = (
            rfc2822(episode.published_at)
            if episode.published_at is not None
            else "unknown date"
        )
        item_description = f"Originally published: {original_pub}."
        if episode.summary:
            item_description += f"\n\n{episode.summary}"
        ET.SubElement(item, "description").text = item_description
        # pubDate = added-to-playlist date: curator adds surface as "new"
        # in podcatchers even for old episodes.
        ET.SubElement(item, "pubDate").text = rfc2822(entry.added_at)
        guid = ET.SubElement(item, "guid", isPermaLink="false")
        guid.text = f"tunedin:{playlist.playlist_id}:{episode.episode_id}"
        if episode.guid:
            ET.SubElement(item, f"{{{TUNEDIN_NS}}}sourceGuid").text = (
                episode.guid
            )
        # Redirect to the publisher's audio — never proxy (v1 decision).
        ET.SubElement(
            item,
            "enclosure",
            url=episode.audio_url,
            length="0",
            type=_enclosure_type(episode.audio_url),
        )
        # iTunes item tags (fallbacks documented in the module docstring).
        _sub(
            item,
            f"{{{ITUNES_NS}}}summary",
            episode.summary,
        )
        duration = _itunes_duration(episode.duration)
        if duration is not None:
            ET.SubElement(item, f"{{{ITUNES_NS}}}duration").text = duration
        ET.SubElement(item, f"{{{ITUNES_NS}}}explicit").text = (
            "yes" if episode.explicit else "no"
        )
        if episode.image_url:
            ET.SubElement(
                item, f"{{{ITUNES_NS}}}image", href=episode.image_url
            )

    xml_bytes = ET.tostring(rss, encoding="utf-8", xml_declaration=True)
    return xml_bytes
