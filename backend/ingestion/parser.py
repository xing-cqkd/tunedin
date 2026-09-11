import calendar
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, List, Optional, Set, Tuple
import feedparser
import httpx
from dateutil import parser as date_parser

from backend.ingestion.http_util import fetch_limited, maybe_client
from backend.ingestion.models import FeedParseResult, ParsedEpisode, ParsedFeedMetadata

logger = logging.getLogger(__name__)


class PodcastFeedParser:
    """
    Parser for podcast RSS / Atom feeds with iTunes extension support
    and incremental episode filtering.
    """

    @classmethod
    def parse_duration(cls, raw_duration: Any) -> Optional[int]:
        """
        Normalize a raw duration representation (seconds string/int, HH:MM:SS, MM:SS)
        into total integer seconds.
        """
        if raw_duration is None:
            return None

        if isinstance(raw_duration, (int, float)):
            return int(round(raw_duration))

        duration_str = str(raw_duration).strip()
        if not duration_str:
            return None

        # Check if purely digits or float string
        try:
            return int(float(duration_str))
        except ValueError:
            pass

        # Split by colon for HH:MM:SS or MM:SS
        parts = duration_str.split(":")
        try:
            if len(parts) == 3:
                hours, minutes, seconds = parts
                return int(hours) * 3600 + int(minutes) * 60 + int(float(seconds))
            elif len(parts) == 2:
                minutes, seconds = parts
                return int(minutes) * 60 + int(float(seconds))
            elif len(parts) == 1:
                return int(float(parts[0]))
        except (ValueError, TypeError):
            return None

        return None

    @classmethod
    def parse_published_date(cls, entry: dict) -> Optional[datetime]:
        """
        Extract and normalize publication date to a timezone-aware UTC datetime.
        """
        # Try feedparser's parsed struct_time first
        struct_time = entry.get("published_parsed") or entry.get("updated_parsed")
        if struct_time:
            try:
                ts = calendar.timegm(struct_time)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                pass

        # Fallback to string parsing
        raw_date = entry.get("published") or entry.get("pubDate") or entry.get("updated")
        if raw_date and isinstance(raw_date, str):
            try:
                dt = date_parser.parse(raw_date)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                return None

        return None

    # Audio file extensions used as a fallback signal when an enclosure's
    # MIME type is missing or generic (XIN-126).
    _AUDIO_EXTENSIONS = (".mp3", ".m4a", ".aac", ".ogg", ".wav", ".opus")

    @classmethod
    def _is_audio_enclosure(cls, url: Optional[str], mime_type: Optional[str]) -> bool:
        """True when an enclosure looks like audio.

        Checks the MIME type first, falling back to the URL's file extension.
        """
        if not url:
            return False
        if (mime_type or "").startswith("audio/"):
            return True
        return url.lower().endswith(cls._AUDIO_EXTENSIONS)

    @classmethod
    def extract_audio_enclosure(cls, entry: dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract primary audio enclosure URL and MIME type from entry.
        """
        # 1. Standard RSS enclosures
        enclosures = entry.get("enclosures", [])
        if isinstance(enclosures, list):
            for enc in enclosures:
                href = enc.get("href") or enc.get("url")
                enc_type = enc.get("type", "")
                if cls._is_audio_enclosure(href, enc_type):
                    return href, enc_type or "audio/mpeg"

            # No audio-looking enclosure found: return None rather than a
            # non-audio enclosure (e.g. a PDF transcript) as the audio URL.

        # 2. Media RSS tags (media_content)
        media_content = entry.get("media_content", [])
        if isinstance(media_content, list):
            for media in media_content:
                url = media.get("url")
                m_type = media.get("type", "")
                if cls._is_audio_enclosure(url, m_type):
                    return url, m_type or "audio/mpeg"

        # 3. Links with rel=enclosure
        links = entry.get("links", [])
        if isinstance(links, list):
            for link in links:
                if link.get("rel") == "enclosure":
                    href = link.get("href")
                    l_type = link.get("type", "")
                    # XIN-126: only audio enclosures — a non-audio enclosure
                    # (e.g. application/pdf) must not be recorded as the
                    # audio URL.
                    if cls._is_audio_enclosure(href, l_type):
                        return href, l_type or "audio/mpeg"

        return None, None

    @classmethod
    def extract_transcript_url(cls, entry: dict) -> Optional[str]:
        """
        Extract Podcasting 2.0 transcript URL (<podcast:transcript url="..." />)
        or transcript link from entry.
        """
        # 1. podcast_transcript tag (parsed by feedparser)
        pt = entry.get("podcast_transcript")
        if isinstance(pt, dict):
            url = pt.get("url")
            if url:
                return url
        elif isinstance(pt, list) and pt:
            first = pt[0]
            if isinstance(first, dict) and first.get("url"):
                return first.get("url")
            elif isinstance(first, str):
                return first

        # 2. transcripts list
        transcripts = entry.get("transcripts", [])
        if isinstance(transcripts, list) and transcripts:
            first = transcripts[0]
            if isinstance(first, dict) and first.get("url"):
                return first.get("url")

        # 3. rel="transcript" links
        links = entry.get("links", [])
        if isinstance(links, list):
            for link in links:
                if link.get("rel") == "transcript" or link.get("type", "").startswith("text/vtt"):
                    href = link.get("href")
                    if href:
                        return href

        return None

    @classmethod
    def extract_chapters_url(cls, entry: dict) -> Optional[str]:
        """
        Extract Podcasting 2.0 chapters URL (<podcast:chapters url="..." />).
        """
        pc = entry.get("podcast_chapters")
        if isinstance(pc, dict):
            return pc.get("url")
        elif isinstance(pc, str):
            return pc
        return None

    @classmethod
    def extract_episode_image(cls, entry: dict) -> Optional[str]:
        """
        Extract episode-specific artwork image URL.
        """
        # itunes_image
        it_img = entry.get("itunes_image")
        if isinstance(it_img, dict):
            return it_img.get("href")
        elif isinstance(it_img, str):
            return it_img

        # image
        img = entry.get("image")
        if isinstance(img, dict):
            return img.get("href")
        elif isinstance(img, str):
            return img

        # media_thumbnail
        thumbs = entry.get("media_thumbnail", [])
        if isinstance(thumbs, list) and thumbs:
            first = thumbs[0]
            if isinstance(first, dict):
                return first.get("url")

        return None

    @classmethod
    def extract_content_html(cls, entry: dict) -> Optional[str]:
        """
        Extract full HTML content / shownotes (<content:encoded> or content array).
        """
        contents = entry.get("content", [])
        if isinstance(contents, list) and contents:
            for c in contents:
                if isinstance(c, dict) and c.get("value"):
                    return c.get("value")

        return entry.get("content_encoded")

    @classmethod
    def parse_explicit(cls, raw_val: Any) -> Optional[bool]:
        """
        Normalize iTunes explicit tags ('yes', 'no', 'clean', 'explicit', True, False).
        """
        if raw_val is None:
            return None
        if isinstance(raw_val, bool):
            return raw_val
        val_str = str(raw_val).strip().lower()
        if val_str in ("yes", "explicit", "true", "1"):
            return True
        elif val_str in ("no", "clean", "false", "0"):
            return False
        return None

    @classmethod
    def extract_feed_image(cls, feed_dict: dict) -> Optional[str]:
        """
        Extract feed-level artwork image URL.
        """
        # feedparser puts itunes_image directly or inside image dict
        if "image" in feed_dict and isinstance(feed_dict["image"], dict):
            href = feed_dict["image"].get("href")
            if href:
                return href

        if "itunes_image" in feed_dict:
            val = feed_dict["itunes_image"]
            if isinstance(val, dict):
                return val.get("href")
            elif isinstance(val, str):
                return val

        if "image" in feed_dict and isinstance(feed_dict["image"], str):
            return feed_dict["image"]

        return None

    @classmethod
    def extract_feed_category(cls, feed_dict: dict) -> Optional[str]:
        """
        Extract feed category / genre.
        """
        tags = feed_dict.get("tags", [])
        if tags and isinstance(tags, list) and len(tags) > 0:
            first_term = tags[0].get("term") if isinstance(tags[0], dict) else str(tags[0])
            if first_term:
                return first_term

        if "category" in feed_dict and isinstance(feed_dict["category"], str):
            return feed_dict["category"]

        if "itunes_category" in feed_dict:
            cat = feed_dict["itunes_category"]
            if isinstance(cat, dict):
                return cat.get("text")
            elif isinstance(cat, str):
                return cat

        return None

    @classmethod
    def _as_utc(cls, dt: Optional[datetime]) -> Optional[datetime]:
        """Normalize an optional datetime to UTC-aware (None stays None)."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @classmethod
    def _filter_new_episodes(
        cls,
        candidates: List[Tuple[str, Optional[datetime], Any]],
        last_updated_at: Optional[datetime] = None,
        known_guids: Optional[Set[str]] = None,
    ) -> List[Tuple[str, Optional[datetime], Any]]:
        """
        Shared episode filtering pipeline used by ``parse_xml_content`` and
        ``parse_json_content``. ``candidates`` is a list of
        ``(guid, published_at, raw_entry)`` tuples where ``raw_entry`` is the
        format-specific entry/item dict. Applies, in order: in-batch duplicate
        dedup, the ``known_guids`` filter, and the incremental
        ``last_updated_at`` cutoff. Returns the surviving candidates in order.
        """
        utc_last_updated_at = cls._as_utc(last_updated_at)
        seen_in_batch: Set[str] = set()
        kept: List[Tuple[str, Optional[datetime], Any]] = []
        for guid, published_at, raw in candidates:
            # In-batch duplicate deduplication
            if guid in seen_in_batch:
                continue
            seen_in_batch.add(guid)

            # Check known GUIDs filter
            if known_guids and guid in known_guids:
                continue

            # Check incremental date filter
            if utc_last_updated_at and published_at:
                if published_at <= utc_last_updated_at:
                    continue

            kept.append((guid, published_at, raw))
        return kept

    @classmethod
    def parse_xml_content(
        cls,
        content: str | bytes,
        rss_url: str,
        last_updated_at: Optional[datetime] = None,
        known_guids: Optional[Set[str]] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FeedParseResult:
        """
        Parses raw XML string or bytes, extracting metadata and filtering for new episodes.

        Raises ValueError when the content does not parse as a usable feed
        (feedparser's ``bozo`` flag set, or no usable feed title/link) so the
        caller's fetch-error path marks the feed ``error`` instead of
        overwriting its title with "Untitled Podcast".
        """
        parsed = feedparser.parse(content)
        feed_dict = parsed.get("feed", {})

        # feedparser never raises on malformed XML: an HTML error page, a
        # truncated download, or an empty 200 body parses to an empty feed.
        # Refuse to treat that as a successful parse (XIN-126).
        has_identity = bool(feed_dict.get("title") or feed_dict.get("link"))
        if parsed.get("bozo") or not has_identity:
            bozo_exc = parsed.get("bozo_exception")
            logger.warning(
                "Rejecting unparsable feed content for %s (bozo=%s, title/link present=%s%s)",
                rss_url,
                bool(parsed.get("bozo")),
                has_identity,
                f", bozo_exception={bozo_exc!r}" if bozo_exc else "",
            )
            raise ValueError(
                f"Feed content for {rss_url} did not parse as a usable RSS/Atom feed"
            )

        # 1. Extract Feed Metadata (null-safe: a present-but-null title must
        # not crash .strip(); XIN-126)
        title = (feed_dict.get("title") or "Untitled Podcast").strip()
        author = feed_dict.get("author") or feed_dict.get("itunes_author") or feed_dict.get("publisher")
        description = feed_dict.get("subtitle") or feed_dict.get("description") or feed_dict.get("summary")
        link = feed_dict.get("link")
        language = feed_dict.get("language")
        feed_type = feed_dict.get("itunes_type")
        podcast_guid = feed_dict.get("podcast_guid")
        explicit = cls.parse_explicit(feed_dict.get("itunes_explicit"))
        image_url = cls.extract_feed_image(feed_dict)
        category = cls.extract_feed_category(feed_dict)

        metadata = ParsedFeedMetadata(
            title=title,
            rss_url=rss_url,
            author=author,
            description=description,
            image_url=image_url,
            category=category,
            link=link,
            language=language,
            website_url=link,
            feed_type=feed_type,
            podcast_guid=podcast_guid,
            explicit=explicit,
            etag=etag,
            last_modified=last_modified,
        )

        # 2. Extract & Filter Episodes (shared pipeline; XIN-129)
        raw_entries = parsed.get("entries", [])
        total_episodes = len(raw_entries)
        new_episodes: List[ParsedEpisode] = []

        candidates: List[Tuple[str, Optional[datetime], Any]] = []
        for entry in raw_entries:
            audio_url, _ = cls.extract_audio_enclosure(entry)

            # Use id/guid, or fallback to audio_url or entry link
            guid = entry.get("id") or entry.get("guid") or audio_url or entry.get("link")
            if not guid:
                continue
            candidates.append((guid, cls.parse_published_date(entry), entry))

        for guid, published_at, entry in cls._filter_new_episodes(
            candidates, last_updated_at=last_updated_at, known_guids=known_guids
        ):
            audio_url, enclosure_type = cls.extract_audio_enclosure(entry)

            # Extract Episode Title (null-safe; XIN-126)
            ep_title = (entry.get("title") or "Untitled Episode").strip()

            # Extract Duration
            raw_dur = entry.get("itunes_duration") or entry.get("duration")
            duration_secs = cls.parse_duration(raw_dur)

            # Extract Summary
            summary = entry.get("summary") or entry.get("description") or entry.get("subtitle")
            content_html = cls.extract_content_html(entry)

            ep_link = entry.get("link")
            transcript_url = cls.extract_transcript_url(entry)
            chapters_url = cls.extract_chapters_url(entry)
            ep_image_url = cls.extract_episode_image(entry)
            ep_type = entry.get("itunes_episodetype") or "full"

            # Parse episode and season numbers
            ep_num = None
            raw_ep_num = entry.get("itunes_episode")
            if raw_ep_num:
                try:
                    ep_num = int(raw_ep_num)
                except (ValueError, TypeError):
                    pass

            season_num = None
            raw_season = entry.get("itunes_season")
            if raw_season:
                try:
                    season_num = int(raw_season)
                except (ValueError, TypeError):
                    pass

            ep_explicit = cls.parse_explicit(entry.get("itunes_explicit"))

            parsed_episode = ParsedEpisode(
                guid=guid,
                title=ep_title,
                audio_url=audio_url or "",
                duration=duration_secs,
                published_at=published_at,
                summary=summary,
                content_html=content_html,
                enclosure_type=enclosure_type,
                link=ep_link,
                transcript_url=transcript_url,
                chapters_url=chapters_url,
                image_url=ep_image_url,
                episode_type=ep_type,
                episode_number=ep_num,
                season_number=season_num,
                explicit=ep_explicit,
            )
            new_episodes.append(parsed_episode)

        return FeedParseResult(
            metadata=metadata,
            episodes=new_episodes,
            total_feed_episodes=total_episodes,
            is_not_modified=False,
        )

    @classmethod
    def _extract_json_audio(cls, item: dict) -> Tuple[
        Optional[str], Optional[str], Any, Optional[str]
    ]:
        """
        Extract (audio_url, enclosure_type, raw_duration, transcript_url) from
        a JSON Feed item, scanning ``attachments`` for audio / transcript
        entries. Shared by guid-fallback computation and episode building.
        """
        audio_url = item.get("audio_url")
        enclosure_type = item.get("enclosure_type", "audio/mpeg")
        raw_duration = item.get("duration")
        transcript_url = item.get("transcript_url")

        attachments = item.get("attachments", [])
        if isinstance(attachments, list):
            for att in attachments:
                m_type = att.get("mime_type", "")
                u = att.get("url")
                if u and (m_type.startswith("audio/") or not audio_url):
                    audio_url = u
                    enclosure_type = m_type or "audio/mpeg"
                    if "duration_in_seconds" in att:
                        raw_duration = att["duration_in_seconds"]
                elif u and (m_type.startswith("text/vtt") or "transcript" in m_type):
                    transcript_url = u

        return audio_url, enclosure_type, raw_duration, transcript_url

    @classmethod
    def _parse_json_published_at(cls, item: dict) -> Optional[datetime]:
        """Parse a JSON Feed item's published date to UTC-aware datetime."""
        published_at = None
        raw_date = item.get("date_published") or item.get("published_at") or item.get("published")
        if raw_date and isinstance(raw_date, str):
            try:
                dt = date_parser.parse(raw_date)
                published_at = cls._as_utc(dt)
            except Exception:
                pass
        return published_at

    @classmethod
    def parse_json_content(
        cls,
        content: str | bytes | dict,
        rss_url: str,
        last_updated_at: Optional[datetime] = None,
        known_guids: Optional[Set[str]] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FeedParseResult:
        """
        Parses JSON Feed payload (JSON string, bytes, or dictionary),
        extracting metadata and filtering for new episodes.
        """
        import json

        if isinstance(content, (str, bytes)):
            data = json.loads(content)
        elif isinstance(content, dict):
            data = content
        else:
            raise ValueError(f"Unsupported content type for JSON parsing: {type(content)}")

        # 1. Feed Metadata (null-safe; XIN-126)
        title = (data.get("title") or "Untitled Podcast").strip()
        author = None
        if "author" in data and isinstance(data["author"], dict):
            author = data["author"].get("name")
        elif "authors" in data and isinstance(data["authors"], list) and data["authors"]:
            author = data["authors"][0].get("name") if isinstance(data["authors"][0], dict) else str(data["authors"][0])
        elif "author" in data and isinstance(data["author"], str):
            author = data["author"]

        description = data.get("description")
        image_url = data.get("icon") or data.get("image") or data.get("favicon")
        link = data.get("home_page_url") or data.get("feed_url")
        category = data.get("category")

        metadata = ParsedFeedMetadata(
            title=title,
            rss_url=rss_url,
            author=author,
            description=description,
            image_url=image_url,
            category=category,
            link=link,
            etag=etag,
            last_modified=last_modified,
        )

        # 2. Episode Items (null-safe; XIN-126; shared pipeline; XIN-129)
        items = data.get("items") or []
        total_episodes = len(items)

        candidates: List[Tuple[str, Optional[datetime], Any]] = []
        for item in items:
            audio_url, _, _, _ = cls._extract_json_audio(item)
            guid = item.get("id") or item.get("guid") or audio_url or item.get("url")
            if not guid:
                continue
            candidates.append((guid, cls._parse_json_published_at(item), item))

        new_episodes: List[ParsedEpisode] = []
        for guid, published_at, item in cls._filter_new_episodes(
            candidates, last_updated_at=last_updated_at, known_guids=known_guids
        ):
            audio_url, enclosure_type, raw_duration, transcript_url = cls._extract_json_audio(item)

            ep_title = (item.get("title") or "Untitled Episode").strip()
            duration_secs = cls.parse_duration(raw_duration)
            summary = item.get("summary") or item.get("content_text") or item.get("content_html")
            ep_link = item.get("url")

            parsed_episode = ParsedEpisode(
                guid=guid,
                title=ep_title,
                audio_url=audio_url or "",
                duration=duration_secs,
                published_at=published_at,
                summary=summary,
                enclosure_type=enclosure_type,
                link=ep_link,
                transcript_url=transcript_url,
            )
            new_episodes.append(parsed_episode)

        return FeedParseResult(
            metadata=metadata,
            episodes=new_episodes,
            total_feed_episodes=total_episodes,
            is_not_modified=False,
        )

    @classmethod
    def parse_content(
        cls,
        content: str | bytes | dict,
        rss_url: str,
        last_updated_at: Optional[datetime] = None,
        known_guids: Optional[Set[str]] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FeedParseResult:
        """
        Unified parser entrypoint: automatically detects XML vs JSON payload.
        """
        if isinstance(content, dict):
            return cls.parse_json_content(
                content=content,
                rss_url=rss_url,
                last_updated_at=last_updated_at,
                known_guids=known_guids,
                etag=etag,
                last_modified=last_modified,
            )

        stripped = content.strip() if isinstance(content, (str, bytes)) else b""
        if (isinstance(stripped, str) and stripped.startswith("{")) or (
            isinstance(stripped, bytes) and stripped.startswith(b"{")
        ):
            return cls.parse_json_content(
                content=content,
                rss_url=rss_url,
                last_updated_at=last_updated_at,
                known_guids=known_guids,
                etag=etag,
                last_modified=last_modified,
            )

        return cls.parse_xml_content(
            content=content,
            rss_url=rss_url,
            last_updated_at=last_updated_at,
            known_guids=known_guids,
            etag=etag,
            last_modified=last_modified,
        )

    @classmethod
    async def fetch_and_parse(
        cls,
        rss_url: str,
        last_updated_at: Optional[datetime] = None,
        known_guids: Optional[Set[str]] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
        timeout: float = 30.0,
    ) -> FeedParseResult:
        """
        Asynchronously fetches an RSS feed via HTTP (supporting ETag & Last-Modified caching)
        and parses new episodes.
        """
        user_agent = (
            client.headers.get("User-Agent")
            if client and "User-Agent" in client.headers
            else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 (compatible; TunedIn/1.0)"
        )
        headers = {
            "User-Agent": user_agent,
            "Accept": "application/rss+xml, application/xml, text/xml, application/feed+json, */*",
        }
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

        # XIN-49: shared client lifecycle; XIN-56/XIN-59: retry + size cap.
        # (XIN-62 SSRF validation lives at the service boundary in
        # service.sync_podcast_episodes, so this stays unit-testable.)
        async with maybe_client(client, timeout=timeout) as c:
            body, resp_headers, status_code = await fetch_limited(
                c, rss_url, headers=headers
            )

            # 304 Not Modified
            if status_code == 304:
                return FeedParseResult(
                    metadata=ParsedFeedMetadata(
                        title="",
                        rss_url=rss_url,
                        etag=etag,
                        last_modified=last_modified,
                    ),
                    episodes=[],
                    total_feed_episodes=0,
                    is_not_modified=True,
                )

            new_etag = resp_headers.get("ETag") or etag
            new_last_modified = resp_headers.get("Last-Modified") or last_modified

            # Route through the unified entrypoint so JSON Feeds served over HTTP
            # are detected and parsed as JSON instead of being misparsed as XML.
            # XIN-75: feedparser.parse is CPU-bound — never block the event loop.
            return await asyncio.to_thread(
                cls.parse_content,
                content=body,
                rss_url=rss_url,
                last_updated_at=last_updated_at,
                known_guids=known_guids,
                etag=new_etag,
                last_modified=new_last_modified,
            )
