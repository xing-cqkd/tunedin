import calendar
from datetime import datetime, timezone
import time
from typing import Any, List, Optional, Set, Tuple
import feedparser
import httpx
from dateutil import parser as date_parser

from backend.ingestion.models import FeedParseResult, ParsedEpisode, ParsedFeedMetadata


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
                if href:
                    if enc_type.startswith("audio/") or any(
                        href.lower().endswith(ext)
                        for ext in (".mp3", ".m4a", ".aac", ".ogg", ".wav", ".opus")
                    ):
                        return href, enc_type or "audio/mpeg"

            # If no explicit audio type, check first enclosure if available
            if enclosures and (enclosures[0].get("href") or enclosures[0].get("url")):
                return (
                    enclosures[0].get("href") or enclosures[0].get("url"),
                    enclosures[0].get("type"),
                )

        # 2. Media RSS tags (media_content)
        media_content = entry.get("media_content", [])
        if isinstance(media_content, list):
            for media in media_content:
                url = media.get("url")
                m_type = media.get("type", "")
                if url and (
                    m_type.startswith("audio/")
                    or any(
                        url.lower().endswith(ext)
                        for ext in (".mp3", ".m4a", ".aac", ".ogg", ".wav")
                    )
                ):
                    return url, m_type or "audio/mpeg"

        # 3. Links with rel=enclosure
        links = entry.get("links", [])
        if isinstance(links, list):
            for link in links:
                if link.get("rel") == "enclosure":
                    href = link.get("href")
                    l_type = link.get("type", "")
                    if href:
                        return href, l_type or "audio/mpeg"

        return None, None

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
        """
        parsed = feedparser.parse(content)
        feed_dict = parsed.get("feed", {})

        # Ensure last_updated_at is UTC aware if provided
        utc_last_updated_at: Optional[datetime] = None
        if last_updated_at:
            if last_updated_at.tzinfo is None:
                utc_last_updated_at = last_updated_at.replace(tzinfo=timezone.utc)
            else:
                utc_last_updated_at = last_updated_at.astimezone(timezone.utc)

        # 1. Extract Feed Metadata
        title = feed_dict.get("title", "Untitled Podcast").strip()
        author = feed_dict.get("author") or feed_dict.get("itunes_author") or feed_dict.get("publisher")
        description = feed_dict.get("subtitle") or feed_dict.get("description") or feed_dict.get("summary")
        link = feed_dict.get("link")
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
            etag=etag,
            last_modified=last_modified,
        )

        # 2. Extract & Filter Episodes
        raw_entries = parsed.get("entries", [])
        total_episodes = len(raw_entries)
        new_episodes: List[ParsedEpisode] = []

        for entry in raw_entries:
            audio_url, enclosure_type = cls.extract_audio_enclosure(entry)
            
            # Use id/guid, or fallback to audio_url or entry link
            guid = entry.get("id") or entry.get("guid") or audio_url or entry.get("link")
            if not guid:
                continue

            # Check known GUIDs filter
            if known_guids and guid in known_guids:
                continue

            # Extract Published Date
            published_at = cls.parse_published_date(entry)

            # Check incremental date filter
            if utc_last_updated_at and published_at:
                if published_at <= utc_last_updated_at:
                    continue

            # Extract Episode Title
            ep_title = entry.get("title", "Untitled Episode").strip()

            # Extract Duration
            raw_dur = entry.get("itunes_duration") or entry.get("duration")
            duration_secs = cls.parse_duration(raw_dur)

            # Extract Summary
            summary = entry.get("summary") or entry.get("description") or entry.get("subtitle")

            ep_link = entry.get("link")

            parsed_episode = ParsedEpisode(
                guid=guid,
                title=ep_title,
                audio_url=audio_url or "",
                duration=duration_secs,
                published_at=published_at,
                summary=summary,
                enclosure_type=enclosure_type,
                link=ep_link,
            )
            new_episodes.append(parsed_episode)

        return FeedParseResult(
            metadata=metadata,
            episodes=new_episodes,
            total_feed_episodes=total_episodes,
            is_not_modified=False,
        )

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

        # Ensure last_updated_at is UTC aware if provided
        utc_last_updated_at: Optional[datetime] = None
        if last_updated_at:
            if last_updated_at.tzinfo is None:
                utc_last_updated_at = last_updated_at.replace(tzinfo=timezone.utc)
            else:
                utc_last_updated_at = last_updated_at.astimezone(timezone.utc)

        # 1. Feed Metadata
        title = data.get("title", "Untitled Podcast").strip()
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

        # 2. Episode Items
        items = data.get("items", [])
        total_episodes = len(items)
        new_episodes: List[ParsedEpisode] = []

        for item in items:
            audio_url = item.get("audio_url")
            enclosure_type = item.get("enclosure_type", "audio/mpeg")
            raw_duration = item.get("duration")

            # Check JSON Feed attachments
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
                        break

            guid = item.get("id") or item.get("guid") or audio_url or item.get("url")
            if not guid:
                continue

            if known_guids and guid in known_guids:
                continue

            # Parse published date
            published_at = None
            raw_date = item.get("date_published") or item.get("published_at") or item.get("published")
            if raw_date and isinstance(raw_date, str):
                try:
                    dt = date_parser.parse(raw_date)
                    if dt.tzinfo is None:
                        published_at = dt.replace(tzinfo=timezone.utc)
                    else:
                        published_at = dt.astimezone(timezone.utc)
                except Exception:
                    pass

            if utc_last_updated_at and published_at:
                if published_at <= utc_last_updated_at:
                    continue

            ep_title = item.get("title", "Untitled Episode").strip()
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
        headers = {
            "User-Agent": "TunedIn/1.0 (+https://github.com/tunedin)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

        close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
            close_client = True

        try:
            response = await client.get(rss_url, headers=headers)

            # 304 Not Modified
            if response.status_code == 304:
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

            response.raise_for_status()

            new_etag = response.headers.get("ETag") or etag
            new_last_modified = response.headers.get("Last-Modified") or last_modified

            return cls.parse_xml_content(
                content=response.content,
                rss_url=rss_url,
                last_updated_at=last_updated_at,
                known_guids=known_guids,
                etag=new_etag,
                last_modified=new_last_modified,
            )
        finally:
            if close_client:
                await client.aclose()
