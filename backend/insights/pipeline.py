"""
LLM insight-pipeline queries (XIN-39 split).

These helpers lived on ``FeedIngestionService``; they belong to the
insights domain: reading episodes awaiting LLM processing (tagging,
insight extraction) and marking them done.

``mark_episode_processed`` commits once; ``get_unprocessed_episodes`` is a
read and never commits. Callers must not wrap ``mark_episode_processed``
in a transaction they expect to roll back.
"""
import uuid

from backend.persistence.models.episode import Episode
from backend.persistence.repositories import Store


async def get_unprocessed_episodes(
    store: Store,
    feed_id: uuid.UUID | None = None,
    limit: int = 50,
) -> list[Episode]:
    """
    Retrieves episodes waiting to be read, analyzed, and tagged by the LLM (processed=False).
    Ordered chronologically descending.
    """
    return await store.episodes.list_unprocessed(feed_id=feed_id, limit=limit)


async def mark_episode_processed(
    store: Store,
    episode_id: uuid.UUID,
    processed: bool = True,
) -> Episode | None:
    """
    Updates the LLM processing status for an episode once insights and tags have been saved.
    Commits once.
    """
    episode = await store.episodes.mark_processed(episode_id, processed)
    await store.commit()
    return episode
