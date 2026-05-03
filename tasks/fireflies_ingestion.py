# Standard library imports
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.http import RetrySession
from utils.parse import parse_list

logger = logging.getLogger(__name__)

_FIREFLIES_API_URL = "https://api.fireflies.ai/graphql"
_PAGE_SIZE = 50
_MIN_OUTLINE_LENGTH = 200

_TRANSCRIPTS_QUERY = """
query Transcripts(
  $fromDate: DateTime
  $toDate: DateTime
  $limit: Int!
  $skip: Int!
  $hostEmail: String
  $organizers: [String!]
  $channelId: String
  $title: String
) {
  transcripts(
    fromDate: $fromDate
    toDate: $toDate
    limit: $limit
    skip: $skip
    host_email: $hostEmail
    organizers: $organizers
    channel_id: $channelId
    title: $title
  ) {
    id
    title
    host_email
    organizer_email
    participants
    date
    duration
    transcript_url
    audio_url
    video_url
    meeting_link
    speakers { id name }
    summary {
      overview
      outline
      notes
      keywords
      gist
      action_items
    }
    sentences { text speaker_name start_time }
  }
}
"""


class FirefliesIngestionJob(IngestionJob):
    """Ingestion connector for Fireflies.ai meeting transcripts.

    Fetches transcripts via the Fireflies GraphQL API, composes document
    content from summary fields and sentence data, and stores them in the
    vector store.

    Configuration (config.yaml):
        - config.api_key: Fireflies API key (required)
        - config.filter_keyword: Filter transcripts by title keyword (optional)
        - config.filter_fromDate: ISO 8601 start date, e.g. 2024-01-01T00:00:00.000Z (optional)
        - config.filter_toDate: ISO 8601 end date (optional)
        - config.filter_hostEmail: Filter by host email (optional)
        - config.filter_organizers: Filter by organizer email(s); comma-separated string or YAML list (optional)
        - config.filter_channel_id: Filter by channel ID (optional)
        - config.max_items: Maximum transcripts to fetch (optional, default 100)

    Note: audio_url and video_url are excluded from metadata — they are expiring signed CDN URLs
    that become invalid quickly and would cause metadata to exceed the chunk size limit.
    Use transcript_url (stable Fireflies app link) for navigation instead.
    """

    @property
    def source_type(self) -> str:
        return "fireflies"

    def __init__(self, config: dict):
        super().__init__(config)
        cfg = config.get("config", {})

        self.api_key = cfg.get("api_key", "").strip()
        if not self.api_key:
            raise ValueError("api_key is required in Fireflies connector config")

        parsed = parse_list(cfg.get("filter_organizers"))
        organizers = parsed or None
        self.filters: dict[str, Any] = {
            k: v
            for k, v in {
                "fromDate": cfg.get("filter_fromDate"),
                "toDate": cfg.get("filter_toDate"),
                "hostEmail": cfg.get("filter_hostEmail"),
                "organizers": organizers,
                "channelId": cfg.get("filter_channel_id"),
                "title": cfg.get("filter_keyword"),
            }.items()
            if v
        }
        self.max_items = int(cfg.get("max_items", 100))

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def _graphql(self, variables: dict) -> dict:
        with RetrySession() as session:
            response = session.post(
                _FIREFLIES_API_URL,
                json={"query": _TRANSCRIPTS_QUERY, "variables": variables},
                headers=self._headers,
                retry=True,
            )
        response.raise_for_status()
        data = response.json()
        self._handle_graphql_errors(data)
        return data

    def _handle_graphql_errors(self, data: dict[str, Any]) -> None:
        if "errors" not in data:
            return
        for error in data["errors"]:
            if error.get("code") == "too_many_requests":
                retry_after = error.get("extensions", {}).get("metadata", {}).get("retryAfter")
                raise RuntimeError(f"Fireflies rate limit exceeded (too_many_requests); retryAfter={retry_after}")
        non_fatal_codes = {"paid_required"}
        fatal = [e for e in data["errors"] if e.get("code") not in non_fatal_codes]
        if fatal:
            raise RuntimeError(f"Fireflies GraphQL error: {fatal}")
        for error in data["errors"]:
            logger.warning("Fireflies API warning [%s]: %s", error.get("code"), error.get("message"))

    def _build_query_variables(self) -> dict[str, Any]:
        return {"limit": _PAGE_SIZE, **self.filters}

    def _transcript_to_item(self, transcript: dict) -> IngestionItem:
        date_ms = transcript.get("date")
        last_modified = datetime.fromtimestamp(date_ms / 1000, tz=UTC) if date_ms else None
        item = IngestionItem(
            id=transcript.get("id"),
            source_ref=transcript.get("transcript_url") or transcript.get("id"),
            last_modified=last_modified,
        )
        item._metadata_cache.update(transcript)
        return item

    def list_items(self) -> Iterator[IngestionItem]:
        variables = self._build_query_variables()
        fetched = 0

        while fetched < self.max_items:
            variables["skip"] = fetched
            transcripts = self._graphql(variables).get("data", {}).get("transcripts", []) or []

            for transcript in transcripts:
                if fetched >= self.max_items:
                    return
                yield self._transcript_to_item(transcript)
                fetched += 1

            if len(transcripts) < _PAGE_SIZE:
                break

    def get_raw_content(self, item: IngestionItem) -> str:
        transcript = item._metadata_cache
        parts = []

        summary = transcript.get("summary") or {}

        overview = (summary.get("overview") or "").strip()
        if overview:
            parts.append(f"## Overview\n\n{overview}")

        outline = (summary.get("outline") or "").strip()
        sentences = transcript.get("sentences") or []

        if outline and len(outline) > _MIN_OUTLINE_LENGTH:
            parts.append(f"## Outline\n\n{outline}")
        elif sentences:
            parts.append("## Transcript\n\n" + self._build_transcript_from_sentences(sentences))

        notes = (summary.get("notes") or "").strip()
        if notes:
            parts.append(f"## Notes\n\n{notes}")

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        return f"fireflies_{item.id}"[:255]

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        transcript = item._metadata_cache
        summary = transcript.get("summary") or {}

        speakers = transcript.get("speakers") or []
        speaker_names = [s.get("name") for s in speakers if s.get("name")]

        participants = transcript.get("participants") or []
        last_modified = item.last_modified

        extra = {
            "title": transcript.get("title"),
            "host_email": transcript.get("host_email"),
            "organizer_email": transcript.get("organizer_email"),
            "participants": ", ".join(participants) if participants else None,
            "date": str(last_modified) if last_modified else None,
            "transcript_url": transcript.get("transcript_url"),
            "duration": transcript.get("duration"),
            "meeting_link": transcript.get("meeting_link"),
            "speakers": ", ".join(speaker_names) if speaker_names else None,
            "keywords": summary.get("keywords"),
            "gist": summary.get("gist"),
            "action_items": summary.get("action_items"),
        }

        return {k: v for k, v in extra.items() if v is not None}

    def _build_transcript_from_sentences(self, sentences: list[dict[str, Any]]) -> str:
        lines = []
        current_speaker = None
        current_lines: list[str] = []

        for sentence in sentences:
            speaker = sentence.get("speaker_name") or "Unknown Speaker"
            text = (sentence.get("text") or "").replace("\xa0", " ").strip()
            start_time = sentence.get("start_time")

            if speaker != current_speaker:
                if current_speaker is not None and current_lines:
                    lines.append(f"**{current_speaker}:** " + " ".join(current_lines))
                current_speaker = speaker
                current_lines = []
                if start_time is not None:
                    lines.append(f"*[{_format_timestamp(start_time)}]*")

            if text:
                current_lines.append(text)

        if current_speaker is not None and current_lines:
            lines.append(f"**{current_speaker}:** " + " ".join(current_lines))

        return "\n\n".join(lines)


def _format_timestamp(seconds: float) -> str:
    t = time.gmtime(seconds)
    if t.tm_hour:
        return time.strftime("%H:%M:%S", t)
    return time.strftime("%M:%S", t)
