import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tasks.fireflies_ingestion import FirefliesIngestionJob, _format_timestamp
from tasks.helper_classes.ingestion_item import IngestionItem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TRANSCRIPT_DATE_MS = 1704067200000  # 2024-01-01T00:00:00Z
TRANSCRIPT_DATE_DT = datetime.fromtimestamp(TRANSCRIPT_DATE_MS / 1000, tz=UTC)


def _make_config(**overrides):
    cfg = {"api_key": "test-key", "max_items": 100}
    cfg.update(overrides)
    return {"name": "test_fireflies", "config": cfg}


def _make_transcript(
    tid="abc123",
    title="Team Standup",
    host_email="host@example.com",
    organizer_email="org@example.com",
    participants=None,
    date=TRANSCRIPT_DATE_MS,
    duration=3600,
    transcript_url="https://app.fireflies.ai/view/abc123",
    audio_url="https://audio.fireflies.ai/abc123.mp3",
    video_url=None,
    meeting_link="https://meet.google.com/abc",
    speakers=None,
    summary=None,
    sentences=None,
):
    return {
        "id": tid,
        "title": title,
        "host_email": host_email,
        "organizer_email": organizer_email,
        "participants": participants or ["alice@example.com", "bob@example.com"],
        "date": date,
        "duration": duration,
        "transcript_url": transcript_url,
        "audio_url": audio_url,
        "video_url": video_url,
        "meeting_link": meeting_link,
        "speakers": speakers or [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
        "summary": summary
        or {
            "overview": "Daily standup meeting covering sprint progress.",
            "outline": "",
            "notes": "Action items discussed.",
            "keywords": "standup, sprint",
            "gist": "Sprint update",
            "action_items": "Review PR #42",
        },
        "sentences": sentences
        or [
            {"text": "Hello everyone.", "speaker_name": "Alice", "start_time": 0},
            {"text": "Let's get started.", "speaker_name": "Alice", "start_time": 3000},
            {"text": "Sure, ready.", "speaker_name": "Bob", "start_time": 7000},
        ],
    }


def _make_job(config=None, **cfg_overrides):
    if config is None:
        config = _make_config(**cfg_overrides)
    with patch("tasks.base.MetadataTracker"), patch("tasks.base.VectorStoreManager"):
        return FirefliesIngestionJob(config)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFirefliesIngestionInit(unittest.TestCase):
    def test_missing_api_key_raises(self):
        with self.assertRaises(ValueError, msg="api_key is required"):
            _make_job(api_key="")

    def test_valid_config(self):
        job = _make_job()
        self.assertEqual(job.api_key, "test-key")
        self.assertEqual(job.max_items, 100)
        self.assertEqual(job.filters, {})

    def test_optional_filters_parsed(self):
        job = _make_job(
            filter_fromDate="2024-01-01T00:00:00.000Z",
            filter_hostEmail="host@example.com",
            filter_channel_id="ch1",
        )
        self.assertEqual(job.filters["fromDate"], "2024-01-01T00:00:00.000Z")
        self.assertEqual(job.filters["hostEmail"], "host@example.com")
        self.assertEqual(job.filters["channelId"], "ch1")

    def test_organizers_from_string(self):
        job = _make_job(filter_organizers="a@example.com, b@example.com")
        self.assertEqual(job.filters["organizers"], ["a@example.com", "b@example.com"])

    def test_organizers_from_list(self):
        config = _make_config(filter_organizers=["a@example.com", "b@example.com"])
        job = _make_job(config=config)
        self.assertEqual(job.filters["organizers"], ["a@example.com", "b@example.com"])


class TestFirefliesListItems(unittest.TestCase):
    def _make_response(self, transcripts):
        mock = MagicMock()
        mock.raise_for_status.return_value = None
        mock.json.return_value = {"data": {"transcripts": transcripts}}
        return mock

    def test_yields_items_single_page(self):
        t = _make_transcript()
        job = _make_job()

        with patch("utils.http.RetrySession.post", return_value=self._make_response([t])):
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "abc123")
        self.assertEqual(items[0].source_ref, t["transcript_url"])
        self.assertEqual(items[0].last_modified, TRANSCRIPT_DATE_DT)

    def test_stops_after_max_items(self):
        transcripts = [_make_transcript(tid=f"id{i}") for i in range(50)]
        job = _make_job(max_items=10)

        with patch("utils.http.RetrySession.post", return_value=self._make_response(transcripts)):
            items = list(job.list_items())

        self.assertEqual(len(items), 10)

    def test_pagination_stops_when_fewer_than_page_size(self):
        page1 = [_make_transcript(tid=f"id{i}") for i in range(50)]
        page2 = [_make_transcript(tid=f"id{i}") for i in range(50, 60)]

        responses = [
            self._make_response(page1),
            self._make_response(page2),
        ]
        job = _make_job(max_items=500)

        with patch("utils.http.RetrySession.post", side_effect=responses):
            items = list(job.list_items())

        self.assertEqual(len(items), 60)

    def test_metadata_cached_on_item(self):
        t = _make_transcript()
        job = _make_job()

        with patch("utils.http.RetrySession.post", return_value=self._make_response([t])):
            items = list(job.list_items())

        self.assertEqual(items[0]._metadata_cache["title"], "Team Standup")

    def test_graphql_error_raises(self):
        mock = MagicMock()
        mock.raise_for_status.return_value = None
        mock.json.return_value = {"errors": [{"message": "Unauthorized"}]}

        job = _make_job()
        with patch("utils.http.RetrySession.post", return_value=mock):
            with self.assertRaises(RuntimeError):
                list(job.list_items())


class TestFirefliesGetRawContent(unittest.TestCase):
    def _item_with(self, transcript):
        item = IngestionItem(id=transcript["id"], source_ref=transcript["transcript_url"])
        item._metadata_cache.update(transcript)
        return item

    def test_uses_summary_when_overview_and_notes_present(self):
        t = _make_transcript()
        job = _make_job()
        content = job.get_raw_content(self._item_with(t))

        self.assertIn("## Overview", content)
        self.assertIn("Daily standup", content)
        self.assertIn("## Notes", content)
        self.assertIn("Action items discussed", content)

    def test_uses_outline_when_long_enough(self):
        long_outline = "x" * 300
        t = _make_transcript(
            summary={
                "overview": "Overview text",
                "outline": long_outline,
                "notes": "",
                "keywords": None,
                "gist": None,
                "action_items": None,
            }
        )
        job = _make_job()
        content = job.get_raw_content(self._item_with(t))

        self.assertIn("## Outline", content)
        self.assertIn(long_outline, content)
        self.assertNotIn("## Transcript", content)

    def test_falls_back_to_sentences_when_outline_short(self):
        t = _make_transcript(
            summary={
                "overview": "",
                "outline": "Short",
                "notes": "",
                "keywords": None,
                "gist": None,
                "action_items": None,
            }
        )
        job = _make_job()
        content = job.get_raw_content(self._item_with(t))

        self.assertIn("## Transcript", content)
        self.assertIn("Alice", content)
        self.assertIn("Hello everyone", content)

    def test_empty_when_no_content(self):
        t = _make_transcript(
            summary={"overview": "", "outline": "", "notes": "", "keywords": None, "gist": None, "action_items": None},
            sentences=None,
        )
        t["sentences"] = []
        job = _make_job()
        content = job.get_raw_content(self._item_with(t))
        self.assertEqual(content.strip(), "")


class TestFirefliesGetItemName(unittest.TestCase):
    def test_basic(self):
        job = _make_job()
        item = IngestionItem(id="abc123", source_ref="https://example.com")
        self.assertEqual(job.get_item_name(item), "fireflies_abc123")

    def test_truncated_to_255(self):
        job = _make_job()
        item = IngestionItem(id="x" * 300, source_ref="https://example.com")
        self.assertEqual(len(job.get_item_name(item)), 255)


class TestFirefliesGetDocumentMetadata(unittest.TestCase):
    def test_all_required_fields_present(self):
        t = _make_transcript()
        job = _make_job()
        item = IngestionItem(id=t["id"], source_ref=t["transcript_url"], last_modified=TRANSCRIPT_DATE_DT)
        item._metadata_cache.update(t)

        meta = job.get_extra_metadata(item, "", {})

        for field in (
            "title",
            "host_email",
            "organizer_email",
            "participants",
            "transcript_url",
            "duration",
            "meeting_link",
            "speakers",
            "gist",
            "action_items",
        ):
            self.assertIn(field, meta, f"Missing field: {field}")

        self.assertEqual(meta["title"], "Team Standup")
        self.assertIn("Alice", meta["speakers"])

    def test_none_fields_excluded(self):
        t = _make_transcript(meeting_link=None)
        job = _make_job()
        item = IngestionItem(id=t["id"], source_ref=t["transcript_url"])
        item._metadata_cache.update(t)

        meta = job.get_extra_metadata(item, "", {})
        self.assertNotIn("meeting_link", meta)


class TestFormatTimestamp(unittest.TestCase):
    def test_seconds(self):
        self.assertEqual(_format_timestamp(90), "01:30")

    def test_hours(self):
        self.assertEqual(_format_timestamp(3661), "01:01:01")

    def test_float_input(self):
        self.assertEqual(_format_timestamp(90.72), "01:30")


class TestBuildTranscriptFromSentences(unittest.TestCase):
    def setUp(self):
        self.job = _make_job()

    def test_groups_by_speaker(self):
        sentences = [
            {"text": "Hello.", "speaker_name": "Alice", "start_time": 0},
            {"text": "World.", "speaker_name": "Alice", "start_time": 1000},
            {"text": "Hi.", "speaker_name": "Bob", "start_time": 5000},
        ]
        result = self.job._build_transcript_from_sentences(sentences)
        self.assertIn("**Alice:** Hello. World.", result)
        self.assertIn("**Bob:** Hi.", result)

    def test_unknown_speaker(self):
        sentences = [{"text": "Test.", "speaker_name": None, "start_time": 0}]
        result = self.job._build_transcript_from_sentences(sentences)
        self.assertIn("Unknown Speaker", result)


if __name__ == "__main__":
    unittest.main()
