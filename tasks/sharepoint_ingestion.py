import hashlib
import logging
import math
import re
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from llama_index.core.schema import Document
from llama_index.readers.microsoft_sharepoint import SharePointReader, SharePointType

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.ingestion_result import IngestionItemResult, IngestionItemStatus
from tasks.helper_classes.sharepoint_video_refs import SharePointVideoReferences
from utils.graph_client import GraphClient
from utils.http import UrlRedactingFilter
from utils.parse import parse_bool, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)


class _VideoSkipped(Exception):
    """A page video was skipped. ``reason`` is safe to log: it never contains a URL."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class SharePointIngestionJob(IngestionJob):
    """Ingestion connector for Microsoft SharePoint.

    Fetches files from SharePoint document libraries or site pages using the
    LlamaIndex SharePoint reader and stores them in the vector store.

    Supports two modes (configured via ``sharepoint_type``):
      - ``file`` (default): load files from a SharePoint drive / folder
      - ``page``: load SharePoint site pages

    For file mode, files are discovered without downloading (``list_resources`` +
    ``get_resource_info``) and downloaded one at a time during ingestion
    (``load_resource``), avoiding the bulk-download bottleneck of ``load_data``.

    Configuration (config.yaml):
        - config.client_id: Azure app client ID (required)
        - config.client_secret: Azure app client secret (required)
        - config.tenant_id: Azure tenant ID (required)
        - config.sharepoint_site_name: SharePoint site name (required unless sharepoint_site_id is set)
        - config.sharepoint_site_id: SharePoint site ID (required unless sharepoint_site_name is set)
        - config.sharepoint_host_name: SharePoint host, e.g. contoso.sharepoint.com (optional, passed through to reader)
        - config.sharepoint_relative_url: Relative URL of the site, e.g. /sites/MySite (optional, passed through to reader)
        - config.sharepoint_folder_path: Folder path within the drive (optional)
        - config.sharepoint_folder_id: Folder ID within the drive (optional)
        - config.drive_name: Name of the document library / drive (optional)
        - config.sharepoint_type: "file" or "page" (optional, default "file")
        - config.recursive: traverse subfolders recursively (optional, default true).
          Ignored when sharepoint_folder_path is unset: the upstream reader always
          recurses from the drive root (run-llama/llama_index#22320).
        - config.transcribe_videos: transcribe video files (file mode) and the SharePoint-hosted
          videos referenced by a page (page mode) and index the speech (optional, default false)
        - config.max_video_mb: largest video to download, in MiB (optional, default 500)

    Video transcripts: the converter output has its ``[mm:ss-mm:ss]`` segment prefixes removed. A
    spoken sentence that starts with a timestamp-shaped token cannot be told apart from a prefix
    in the plain converter output, so a prefix removal can also drop such a token from the speech.
    A page keeps the checksum of its own text, so a page whose text does not change is not
    enriched again, even if an embedded video changed. Switching ``transcribe_videos`` changes the
    checksum of every page, so each page is ingested once more.
    """

    DEFAULT_MAX_VIDEO_MB = 500
    PAGE_VIDEOS_CHECKSUM_SUFFIX = "\0transcribe_videos"
    _SEGMENT_PREFIX = re.compile(r"^\[(?:\d+:)?\d{2}:\d{2}-(?:\d+:)?\d{2}:\d{2}\] ?")

    @property
    def source_type(self) -> str:
        return "sharepoint"

    @property
    def content_format(self) -> str:
        return "text"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.client_id = cfg.get("client_id", "").strip()
        if not self.client_id:
            raise ValueError("client_id is required in SharePoint connector config")

        self.client_secret = cfg.get("client_secret", "").strip()
        if not self.client_secret:
            raise ValueError("client_secret is required in SharePoint connector config")

        self.tenant_id = cfg.get("tenant_id", "").strip()
        if not self.tenant_id:
            raise ValueError("tenant_id is required in SharePoint connector config")

        self.sharepoint_site_name: str | None = cfg.get("sharepoint_site_name", "").strip() or None
        self.sharepoint_site_id: str | None = cfg.get("sharepoint_site_id", "").strip() or None
        if not self.sharepoint_site_name and not self.sharepoint_site_id:
            raise ValueError(
                "SharePoint connector requires either sharepoint_site_name or sharepoint_site_id in config"
            )
        self.sharepoint_host_name: str | None = cfg.get("sharepoint_host_name", "").strip() or None
        self.sharepoint_relative_url: str | None = cfg.get("sharepoint_relative_url", "").strip() or None
        self.sharepoint_folder_path: str | None = cfg.get("sharepoint_folder_path", "").strip() or None
        self.sharepoint_folder_id: str | None = cfg.get("sharepoint_folder_id", "").strip() or None
        self.drive_name: str | None = cfg.get("drive_name", "").strip() or None

        _type_map = {"file": SharePointType.DRIVE, "page": SharePointType.PAGE}
        sharepoint_type_raw = cfg.get("sharepoint_type", "file").strip().lower()
        if sharepoint_type_raw not in _type_map:
            raise ValueError(f"Invalid sharepoint_type {sharepoint_type_raw!r}; expected one of {list(_type_map)}")
        self.sharepoint_type = _type_map[sharepoint_type_raw]

        if (
            self.sharepoint_type == SharePointType.DRIVE
            and self.sharepoint_folder_path
            and not self.sharepoint_site_name
        ):
            raise ValueError(
                "sharepoint_site_name is required when sharepoint_folder_path is set in DRIVE mode. "
                "list_resources() builds the path as os.path.join(sharepoint_site_name, "
                "sharepoint_folder_path) and raises TypeError when sharepoint_site_name is None."
            )

        self.recursive = parse_bool(cfg.get("recursive", True), default=True)

        self.transcribe_videos = parse_bool(cfg.get("transcribe_videos"), default=False)
        if self.transcribe_videos:
            self.max_video_bytes = self._parse_max_video_bytes(cfg.get("max_video_mb", self.DEFAULT_MAX_VIDEO_MB))
        else:
            # A disabled connector never reads the limit, so a bad value must not stop ordinary ingestion.
            self.max_video_bytes = self._parse_max_video_bytes(self.DEFAULT_MAX_VIDEO_MB)
        self._graph: GraphClient | None = None
        if self.transcribe_videos and self.sharepoint_type == SharePointType.PAGE:
            self._graph = GraphClient(
                self.client_id,
                self.client_secret,
                self.tenant_id,
                max_file_size_bytes=self.max_video_bytes,
            )
        if self.transcribe_videos and self.sharepoint_type == SharePointType.DRIVE:
            self._redact_reader_logs()

        logger.info(
            f"Initialized SharePoint connector: site={self.sharepoint_site_name!r}, "
            f"type={sharepoint_type_raw!r}, recursive={self.recursive}, transcribe_videos={self.transcribe_videos}"
        )

    @staticmethod
    def _redact_reader_logs() -> None:
        """Keep signed download URLs out of the reader's own error logs.

        The reader logs the raw exception of a failed download, and a transport error names the
        signed URL. A filter on the reader's logger is the only place to remove it.
        """
        reader_logger = logging.getLogger(SharePointReader.__module__)
        if not any(isinstance(log_filter, UrlRedactingFilter) for log_filter in reader_logger.filters):
            reader_logger.addFilter(UrlRedactingFilter())

    @staticmethod
    def _parse_max_video_bytes(value: Any) -> int:
        """Convert ``max_video_mb`` (MiB) to bytes. Reject booleans, non-numbers, and non-positive or infinite values."""
        if isinstance(value, bool):
            raise ValueError(f"Invalid max_video_mb {value!r}; expected a positive number")
        try:
            megabytes = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid max_video_mb {value!r}; expected a positive number") from None
        if not math.isfinite(megabytes) or megabytes <= 0:
            raise ValueError(f"Invalid max_video_mb {value!r}; expected a positive number")
        return int(megabytes * 1024 * 1024)

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover SharePoint items and yield one IngestionItem per document/page."""
        self._reader = SharePointReader(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            sharepoint_site_name=self.sharepoint_site_name,
            sharepoint_site_id=self.sharepoint_site_id,
            sharepoint_host_name=self.sharepoint_host_name,
            sharepoint_relative_url=self.sharepoint_relative_url,
            sharepoint_type=self.sharepoint_type,
            drive_name=self.drive_name,
        )
        logger.info(f"[{self.source_name}] Loading SharePoint content")
        if self.sharepoint_type == SharePointType.DRIVE:
            if not self.sharepoint_site_name and self.sharepoint_site_id:
                logger.warning(
                    "[%s] DRIVE mode with sharepoint_site_id only (no sharepoint_site_name): "
                    "path-prefix stripping in the reader assumes a site-name prefix, so "
                    "Graph root:/{path} lookups may be unreliable. Prefer setting "
                    "sharepoint_site_name (optionally alongside sharepoint_site_id).",
                    self.source_name,
                )
            try:
                yield from self._list_drive_items()
            finally:
                self._close_graph()
        else:
            try:
                yield from self._list_page_items()
            finally:
                self._close_graph()

    def _close_graph(self) -> None:
        """Close the Graph HTTP sessions once the run has no more items to process."""
        if self._graph is not None:
            self._graph.close()

    def _list_drive_items(self) -> Iterator[IngestionItem]:
        list_kwargs: dict[str, Any] = {
            k: v
            for k, v in {
                "sharepoint_site_name": self.sharepoint_site_name,
                "sharepoint_site_id": self.sharepoint_site_id,
                "sharepoint_folder_path": self.sharepoint_folder_path,
                "sharepoint_folder_id": self.sharepoint_folder_id,
            }.items()
            if v is not None
        }
        list_kwargs["recursive"] = self.recursive

        paths = self._reader.list_resources(**list_kwargs)
        logger.info(f"[{self.source_name}] Found {len(paths)} resource(s) in SharePoint drive")

        for path in paths:
            resource_id = str(path)
            try:
                info = self._reader.get_resource_info(resource_id)
                file_path = info["file_path"]
            except KeyError:
                logger.warning(
                    "[%s] Resource info for %r missing file_path; skipping",
                    self.source_name,
                    resource_id,
                )
                continue
            except Exception:
                logger.exception(
                    "[%s] Failed to get resource info for %r; skipping",
                    self.source_name,
                    resource_id,
                )
                continue

            last_modified = parse_timestamp(info.get("modified_at"))
            if last_modified is None:
                logger.warning(
                    "[%s] Missing modified_at for %r; using now()",
                    self.source_name,
                    resource_id,
                )
                last_modified = datetime.now(UTC)

            # Prefer the Graph driveItem.id (file_id) for a stable item_id that
            # survives renames and moves. get_resource_info() in reader 0.8.1 does
            # not expose file_id directly, but some reader versions / configs may
            # include it. Fall back to file_path when absent (v1 limitation).
            stable_id = info.get("file_id") or file_path
            item_id = f"sharepoint:{self.source_name}:{stable_id}"
            yield IngestionItem(id=item_id, source_ref=info, last_modified=last_modified)

    def _list_page_items(self) -> Iterator[IngestionItem]:
        load_kwargs: dict[str, Any] = {
            k: v
            for k, v in {
                "sharepoint_site_name": self.sharepoint_site_name,
                "sharepoint_site_id": self.sharepoint_site_id,
            }.items()
            if v is not None
        }

        docs = self._reader.load_data(**load_kwargs)
        logger.info(f"[{self.source_name}] Loaded {len(docs)} page(s) from SharePoint")

        for doc in docs:
            stable_id = (
                doc.metadata.get("file_id") or doc.id_ or doc.metadata.get("file_path") or doc.metadata.get("file_name")
            )
            item_id = f"sharepoint:{self.source_name}:{stable_id}"
            raw_ts = (
                doc.metadata.get("lastModifiedDateTime")
                or doc.metadata.get("last_modified_datetime")
                or doc.metadata.get("last_modified")
            )
            parsed = parse_timestamp(raw_ts)
            if parsed is None:
                if raw_ts is not None:
                    logger.debug(
                        "[%s] Could not parse last_modified %r for %s; falling back to now()",
                        self.source_name,
                        raw_ts,
                        stable_id,
                    )
                else:
                    logger.debug(
                        "[%s] No last_modified metadata for %s; falling back to now()",
                        self.source_name,
                        stable_id,
                    )
                last_modified = datetime.now(UTC)
            else:
                last_modified = parsed.astimezone(UTC)
            yield IngestionItem(id=item_id, source_ref=doc, last_modified=last_modified)

    def get_raw_content(self, item: IngestionItem) -> str | IngestionItemResult:
        """Return document text; for file mode, downloads one file at a time."""
        if self.sharepoint_type == SharePointType.DRIVE:
            if self.transcribe_videos and self._is_video_path(item.source_ref["file_path"]):
                return self._get_drive_video_content(item)
            docs = self._reader.load_resource(item.source_ref["file_path"])
            # The reader returns one document per sheet for Excel workbooks.
            pieces = ((doc.text or "").strip() for doc in docs)
            return "\n\n".join(piece for piece in pieces if piece)
        if self.transcribe_videos:
            return self._append_page_video_transcripts(item.source_ref)
        return item.source_ref.text or ""

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return the checksum that lets the pipeline skip unchanged items before any download.

        DRIVE items use the Graph etag, so an unchanged file is not downloaded or converted.
        PAGE items return None (the pipeline then hashes the content) unless video transcription
        is on. Then the checksum is the MD5 of the page's own text and ``PAGE_VIDEOS_CHECKSUM_SUFFIX``,
        taken before transcripts are appended, so an unchanged page does no video work. The suffix
        changes the checksum of every page when ``transcribe_videos`` is switched, so the next run
        ingests each page again.
        """
        if self.sharepoint_type == SharePointType.DRIVE:
            etag = item.source_ref.get("etag")
            return str(etag) if etag is not None else None
        if self.transcribe_videos:
            text = (item.source_ref.text or "") + self.PAGE_VIDEOS_CHECKSUM_SUFFIX
            return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()
        return None

    @classmethod
    def _is_video_path(cls, path: str) -> bool:
        return Path(path).suffix.lower() in cls.VIDEO_EXTENSIONS

    @classmethod
    def _strip_segment_prefixes(cls, transcript: str) -> str:
        """Remove the one converter-generated time prefix at the start of each transcript segment."""
        return "\n\n".join(cls._SEGMENT_PREFIX.sub("", segment, count=1) for segment in transcript.split("\n\n"))

    def _transcribe_video(self, content: bytes, suffix: str) -> str:
        """Convert video bytes to a transcript without time prefixes. The temporary files are always removed."""
        with tempfile.TemporaryDirectory(prefix="sharepoint_video_") as tmp_dir:
            video_path = Path(tmp_dir) / f"video{suffix}"
            video_path.write_bytes(content)
            transcript = self.convert_video_to_markdown(video_path)
        return self._strip_segment_prefixes(transcript).strip()

    def _get_drive_video_content(self, item: IngestionItem) -> IngestionItemResult:
        """Download and transcribe one video file.

        Every failure returns FAIL, so the item is retried. A video without speech returns an
        explicit empty SUCCESS, so the pipeline records it once and does not transcribe it again.
        """
        info = item.source_ref
        file_path = info["file_path"]
        declared_size = info.get("size")
        if isinstance(declared_size, int) and not isinstance(declared_size, bool):
            if declared_size > self.max_video_bytes:
                logger.warning("[%s] Video %s exceeds max_video_mb; skipping", self.source_name, item.id)
                return IngestionItemResult(IngestionItemStatus.FAIL, errors=("video exceeds max_video_mb",))
        try:
            content = self._reader.read_file_content(Path(file_path))
            if len(content) > self.max_video_bytes:
                logger.warning("[%s] Video %s exceeds max_video_mb; skipping", self.source_name, item.id)
                return IngestionItemResult(IngestionItemStatus.FAIL, errors=("video exceeds max_video_mb",))
            transcript = self._transcribe_video(content, Path(file_path).suffix.lower())
        except Exception as exc:
            reason = f"video transcription failed ({type(exc).__name__})"
            logger.warning("[%s] %s for %s", self.source_name, reason, item.id)
            return IngestionItemResult(IngestionItemStatus.FAIL, errors=(reason,))
        # A video without speech is a successful, empty result, as in the Directory connector.
        return IngestionItemResult(IngestionItemStatus.SUCCESS, transcript)

    def _append_page_video_transcripts(self, doc: Document) -> str | IngestionItemResult:
        """Return the page text followed by the transcript of each SharePoint-hosted video on the page.

        Any failure while finding or transcribing videos leaves the page text, and the transcripts
        that already succeeded, in the result. A page with no text whose videos all converted
        without speech returns an explicit empty SUCCESS, so the pipeline records it once.
        """
        text = doc.text or ""
        sections: list[str] = []
        succeeded = failed = 0
        try:
            succeeded, failed = self._collect_page_video_sections(doc, sections)
        except Exception as exc:
            logger.warning(
                "[%s] Could not read videos of page %s (%s); indexing page text only",
                self.source_name,
                doc.id_,
                type(exc).__name__,
            )
        if not sections:
            if not text.strip() and succeeded and not failed:
                return IngestionItemResult(IngestionItemStatus.SUCCESS, "")
            return text
        return "\n\n".join(part for part in (text.rstrip(), *sections) if part)

    def _collect_page_video_sections(self, doc: Document, sections: list[str]) -> tuple[int, int]:
        """Append the section of each page video to ``sections``. Return how many videos succeeded and failed."""
        site_id = doc.metadata.get("site_id")
        page_id = doc.metadata.get("page_id") or doc.id_
        if not site_id or not page_id:
            logger.warning("[%s] Page %s has no site or page ID; skipping videos", self.source_name, doc.id_)
            return 0, 0
        page = self._reader.get_page_text(site_id=site_id, list_id="", page_id=page_id)
        references = SharePointVideoReferences.parse(
            page.get("rawHtml") or "",
            SharePointVideoReferences.allowed_hosts(site_id.split(",")[0]),
            self.VIDEO_EXTENSIONS,
        )
        for reason, count in references.skipped.items():
            logger.warning("[%s] Page %s: skipped %d video reference(s): %s", self.source_name, page_id, count, reason)

        seen: set[tuple[str, str]] = set()
        succeeded = failed = 0
        for url in references.urls:
            try:
                drive_item = self._graph.get_item_by_web_url(url)
                identity = (drive_item["parentReference"]["driveId"], drive_item["id"])
                if identity in seen:
                    continue
                seen.add(identity)
                section = self._transcribe_page_video(drive_item)
            except Exception as exc:
                logger.warning(
                    "[%s] Skipping a video on page %s: %s (%s)",
                    self.source_name,
                    page_id,
                    getattr(exc, "reason", "video could not be processed"),
                    type(exc).__name__,
                )
                failed += 1
                continue
            succeeded += 1
            if section:
                sections.append(section)
        return succeeded, failed

    def _transcribe_page_video(self, drive_item: dict[str, Any]) -> str | None:
        """Download and transcribe one drive item. Return a labelled section, or None without speech."""
        name = drive_item.get("name") or ""
        suffix = Path(name).suffix.lower()
        if suffix not in self.VIDEO_EXTENSIONS:
            raise _VideoSkipped("not a supported video file")
        size = drive_item.get("size")
        if isinstance(size, int) and not isinstance(size, bool) and size > self.max_video_bytes:
            raise _VideoSkipped("video exceeds max_video_mb")
        download_url = drive_item.get("@microsoft.graph.downloadUrl")
        if not download_url:
            raise _VideoSkipped("no download URL")
        transcript = self._transcribe_video(self._graph.download_content(download_url), suffix)
        if not transcript:
            return None
        heading = f"## Video transcript: {name}"
        web_url = drive_item.get("webUrl")
        return f"{heading}\n\n{web_url}\n\n{transcript}" if web_url else f"{heading}\n\n{transcript}"

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe unique name for this item."""
        if self.sharepoint_type == SharePointType.DRIVE:
            return slugify(f"{self.source_name}_{item.source_ref['file_path']}")
        doc = item.source_ref
        file_path = doc.metadata.get("file_path") or doc.metadata.get("file_name") or doc.id_
        return slugify(f"{self.source_name}_{file_path}")

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return SharePoint-specific metadata fields."""
        if self.sharepoint_type == SharePointType.DRIVE:
            info = item.source_ref
            file_path = info.get("file_path", "")
            file_name = Path(file_path).name if file_path else ""
            return {
                "file_path": file_path,
                "file_name": file_name,
                "url": info.get("url", ""),
                "title": file_name,
            }
        doc = item.source_ref
        return {
            "file_path": doc.metadata.get("file_path", ""),
            "file_name": doc.metadata.get("file_name", ""),
            "url": doc.metadata.get("url", ""),
            "title": doc.metadata.get("title", "") or doc.metadata.get("file_name", ""),
        }
