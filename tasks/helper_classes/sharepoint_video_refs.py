import json
from collections import Counter
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

_WEBPART_DATA_ATTRIBUTE = "data-sp-webpartdata"


@dataclass(frozen=True)
class VideoReferences:
    """Video URLs found on one page, in first-occurrence order, and why other values were skipped."""

    urls: tuple[str, ...] = ()
    skipped: Counter = field(default_factory=Counter)


class _WebPartDataCollector(HTMLParser):
    """Collect the ``data-sp-webpartdata`` attribute of every web part, in document order."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.values: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if name == _WEBPART_DATA_ATTRIBUTE and value:
                self.values.append(value)


class SharePointVideoReferences:
    """Find SharePoint-hosted video files referenced by the web parts of a modern page.

    PROVISIONAL: the parser reads the JSON in the ``data-sp-webpartdata`` attribute of
    ``CanvasContent1`` and treats every URL string in it whose path ends in a known video
    extension as a reference. Confirmed on a live tenant: the Video web part with a single
    video. The File viewer and Stream web part layouts are not confirmed. The host allow-list
    keeps external embeds (YouTube, Vimeo, other tenants) from ever being downloaded.

    TODO: list the page web parts with the Microsoft Graph webParts API instead of parsing
    the page HTML, see https://learn.microsoft.com/en-us/graph/api/webpart-list. On the test
    tenant the Video web part returns the file URL in ``data.properties.file``.
    """

    @staticmethod
    def allowed_hosts(site_host: str | None) -> frozenset[str]:
        """Return the hosts of the page's own tenant: the site host and its ``-my`` OneDrive host."""
        host = (site_host or "").strip().lower()
        label, _, rest = host.partition(".")
        if not label or not rest:
            return frozenset()
        return frozenset({host, f"{label}-my.{rest}"})

    @staticmethod
    def parse(raw_html: str, allowed_hosts: frozenset[str], video_extensions: frozenset[str]) -> VideoReferences:
        """Return the video references of a page.

        Args:
            raw_html: The ``CanvasContent1`` HTML of the page.
            allowed_hosts: Hosts whose files may be downloaded. A video on any other host is skipped.
            video_extensions: Lowercase extensions with a leading dot, for example ``.mp4``.
        """
        collector = _WebPartDataCollector()
        collector.feed(raw_html or "")
        collector.close()

        urls: list[str] = []
        skipped: Counter = Counter()
        for value in collector.values:
            try:
                data = json.loads(value)
            except ValueError:
                skipped["malformed web part data"] += 1
                continue
            for candidate in SharePointVideoReferences._iter_strings(data):
                try:
                    parsed = urlparse(candidate)
                    hostname = parsed.hostname
                except ValueError:
                    skipped["malformed video URL"] += 1
                    continue
                if parsed.scheme not in ("http", "https") or not hostname:
                    continue
                if PurePosixPath(unquote(parsed.path)).suffix.lower() not in video_extensions:
                    continue
                if hostname.lower() not in allowed_hosts:
                    skipped["external or other-tenant video"] += 1
                    continue
                if candidate not in urls:
                    urls.append(candidate)
        return VideoReferences(urls=tuple(urls), skipped=skipped)

    @staticmethod
    def _iter_strings(node: Any):
        """Yield every string value in a JSON structure, in document order."""
        if isinstance(node, str):
            yield node
        elif isinstance(node, dict):
            for child in node.values():
                yield from SharePointVideoReferences._iter_strings(child)
        elif isinstance(node, list):
            for child in node:
                yield from SharePointVideoReferences._iter_strings(child)
