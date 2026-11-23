import html
import json
import unittest

from tasks.helper_classes.sharepoint_video_refs import SharePointVideoReferences

VIDEO_EXTENSIONS = frozenset({".mp4", ".avi", ".mov", ".mkv", ".webm"})
HOSTS = SharePointVideoReferences.allowed_hosts("contoso.sharepoint.com")


def _page(*webparts, raw=()):
    parts = [f'<div data-sp-webpartdata="{html.escape(json.dumps(part))}"></div>' for part in webparts]
    parts += [f'<div data-sp-webpartdata="{html.escape(value)}"></div>' for value in raw]
    return "".join(parts)


def _parse(raw_html):
    return SharePointVideoReferences.parse(raw_html, HOSTS, VIDEO_EXTENSIONS)


class TestAllowedHosts(unittest.TestCase):
    def test_site_host_and_my_host(self):
        self.assertEqual(HOSTS, frozenset({"contoso.sharepoint.com", "contoso-my.sharepoint.com"}))

    def test_missing_or_bare_host_allows_nothing(self):
        for host in (None, "", "localhost"):
            with self.subTest(host=host):
                self.assertEqual(SharePointVideoReferences.allowed_hosts(host), frozenset())


class TestParse(unittest.TestCase):
    def test_returns_urls_in_page_order_without_duplicates(self):
        a = "https://contoso.sharepoint.com/sites/s/Docs/a.mp4"
        b = "https://contoso-my.sharepoint.com/personal/u/Documents/b.WEBM"
        result = _parse(_page({"properties": {"file": a}, "links": {"file": a}}, {"properties": {"file": b}}, {"x": a}))
        self.assertEqual(result.urls, (a, b))

    def test_percent_encoded_path_extension_is_recognised(self):
        url = "https://contoso.sharepoint.com/sites/s/My%20Docs/clip%20one.mov?web=1"
        self.assertEqual(_parse(_page({"file": url})).urls, (url,))

    def test_ignores_non_video_values(self):
        result = _parse(_page({"file": "https://contoso.sharepoint.com/sites/s/a.docx", "title": "clip.mp4"}))
        self.assertEqual(result.urls, ())
        self.assertEqual(sum(result.skipped.values()), 0)

    def test_external_and_other_tenant_videos_are_skipped_and_counted(self):
        result = _parse(
            _page(
                {"file": "https://www.youtube.com/watch.mp4"},
                {"file": "https://other.sharepoint.com/sites/s/a.mp4"},
            )
        )
        self.assertEqual(result.urls, ())
        self.assertEqual(result.skipped["external or other-tenant video"], 2)

    def test_malformed_web_part_data_does_not_hide_later_videos(self):
        url = "https://contoso.sharepoint.com/sites/s/a.mp4"
        result = _parse(_page({"file": url}, raw=["{not json"]))
        self.assertEqual(result.urls, (url,))
        self.assertEqual(result.skipped["malformed web part data"], 1)

    def test_unparsable_url_does_not_hide_later_videos(self):
        url = "https://contoso.sharepoint.com/sites/s/a.mp4"
        result = _parse(_page({"bad": "https://[broken/video.mp4", "file": url}))
        self.assertEqual(result.urls, (url,))
        self.assertEqual(result.skipped["malformed video URL"], 1)

    def test_empty_html(self):
        self.assertEqual(_parse("").urls, ())
        self.assertEqual(_parse(None).urls, ())


if __name__ == "__main__":
    unittest.main()
