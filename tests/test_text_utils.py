import unittest

from utils.text import html_to_markdown, sanitize_ascii_key, slugify


class TestSlugify(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(slugify("Hello World"), "Hello_World")

    def test_non_word_chars(self):
        self.assertEqual(slugify("foo/bar.baz"), "foo_bar_baz")

    def test_max_len(self):
        result = slugify("a" * 300, max_len=255)
        self.assertEqual(len(result), 255)

    def test_strips_leading_trailing_underscores(self):
        self.assertEqual(slugify("__hello__"), "hello")

    def test_extra_replacements_mediawiki(self):
        result = slugify("Talk:Foo/Bar", extra_replacements={":": "__", "/": "_"})
        self.assertEqual(result, "Talk__Foo_Bar")

    def test_empty_value_returns_hash(self):
        result = slugify("")
        self.assertEqual(len(result), 8)
        self.assertTrue(result.isalnum())

    def test_only_non_word_non_hyphen_returns_hash(self):
        # "!!!" → re.sub → "___" → strip → "" → hash fallback
        result = slugify("!!!")
        self.assertEqual(len(result), 8)

    def test_hyphen_preserved(self):
        self.assertEqual(slugify("my-slug"), "my-slug")

    def test_unicode_word_chars_preserved(self):
        # Python's \w matches Unicode letters, so accented chars are kept
        result = slugify("héllo")
        self.assertIn("héllo", result)


class TestHtmlToMarkdown(unittest.TestCase):
    def test_strips_tags(self):
        result = html_to_markdown("<p>Hello <b>world</b></p>")
        self.assertIn("Hello", result)
        self.assertNotIn("<", result)

    def test_ignores_links(self):
        result = html_to_markdown('<a href="http://example.com">click</a>')
        self.assertNotIn("http://example.com", result)
        self.assertIn("click", result)

    def test_ignores_images(self):
        result = html_to_markdown('<img src="pic.png" alt="pic">')
        self.assertNotIn("pic.png", result)

    def test_empty_string(self):
        self.assertEqual(html_to_markdown(""), "")


class TestSanitizeAsciiKey(unittest.TestCase):
    def test_replaces_slashes_and_spaces(self):
        self.assertEqual(sanitize_ascii_key("a/b c"), "a_b_c")

    def test_preserves_dots(self):
        self.assertEqual(sanitize_ascii_key("file.name.txt"), "file.name.txt")

    def test_drops_non_ascii(self):
        self.assertEqual(sanitize_ascii_key("héllo"), "hello")


if __name__ == "__main__":
    unittest.main()
