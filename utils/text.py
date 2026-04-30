from __future__ import annotations

import hashlib
import re
import unicodedata

import html2text


def slugify(
    value: str,
    max_len: int = 255,
    extra_replacements: dict[str, str] | None = None,
) -> str:
    """Return a filesystem-safe slug from value.

    extra_replacements are applied first (in order), then any remaining
    non-word characters are replaced with underscores.  The result is
    stripped of leading/trailing underscores and truncated to max_len.
    If the result is empty after sanitisation a short md5 hash is used
    as a fallback (collision-avoidance only, not cryptographic).

    Example — MediaWiki namespace preservation:
        slugify("Talk:Foo/Bar", extra_replacements={":" : "__", "/": "_"})
        → "Talk__Foo_Bar"

        Dots are preserved, so "Foo.Bar" → "Foo.Bar".
    """
    result = value
    for src, dst in (extra_replacements or {}).items():
        result = result.replace(src, dst)
    result = re.sub(r"[^\w\-_.]", "_", result)
    result = result.strip("_")[:max_len]
    if not result:
        result = hashlib.md5(value.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    return result


def sanitize_ascii_key(value: str, max_len: int = 255) -> str:
    """Sanitize value into a filesystem-safe ASCII key.

    This is stricter than slugify(): it first normalizes to ASCII (dropping
    non-ASCII characters), preserves dots, and removes any remaining disallowed
    characters. Used for connectors that ingest file paths/keys where stable
    ASCII identifiers are preferred (e.g. S3 keys, local file paths).
    """
    result = unicodedata.normalize("NFKD", value)
    result = result.encode("ascii", "ignore").decode("ascii")
    result = re.sub(r"[ \\/]+", "_", result)
    result = re.sub(r"[^a-zA-Z0-9\-_\.]", "", result)
    return result[:max_len]


def html_to_markdown(html: str) -> str:
    """Convert an HTML string to plain Markdown using consistent settings.

    Ignores hyperlinks and images so the output is clean prose.
    """
    h = html2text.HTML2Text()
    h.ignore_links = True
    h.ignore_images = True
    h.body_width = 0
    return h.handle(html).strip()
