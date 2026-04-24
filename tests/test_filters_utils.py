import pytest

from utils.filters import path_accepted


def _accepted(path, inc_ext=None, exc_ext=None, inc_dir=None, exc_dir=None):
    return path_accepted(
        path,
        include_extensions=inc_ext,
        exclude_extensions=exc_ext,
        include_directories=inc_dir,
        exclude_directories=exc_dir,
    )


def test_no_filters_accepts_all():
    assert _accepted("src/foo.py") is True


@pytest.mark.parametrize(
    ("path", "inc_ext", "exc_ext", "expected"),
    [
        ("README.md", {".md"}, None, True),
        ("main.py", {".md"}, None, False),
        ("image.png", None, {".png"}, False),
        ("main.py", None, {".png"}, True),
    ],
)
def test_extension_filters(path, inc_ext, exc_ext, expected):
    assert _accepted(path, inc_ext=inc_ext, exc_ext=exc_ext) is expected


def test_extension_case_insensitive():
    assert _accepted("file.MD", inc_ext={".md"}) is True


def test_no_extension_file():
    assert _accepted("Makefile", inc_ext={".py"}) is False
    assert _accepted("Makefile", inc_ext={""}) is True


def test_include_directory_match():
    assert _accepted("src/foo.py", inc_dir={"src"}) is True


def test_include_directory_no_match():
    assert _accepted("docs/foo.md", inc_dir={"src"}) is False


def test_exclude_directory_match():
    assert _accepted("vendor/lib.py", exc_dir={"vendor"}) is False


def test_exclude_directory_no_match():
    assert _accepted("src/lib.py", exc_dir={"vendor"}) is True


def test_directory_prefix_not_partial():
    # "src2/foo" should not match include_dir={"src"}
    assert _accepted("src2/foo.py", inc_dir={"src"}) is False


def test_combined_filters():
    assert _accepted("src/main.py", inc_ext={".py"}, inc_dir={"src"}) is True
    assert _accepted("src/main.md", inc_ext={".py"}, inc_dir={"src"}) is False


def test_empty_sets_treated_as_no_filter():
    assert _accepted("foo.py", inc_ext=set(), exc_ext=set()) is True
