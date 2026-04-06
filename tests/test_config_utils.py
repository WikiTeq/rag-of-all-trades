import pytest

from utils.config import parse_bool
from utils.config_validation import mutually_exclusive, require_one_of


def test_mutually_exclusive_both_set_raises():
    with pytest.raises(ValueError, match="mutually exclusive"):
        mutually_exclusive({"a": "x", "b": "y"}, "a", "b", "TestConnector")


@pytest.mark.parametrize("cfg", [{}, {"a": "x"}, {"b": "y"}])
def test_mutually_exclusive_valid_combinations_ok(cfg):
    mutually_exclusive(cfg, "a", "b", "TestConnector")


def test_mutually_exclusive_falsy_values_treated_as_unset():
    mutually_exclusive({"a": "", "b": None}, "a", "b", "TestConnector")


@pytest.mark.parametrize(
    ("cfg", "keys", "expected"),
    [
        ({"x": "val"}, ["x", "y", "z"], "x"),
        ({"x": "", "y": "val", "z": None}, ["x", "y", "z"], "y"),
        ({"a": 1}, ("a", "b"), "a"),
    ],
)
def test_require_one_of_valid_returns_key(cfg, keys, expected):
    assert require_one_of(cfg, keys, "TestConnector") == expected


def test_require_one_of_none_set_raises():
    with pytest.raises(ValueError, match="none"):
        require_one_of({}, ["x", "y"], "TestConnector")


def test_require_one_of_two_set_raises():
    with pytest.raises(ValueError):
        require_one_of({"x": "v", "y": "v"}, ["x", "y", "z"], "TestConnector")


@pytest.mark.parametrize(("value", "expected"), [(True, True), (False, False)])
def test_parse_bool_native_bool(value, expected):
    assert parse_bool(value) is expected


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", "YES", "on", "ON"])
def test_parse_bool_truthy_strings(value):
    assert parse_bool(value) is True


@pytest.mark.parametrize("value", ["false", "False", "FALSE", "0", "no", "NO", "off", "OFF"])
def test_parse_bool_falsy_strings(value):
    assert parse_bool(value) is False


@pytest.mark.parametrize(("default", "expected"), [(False, False), (True, True)])
def test_parse_bool_none_returns_default(default, expected):
    assert parse_bool(None, default=default) is expected


@pytest.mark.parametrize(("value", "expected"), [(1, True), (0, False)])
def test_parse_bool_int(value, expected):
    assert parse_bool(value) is expected


@pytest.mark.parametrize("value", ["maybe", 2, []])
def test_parse_bool_invalid_raises(value):
    with pytest.raises(ValueError):
        parse_bool(value)


@pytest.mark.parametrize(("value", "expected"), [("  true  ", True), ("  false  ", False)])
def test_parse_bool_whitespace_stripped(value, expected):
    assert parse_bool(value) is expected
