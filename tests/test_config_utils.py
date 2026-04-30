import pytest

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
