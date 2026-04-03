from unittest.mock import Mock

from utils.api import format_chunks


def _make_node(text: str, score: float | None) -> Mock:
    node = Mock()
    node.node.get_text.return_value = text
    node.score = score
    return node


def test_format_chunks_with_score():
    nodes = [_make_node("hello world", 0.1234)]
    result = format_chunks(nodes)
    assert result == ["Score: 0.1234 | Text: hello world"]


def test_format_chunks_score_none():
    nodes = [_make_node("hello world", None)]
    result = format_chunks(nodes)
    assert result == ["Score: n/a | Text: hello world"]


def test_format_chunks_empty():
    assert format_chunks([]) == []


def test_format_chunks_multiple():
    nodes = [
        _make_node("first", 0.9),
        _make_node("second", None),
        _make_node("third", 0.1),
    ]
    result = format_chunks(nodes)
    assert result[0] == "Score: 0.9000 | Text: first"
    assert result[1] == "Score: n/a | Text: second"
    assert result[2] == "Score: 0.1000 | Text: third"
