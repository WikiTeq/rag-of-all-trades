import pytest
from pydantic import ValidationError

from api.v1.rephrase_retrieval.schema import QueryRequest


def test_default_top_k():
    req = QueryRequest(query="test")
    assert req.top_k == 20


@pytest.mark.parametrize(
    "top_k,expected",
    [
        (50, 50),
        (1, 1),
        (100, 100),
    ],
)
def test_top_k_valid(top_k, expected):
    assert QueryRequest(query="test", top_k=top_k).top_k == expected


@pytest.mark.parametrize("top_k", [0, 101])
def test_top_k_invalid_raises(top_k):
    with pytest.raises(ValidationError):
        QueryRequest(query="test", top_k=top_k)
