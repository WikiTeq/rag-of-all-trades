import unittest

from pydantic import ValidationError

from api.v1.chunk_retrieval.schema import QueryRequest


class TestChunkSchemaTopK(unittest.TestCase):
    def test_default_top_k(self):
        req = QueryRequest(query="test")
        self.assertEqual(req.top_k, 20)

    def test_top_k_valid(self):
        for top_k, expected in [(50, 50), (1, 1), (100, 100)]:
            with self.subTest(top_k=top_k):
                self.assertEqual(QueryRequest(query="test", top_k=top_k).top_k, expected)

    def test_top_k_invalid_raises(self):
        for top_k in [0, 101]:
            with self.subTest(top_k=top_k):
                with self.assertRaises(ValidationError):
                    QueryRequest(query="test", top_k=top_k)

    def test_top_k_bounds_exported_to_json_schema(self):
        top_k_schema = QueryRequest.model_json_schema()["properties"]["top_k"]
        self.assertEqual(top_k_schema["minimum"], 1)
        self.assertEqual(top_k_schema["maximum"], 100)
