import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI

from api import mcp_server


class _DummyNode:
    def __init__(self, text: str, metadata=None):
        self._text = text
        self.metadata = metadata or {}

    def get_text(self):
        return self._text

    def get_content(self):
        return self._text


class _DummyNodeWithScore:
    def __init__(self, text: str, score: float = 0.7, metadata=None):
        self.node = _DummyNode(text=text, metadata=metadata or {})
        self.score = score


class TestMCPServer(unittest.TestCase):
    def test_retrieve_chunks_response_returns_expected_shape(self):
        rag_engine = Mock()
        rag_engine.retrieve_top_k.return_value = [
            _DummyNodeWithScore(
                text="Chunk text",
                score=0.9,
                metadata={"source_name": "docs", "file_name": "a.md"},
            )
        ]

        result = mcp_server.retrieve_chunks_response(
            rag_engine=rag_engine,
            query="hello",
            top_k=3,
            metadata_filters={"source_name": "docs"},
        )

        rag_engine.retrieve_top_k.assert_called_once_with(
            query="hello",
            top_k=3,
            metadata={"source_name": "docs"},
        )
        self.assertIn("references", result)
        self.assertIn("raw", result)
        self.assertEqual(len(result["references"]), 1)
        self.assertTrue(result["raw"][0].startswith("Score: 0.9000 | Text: Chunk text"))

    def test_retrieve_chunks_response_validates_inputs(self):
        rag_engine = Mock()
        with self.assertRaises(ValueError):
            mcp_server.retrieve_chunks_response(rag_engine=rag_engine, query="", top_k=5)
        with self.assertRaises(ValueError):
            mcp_server.retrieve_chunks_response(rag_engine=rag_engine, query="ok", top_k=0)

    def test_rephrase_chunks_response_without_results(self):
        rag_engine = Mock()
        rag_engine.retrieve_top_k.return_value = []

        with patch.object(mcp_server, "llm", Mock()):
            result = mcp_server.rephrase_chunks_response(
                rag_engine=rag_engine,
                query="hello",
            )

        self.assertEqual(result, {"answer": "No relevant content found.", "references": []})

    def test_rephrase_chunks_response_requires_llm(self):
        rag_engine = Mock()
        rag_engine.retrieve_top_k.return_value = [_DummyNodeWithScore(text="content", score=0.5)]

        with patch.object(mcp_server, "llm", None):
            with self.assertRaises(RuntimeError):
                mcp_server.rephrase_chunks_response(rag_engine=rag_engine, query="hello")

    def test_rephrase_chunks_response_success(self):
        rag_engine = Mock()
        rag_engine.retrieve_top_k.return_value = [
            _DummyNodeWithScore(
                text="Some content",
                score=0.5,
                metadata={"source_name": "docs", "file_name": "note.md"},
            )
        ]
        llm_mock = Mock()
        llm_mock.complete.return_value = "Rephrased output"

        with patch.object(mcp_server, "llm", llm_mock):
            result = mcp_server.rephrase_chunks_response(
                rag_engine=rag_engine,
                query="question",
                top_k=2,
            )

        rag_engine.retrieve_top_k.assert_called_once_with(query="question", top_k=2)
        self.assertEqual(result["answer"], "Rephrased output")
        self.assertEqual(len(result["references"]), 1)

    def test_create_mcp_server_builds_instance(self):
        app = FastAPI()
        mcp = mcp_server.create_mcp_server(app=app, api_key="")
        self.assertIsNotNone(mcp)


if __name__ == "__main__":
    unittest.main()
