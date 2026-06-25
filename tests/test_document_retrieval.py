import unittest
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1.document_retrieval.modules import DocumentRetriever
from api.v1.document_retrieval.routes import _get_retriever, router
from api.v1.document_retrieval.schema import (
    ChunkOut,
    DocumentOut,
    MetadataFilterInput,
)


def _make_row(text: str, ref_doc_id: str, extra: dict | None = None) -> MagicMock:
    row = MagicMock()
    row.text = text
    row.metadata_ = {"ref_doc_id": ref_doc_id, **(extra or {})}
    return row


def _make_retriever(db=None) -> DocumentRetriever:
    return DocumentRetriever(db or MagicMock())


def _make_app():
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app, _get_retriever


class TestMetadataFilterInput(unittest.TestCase):
    def test_valid_operator_accepted(self):
        f = MetadataFilterInput(name="source", operator="==", value="Jira")
        self.assertEqual(f.operator, "==")

    def test_invalid_operator_raises(self):
        with self.assertRaises(Exception):
            MetadataFilterInput(name="x", operator="INVALID", value="y")


class TestDocumentRetrieverGetDocument(unittest.TestCase):
    def test_returns_none_when_no_rows(self):
        retriever = _make_retriever()
        retriever.db.query.return_value.filter.return_value.limit.return_value.all.return_value = []
        result = retriever.get_document("missing-id", max_chunks=10, max_chunk_length=2000)
        self.assertIsNone(result)

    def test_returns_document_with_chunks(self):
        rows = [_make_row("chunk one", "doc-1"), _make_row("chunk two", "doc-1")]
        retriever = _make_retriever()
        retriever.db.query.return_value.filter.return_value.limit.return_value.all.return_value = rows
        result = retriever.get_document("doc-1", max_chunks=10, max_chunk_length=2000)
        self.assertIsNotNone(result)
        self.assertEqual(result.document_id, "doc-1")
        self.assertEqual(len(result.chunks), 2)
        self.assertEqual(result.chunks[0].text, "chunk one")

    def test_text_is_truncated(self):
        rows = [_make_row("abcdefghij", "doc-1")]
        retriever = _make_retriever()
        retriever.db.query.return_value.filter.return_value.limit.return_value.all.return_value = rows
        result = retriever.get_document("doc-1", max_chunks=10, max_chunk_length=5)
        self.assertEqual(result.chunks[0].text, "abcde")

    def test_metadata_from_first_row(self):
        rows = [_make_row("text", "doc-1", extra={"source": "Jira"})]
        retriever = _make_retriever()
        retriever.db.query.return_value.filter.return_value.limit.return_value.all.return_value = rows
        result = retriever.get_document("doc-1", max_chunks=10, max_chunk_length=2000)
        self.assertEqual(result.metadata["source"], "Jira")


class TestDocumentRetrieverGetDocuments(unittest.TestCase):
    def _setup_retriever_with_docs(self, doc_ids, chunks_per_doc):
        retriever = _make_retriever()
        db = retriever.db

        def id_row(doc_id):
            return MagicMock(ref_doc_id=doc_id)

        id_rows = [id_row(d) for d in doc_ids]

        distinct_mock = MagicMock()
        distinct_mock.limit.return_value.all.return_value = id_rows

        chunk_rows = {doc_id: [_make_row(f"chunk-{i}", doc_id) for i in range(chunks_per_doc)] for doc_id in doc_ids}

        call_count = [0]

        def side_effect_query(*args):
            q = MagicMock()
            q.distinct.return_value = distinct_mock

            def filter_side(*a):
                fq = MagicMock()
                fq.limit.return_value.all.side_effect = lambda: chunk_rows.get(doc_ids[call_count[0] % len(doc_ids)])
                call_count[0] += 1
                return fq

            q.filter.side_effect = filter_side
            return q

        db.query.side_effect = side_effect_query
        return retriever

    def test_empty_when_no_doc_ids(self):
        retriever = _make_retriever()
        distinct_mock = MagicMock()
        distinct_mock.limit.return_value.all.return_value = []
        retriever.db.query.return_value.distinct.return_value = distinct_mock
        result = retriever.get_documents([], 10, 2000, 10)
        self.assertEqual(result, [])


class TestDocumentRoutes(unittest.TestCase):
    def setUp(self):
        app, self._get_retriever = _make_app()
        self.app = app
        self._api_key_patch = patch("api.dependencies.settings.env.API_KEY", "")
        self._api_key_patch.start()
        self.client = TestClient(app, raise_server_exceptions=True)

    def _override(self, retriever):
        self.app.dependency_overrides[self._get_retriever] = lambda: retriever

    def tearDown(self):
        self.app.dependency_overrides.clear()
        self._api_key_patch.stop()

    def test_get_document_found(self):
        retriever = _make_retriever()
        retriever.get_document = MagicMock(
            return_value=DocumentOut(
                document_id="doc-1",
                metadata={"source": "Jira"},
                chunks=[ChunkOut(text="hello", metadata={})],
            )
        )
        self._override(retriever)
        resp = self.client.post("/api/v1/document", json={"document_id": "doc-1"})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["document_id"], "doc-1")
        self.assertEqual(len(data["chunks"]), 1)

    def test_get_document_not_found_returns_404(self):
        retriever = _make_retriever()
        retriever.get_document = MagicMock(return_value=None)
        self._override(retriever)
        resp = self.client.post("/api/v1/document", json={"document_id": "missing"})
        self.assertEqual(resp.status_code, 404)

    def test_get_documents_returns_list(self):
        retriever = _make_retriever()
        retriever.get_documents = MagicMock(
            return_value=[
                DocumentOut(
                    document_id="doc-1",
                    metadata={},
                    chunks=[ChunkOut(text="chunk", metadata={})],
                )
            ]
        )
        self._override(retriever)
        resp = self.client.post(
            "/api/v1/documents",
            json={
                "metadata_filters": [{"name": "source", "operator": "==", "value": "Jira"}],
                "max_documents": 5,
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.json()), 1)

    def test_get_documents_invalid_operator_returns_422(self):
        resp = self.client.post(
            "/api/v1/documents",
            json={"metadata_filters": [{"name": "x", "operator": "INVALID", "value": "y"}]},
        )
        self.assertEqual(resp.status_code, 422)

    def test_get_documents_empty_filters_returns_list(self):
        retriever = _make_retriever()
        retriever.get_documents = MagicMock(return_value=[])
        self._override(retriever)
        resp = self.client.post("/api/v1/documents", json={})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), [])
