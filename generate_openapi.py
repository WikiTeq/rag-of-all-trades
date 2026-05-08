#!/usr/bin/env python
"""Generate openapi.yaml from the FastAPI app definition.

Run this script whenever the API schemas change:
    python generate_openapi.py

The pre-commit hook runs this automatically when Python files are modified.
"""

import sys
import types
import unittest.mock

import yaml

# Stub modules that perform I/O or heavy initialisation at import time so the
# script can run without a live Celery broker, database, or embedding model.
for _mod, _attrs in (
    ("celery_app", ["celery_app"]),
    ("utils.llm_embedding", ["embed_model", "llm"]),
    ("api.v1.chunk_retrieval.modules", ["RAGQueryEngine"]),
):
    if _mod not in sys.modules:
        _stub = types.ModuleType(_mod)
        for _attr in _attrs:
            setattr(_stub, _attr, unittest.mock.MagicMock())
        sys.modules[_mod] = _stub

from main import app  # noqa: E402

schema = app.openapi()
with open("openapi.yaml", "w", encoding="utf-8") as f:
    yaml.dump(schema, f, allow_unicode=True, sort_keys=False)

print("openapi.yaml generated")
