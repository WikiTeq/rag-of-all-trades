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

# Stub out celery_app before importing main so that the Celery worker
# initialisation (which reads config.yaml sources at module level) does
# not run during schema generation.
_celery_stub = types.ModuleType("celery_app")
_celery_stub.celery_app = unittest.mock.MagicMock()
sys.modules["celery_app"] = _celery_stub

from main import app  # noqa: E402

schema = app.openapi()
with open("openapi.yaml", "w") as f:
    yaml.dump(schema, f, allow_unicode=True, sort_keys=False)

print("openapi.yaml generated")
