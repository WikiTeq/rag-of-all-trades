# AGENTS

## Purpose
Rag Of All Trades is a FastAPI-based RAG microservice that ingests content into PGVector via Celery and serves retrieval and rephrase APIs using LlamaIndex.

## Architecture
- API runtime: `main.py` initializes PGVectorStore, builds `RAGQueryEngine`, configures CORS and rate limiting, and exposes `/health`.
- API routes: `api/v1/` provides `/api/v1/query/` (top-k chunks) and `/api/v1/rephrase/` (LLM rephrase).
- Ingestion: `celery_app.py` registers Celery tasks from `settings.SOURCES` and schedules them via RedBeat.
- Ingestion jobs: `tasks/base.py` handles dedupe, versioning, metadata tracking, and vector insertion.
- Vector store: `tasks/helper_classes/vector_store.py` uses LlamaIndex ingestion pipeline and PGVector.
- Metadata: `tasks/helper_classes/metadata_tracker.py` updates `models/metadata.py` and deletes from `models/embedding.py`.
- Connectors: `tasks/s3_ingestion.py`, `tasks/mediawiki_ingestion.py`, `tasks/serpapi_ingestion.py` (note factory registration below).

## Repo Map
- `main.py`: FastAPI app setup and health check.
- `api/v1/`: endpoint routing, schemas, and RAG query logic.
- `tasks/`: ingestion jobs and Celery factory.
- `utils/`: config loading, DB setup, LLM/embedding init, S3 client.
- `models/`: SQLAlchemy models for metadata and embeddings.
- `alembic/`: migrations (pgvector schema).
- `compose*.yml`, `Dockerfile`: containerized deployment.
- `tests/`: unittest-based tests.

## Configuration
- Required files in repo root:
  - `.env` (see `.env.example`)
  - `config.yaml` (see `config.yaml.example`)
- `.env` required keys: `REDIS_URL`, `POSTGRES_*`, `DATABASE_URL`, `OPENROUTER_API_KEY`, `OPENROUTER_API_BASE`.
- `config.yaml` defines `sources`, `embedding`, `inference`, and `vector_store`.
- `config.yaml` supports `${VAR}` interpolation from environment at load time (see `utils/config.py`).
- If you add or rename config keys, update `README.md`, `.env.example`, and `config.yaml.example` (per `CONTRIBUTING.md`).
- Secrets: avoid committing or printing `.env` and `config.yaml` (see `.aiignore`).

## Running (Docker)
```bash
cp .env.example .env
cp config.yaml.example config.yaml
docker compose -f compose.yml -f compose.dev.yml up -d --build
```
API is at `http://localhost:8000`, docs at `/docs`.

## Running (Python Only)
```bash
uv sync
source .venv/bin/activate
```
You still need PostgreSQL with pgvector and Redis running, plus valid `.env` and `config.yaml`.

## Tests
```bash
python -m unittest
python -m unittest tests.test_mediawiki_ingestion
```

## Migrations
```bash
alembic upgrade head
alembic revision --autogenerate -m "message"
```
Ensure `embedding.embedding_dim` in `config.yaml` is a positive integer before running migrations.

## Adding Connectors
- Implement a subclass of `tasks/base.py` `IngestionJob`.
- Register the connector type in `tasks/factory.py` (currently only `s3` and `mediawiki` are registered).
- Add config and env keys to `config.yaml.example` and `.env.example`.
- Document the connector in `README.md`.

## Gotchas
- `celery_app.py` registers tasks at import time from `settings.SOURCES`; invalid config or missing env will fail startup.
- `utils/llm_embedding.py` initializes models at import time; `inference.provider` must be `openai` or `openrouter` to enable `/rephrase/`.
- Rate limiting is controlled by `ENABLE_RATE_LIMIT`, `CHUNK_RATE_LIMIT`, and `REPHRASE_RATE_LIMIT` in `.env`.
