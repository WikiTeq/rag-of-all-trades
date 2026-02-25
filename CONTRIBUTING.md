# Contributing

Thanks for your interest in contributing to Rag Of All Trades! This guide explains how to set up a dev
environment, run tests, and what we expect in pull requests.

## Getting started

- Read `README.md` for the project overview and usage.
- This project targets Python 3.11.

### Local setup (Docker Compose)

For local development it's suggested to use `compose.dev.yaml` override
file because:

1. It exposes ports of supplimentary services (db, redis) for your access
2. It forces image to be built from the `Dockerfile` rather than downloaded from the container registry
3. It mounts your project directory as `/app` into the runnig container allowing for easier edits
4. It enables live reloading of the application when files change (via `uvicorn --reload`) for API container

```bash
cp .env.example .env
cp config.yaml.example config.yaml
# edit .env and config.yaml as needed
docker compose -f compose.yaml -f compose.dev.yaml up -d --build
```

The API will be available on `http://localhost:8000` and docs on `http://localhost:8000/docs`.

### Local setup (Python only)

If you prefer to run services manually:

```bash
uv sync
source .venv/bin/activate
```

You still need PostgreSQL with pgvector and Redis running, plus valid `.env` and `config.yaml` files.

## Running tests

Tests use the standard library `unittest` module.

```bash
python -m unittest
```

To run a single test file:

```bash
python -m unittest tests.test_mediawiki_ingestion
```

## Migrations

Database migrations are managed with Alembic:

```bash
alembic upgrade head
alembic revision --autogenerate -m "your message"
```

If you change models or database-related config, update the migration history accordingly.

## Project conventions

- Keep changes small and focused.
- Follow the existing code style and naming conventions.
- Add or update tests for behavior changes when feasible.
- If you add or rename config keys, update:
  - `README.md`
  - `config.yaml.example`
  - `.env.example`

## Pull request checklist

- Explain the motivation and the change in the PR description.
- Update documentation when behavior or configuration changes.
- Ensure tests pass and add coverage for new logic.

## Reporting issues

Please include:

- Steps to reproduce
- Expected vs. actual behavior
- Environment details (OS, Python version, Docker version)
- Relevant logs or stack traces
