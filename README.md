# Rag Of All Trades

[![Image artifact](https://github.com/WikiTeq/rag-of-all-trades/actions/workflows/docker-image.yml/badge.svg)](https://github.com/WikiTeq/rag-of-all-trades/actions/workflows/docker-image.yml)

This project is a complete **RAG (Retrieval-Augmented Generation)** microservice. The service is based on
FastAPI, PGVector, Redis, Celery, and LlamaIndex.

The service is built with extensibility in mind and provides a flexible configuration that allows you to
easily connect to an arbitrary number of data sources with pre-defined ingestion schedules.

## ✨ Features

* Ingestion from S3 buckets with Everything-to-Markdown conversion via [MarkItDown](https://github.com/microsoft/markitdown)
* Ingestion from local directories via [LlamaIndex SimpleDirectoryReader](https://developers.llamaindex.ai/python/framework/module_guides/loading/simpledirectoryreader/)
* Ingestion from MediaWiki with Wiki-to-Markdown conversion via [html2text](https://github.com/Alir3z4/html2text)
* SerpAPI ingestion from Google Search results with customizable queries
* Jira ingestion from Cloud and on-premise instances via JQL queries, with optional comment loading
* Dropbox ingestion — files and folders from Dropbox using the official Dropbox SDK with flexible path and extension filters
* Flexible configuration supporting an arbitrary number of connectors
* Built with extensibility in mind, allowing for custom connectors with ease

## 🌐 Connectors included

* S3
* Directory
* MediaWiki
* SerpAPI
* Jira
* Web
* Dropbox

## Embeddings support

* `Local` (running arbitrary embedding models from HuggingFace)
* `OpenRouter`
* `OpenAI`

## Inference support

* `OpenRouter`
* `OpenAI`

## Tech Stack

* FastAPI
* Vector search with `pgvector`
* Celery-based ingestion pipeline
* OpenAI/OpenRouter support for inference and embeddings
* Local LLM support for inference and embeddings
* LlamaIndex-powered RAG Query Engine
* Docker Compose for deployment

## ⚡️Quick Start

* Create a `.env` file based on the `.env.example` file.
  * The defaults are good enough, you just need to put your OpenRouter key into `OPENROUTER_API_KEY`
* If you use OpenAI or a different OpenAI-compatible endpoint, also update the `OPENROUTER_API_BASE` variable
* By default, a single S3 connector is configured; specify your S3 bucket credentials in the `S3_ACCOUNT1_` variables
* Create a `config.yaml` file based on the `config.yaml.example` file
  * The defaults are good enough with `openai/gpt-oss-120b:free` used for inference and `sentence-transformers/all-mpnet-base-v2` for embeddings.
  * If you would like to use different models, update the `embedding` and `inference` sections accordingly.
* Run `docker compose up -d --build` to start the service
* Access the API at `:8000`
* Access the API docs at `:8000/docs`

## MCP endpoint

The service includes an optional MCP (Model Context Protocol) server at `/mcp/` (trailing slash required).
It is disabled by default and can be enabled via environment variables.

### Enabling MCP

Set the following in your `.env` file:

```dotenv
MCP_ENABLE=1
MCP_API_KEY=your-strong-api-key
```

### Authentication

All MCP requests require a Bearer token in the `Authorization` header:

```http
Authorization: Bearer <MCP_API_KEY>
```

### Transport

The MCP server uses stateless HTTP mode (Streamable HTTP transport), so no
`mcp-session-id` header is required. The endpoint accepts JSON-RPC requests.

### Available tools

* `retrieve_chunks` - top-k retrieval from vector store with optional metadata filters
* `rephrase_chunks` - LLM-based answer generation over top-k retrieved chunks (requires `inference` to be configured)

### Testing with MCP Inspector

You can use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) to test the MCP endpoint:

```bash
./mcp_inspector.sh
```

This starts the inspector in Docker and prints a URL with pre-filled connection settings.

## Connectors

The service supports multiple data sources, including multiple data sources of the same type, each with its own
ingestion schedule. The connectors to enable are defined via `config.yaml`, and their secrets are defined
in the `.env` file.

### S3 Connector

The S3 connector ingests documents from S3 buckets and converts them to Markdown format.
The connector has the following configuration options:

```yaml
# config.yaml

sources:
  - type: "s3" # must be s3
    name: "account1" # arbitrary name for the connector, will be stored in metadata
    config:
      endpoint: "${S3_ACCOUNT1_ENDPOINT}" # s3 endpoint
      access_key: "${S3_ACCOUNT1_ACCESS_KEY}" # s3 access key
      secret_key: "${S3_ACCOUNT1_SECRET_KEY}" # s3 secret key
      region: "${S3_ACCOUNT1_REGION}" # s3 region
      use_ssl: "${S3_ACCOUNT1_USE_SSL}" # use ssl for s3 connection, can be True or False
      buckets: "${S3_ACCOUNT1_BUCKETS}" # single entry or comma-separated list i.e. bucket1,bucket2
      schedules: "${S3_ACCOUNT1_SCHEDULES}" # single entry or comma-separated list i.e. 3600,60

  - type: "s3"
    name: "account2"
    config:
      ...

  - type: "s3"
    name: "account3"
    config:
      ...
```

````dotenv
# .env

S3_ACCOUNT1_ENDPOINT=https://s3.amazonaws.com
S3_ACCOUNT1_ACCESS_KEY=xxx
S3_ACCOUNT1_SECRET_KEY=xxx
S3_ACCOUNT1_REGION=us-east-1
S3_ACCOUNT1_USE_SSL=True
S3_ACCOUNT1_BUCKETS=bucket1,bucket2
S3_ACCOUNT1_SCHEDULES=3600,60
````

### Directory Connector

The directory connector ingests files from a local filesystem directory using LlamaIndex `SimpleDirectoryReader`.
The connector has the following configuration options:

```yaml
# config.yaml

sources:
  - type: "directory"
    name: "local_docs"
    config:
      path: "/data/docs" # required path to directory
      recursive: true # optional, default true
      required_exts: "txt,md,pdf" # optional, comma-separated extensions
      exclude_hidden: true # optional, default true
      exclude_empty: false # optional, default false
      num_files_limit: 1000 # optional, positive integer
      schedules: "3600"
```

### MediaWiki Connector

The MediaWiki connector ingests documents from MediaWiki sites and converts them to Markdown format.
The connector has the following configuration options:

```yaml
# config.yaml

sources:
  - type: "mediawiki"
    name: "wiki1"
    config:
      api_url: "${MEDIAWIKI1_API_URL}"
      request_delay: 0.1
      schedules: "${MEDIAWIKI1_SCHEDULES}"

  - type: "mediawiki"
    name: "wiki2"
    config:
      ...

  - type: "mediawiki"
    name: "wiki3"
    config:
      ...
```

```dotenv
# .env

MEDIAWIKI1_API_URL=https://en.wikipedia.org/w/api.php
MEDIAWIKI1_SCHEDULES=3600
````

### SerpAPI Connector

The SerpAPI connector ingests documents from Google Search results and converts them to Markdown format.
The connector has the following configuration options:

```yaml
# config.yaml

sources:
  - type: "serpapi"
    name: "serp_ingestion1"
    config:
      api_key: "${SERPAPI1_KEY}"
      queries: "${SERPAPI1_QUERIES}"
      schedules: "${SERPAPI1_SCHEDULES}"

  - type: "serpapi"
    name: "serp_ingestion2"
    config:

  - type: "serpapi"
    name: "serp_ingestion3"
    config:
```

```dotenv
# .env

SERPAPI1_KEY=xxxx
SERPAPI1_QUERIES=aaa
SERPAPI1_SCHEDULES=3600
````

### Web Connector

The Web connector ingests content from web pages using the LlamaIndex `BeautifulSoupWebReader` (URLs mode) or `SitemapReader` (sitemap mode). The two modes are mutually exclusive.

**URLs mode** — scrape a fixed list of pages:

```yaml
- type: web
  name: web1
  config:
    urls:
      - https://example.com/page1
      - https://example.com/page2
    html_to_text: true   # optional, default true
    schedules: "${WEB1_SCHEDULES}"
```

**Sitemap mode** — discover and scrape URLs from a sitemap.xml:

```yaml
- type: web
  name: web2
  config:
    sitemap_url: https://example.com/sitemap.xml
    include_prefix: "/wiki/"   # optional: only ingest URLs containing this string
    html_to_text: true         # optional, default true
    schedules: "${WEB2_SCHEDULES}"
```

> **Note:** `exclude_prefix` and sitemap index (`<sitemapindex>`) are not supported in this iteration — the underlying `SitemapReader` only supports include-style filtering and flat sitemaps.

**.env variables:**

```dotenv
WEB1_SCHEDULES=60
WEB2_SCHEDULES=60
```

No other credentials are required for public web pages.

### Jira Connector

The Jira connector ingests issues from Jira Cloud or on-premise (Server/Data Center) instances using a
JQL query. Issue content (summary + description) is converted to Markdown. Metadata collected per issue
includes: id, title, url, status, assignee, reporter, labels, project, priority, issue type

Supports two authentication modes:
- **Basic auth** (`auth_type: basic`) — email + API token, for Jira Cloud
- **Personal Access Token** (`auth_type: token`) — PAT as Bearer header, for Jira Server / Data Center

```yaml
# config.yaml

sources:
  - type: "jira"
    name: "jira1"
    config:
      server_url: "${JIRA1_SERVER_URL}"
      auth_type: "basic"              # "basic" or "token"
      email: "${JIRA1_EMAIL}"         # required for auth_type=basic
      api_token: "${JIRA1_API_TOKEN}"
      jql: "${JIRA1_JQL}"
      max_results: 50                 # optional, default 50
      schedules: "${JIRA1_SCHEDULES}"
      # Optional: load top N comments per issue
      load_comments: false            # optional, default false
      max_comments: 10                # optional, default 10
```

```dotenv
# .env

# Jira Cloud (basic auth)
JIRA1_SERVER_URL=https://your-org.atlassian.net
JIRA1_EMAIL=your-email@example.com
JIRA1_API_TOKEN=your-api-token
JIRA1_JQL=project = MYPROJECT ORDER BY updated DESC
JIRA1_SCHEDULES=3600

# Jira Server / Data Center (Personal Access Token)
# JIRA1_SERVER_URL=https://jira.your-company.com
# JIRA1_API_TOKEN=your-personal-access-token
# (set auth_type: "token" in config.yaml; email is not needed)
```

## Reference of the `config.yaml`

The `config.yaml` file contains the main configuration of the service.

### Common connector parameters

The following parameters are supported by all connector types:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedules` | string | — | Cron expression or interval (in seconds) defining how often the connector runs. |
| `request_delay` | float | `0` | Delay in seconds between processing each item. Useful for rate-limiting requests to external APIs. |

> Environment variables (`${...}`) in the config file are evaluated at runtime.

```yaml
sources: # holds the list of sources to ingest from (Connectors)

  - type: # type of the connector (s3, directory, mediawiki, serpapi, jira, etc.)
    name: # arbitrary name for the connector, will be stored in metadata
    config:
      # connector specific configuration
      schedules: "${S3_ACCOUNT1_SCHEDULES}"
      request_delay: 0  # optional, delay in seconds between items (default: 0)

# configures models and dimensions for embeddings
embedding:
  provider: openrouter # `openrouter`/`openai` or `local` for local HuggingFace embeddings
  model_config: text-embedding-3-small # model to use
  embedding_dim: 1536 # dimensions (check with the model docs)

# configures the LLM provider and model
inference:
  provider: openrouter # `openrouter`/`openai`
  model_config: gpt-4o # model to use

# vector store configuration
vector_store:
  table_name: embeddings
  hybrid_search: true # whether to use hybrid search or not
  chunk_size: 512 # chunk size for vector indexing
  chunk_overlap: 50 # overlap between chunks
  # hnsw indexes settings
  hnsw:
    hnsw_m: 16 # number of neighbors
    hnsw_ef_construction: 64 # ef construction parameter for HNSW
    hnsw_ef_search: 40 # ef search parameter for HNSW
    hnsw_dist_method: vector_cosine_ops # distance metric for HNSW
```

### Dropbox Connector

The Dropbox connector ingests files from Dropbox using the [official Dropbox Python SDK](https://pypi.org/project/dropbox/).
Supports ingesting from specific paths or the entire account root, with optional extension and directory filters.
Content is extracted with [MarkItDown](https://github.com/microsoft/markitdown) and falls back to raw text.

Authentication requires a [Dropbox access token](https://www.dropbox.com/developers/apps) with `files.content.read` scope.

```yaml
# config.yaml

sources:
  - type: "dropbox"
    name: "dropbox1"
    config:
      access_token: "${DROPBOX1_ACCESS_TOKEN}"
      # Paths to ingest (optional). If omitted, ingests everything from root recursively.
      paths:
        - "/Documents/Engineering"
        - "/Shared/Wiki"
      # Extension filters (mutually exclusive, optional):
      #include_extensions: "md,docx,pdf"   # only these extensions
      #exclude_extensions: "png,jpg,gif"   # all except these
      # Directory name filters (mutually exclusive, optional):
      #include_directories: "source,docs"  # only these folder names
      #exclude_directories: "archive,tmp"  # all except these folder names
      schedules: "${DROPBOX1_SCHEDULES}"
```

```dotenv
# .env

DROPBOX1_ACCESS_TOKEN=sl.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
DROPBOX1_SCHEDULES=3600
```

## Embeddings and Inference configuration examples

### Embeddings-only HuggingFace local model

You can configure the service to use **local embeddings** only, in this mode
you can use any embedding model supported by HuggingFace. Inference is disabled in
this mode, so you won't be able to use the **rephrase** endpoint.

```yaml
# config.yaml

embedding:
  provider: local
  # you can use any embedding model supported by HuggingFace
  model_config: sentence-transformers/all-MiniLM-L6-v2
  embedding_dim: 384

inference:
  provider: None
  model_config: None
```

### Embeddings-only OpenRouter/OpenAI model

You can configure the service to use **remote embeddings**, in this mode
you can use any embedding model supported by OpenRouter/OpenAI. Inference is disabled in
this mode, so you won't be able to use the **rephrase** endpoint.

```yaml
# config.yaml

embedding:
  provider: openrouter
  model_config: text-embedding-3-small
  embedding_dim: 1536

inference:
  provider: None
  model_config: None
```

You must set `OPENROUTER_API_KEY` and `OPENROUTER_API_BASE` in the `.env` file.

### Embeddings and inference OpenRouter/OpenAI model

You can configure the service to use **remote embeddings** and **remote inference**, in this mode
you can use any embedding and inference models supported by OpenRouter/OpenAI.

```yaml
# config.yaml

embedding:
  provider: openrouter
  model_config: text-embedding-3-small
  embedding_dim: 1536

inference:
  provider: openrouter
  model_config: gpt-4o
```

You must set `OPENROUTER_API_KEY` and `OPENROUTER_API_BASE` in the `.env` file.

## API

The following API endpoints are available:

### /api/v1/query/

This endpoint is used to perform a query against the vector store:

```bash
curl -X 'POST' \
  'http://localhost:8000/api/v1/query/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "query": "AWS Services",
  "top_k": 5
}'
```

**Response example**:

```json
{
  "references": [
    {
      "source_name": null,
      "source_type": null,
      "url": null,
      "score": 0.6172290216224814,
      "title": null,
      "text": "You can also\n\nrequire WAF Captcha challenges for suspicious...",
      "extras": {
        "source": "s3",
        "key": "aws-overview.pdf",
        "checksum": "5b4da9267b0b861792d1163fcc9f0550",
        "version": 1,
        "format": "markdown"
      }
    },
    {...},
    {...}
  ],
  "raw": [
    "Score: 0.6172 | Text: You can also\n\nrequire WAF Captcha challenges for suspicious...",
    "Score: 0.5172 | Text: You can also\n\nrequire WAF Captcha challenges for suspicious...",
    "Score: 0.3172 | Text: You can also\n\nrequire WAF Captcha challenges for suspicious..."
  ]
}
```

### /api/v1/rephrase/

This endpoint rephrases the query and provides the best answer.

> This endpoint requires `inference` to be configured in the `config.yaml`.

```bash
curl -X 'POST' \
  'http://localhost:8000/api/v1/rephrase/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "query": "WAF Captcha challenges for suspicious requests"
}'
```

**Response format**:

```json
{
  "answer": "You can configure AWS WAF to require Captcha challenges for suspicious requests based on:\n- Request rate and attributes",
  "references": [
    {
      "source_name": null,
      "source_type": null,
      "url": null,
      "score": 0.5415070280718167,
      "title": null,
      "extras": {
        "source": "s3",
        "key": "aws-overview.pdf",
        "checksum": "5b4da9267b0b861792d1163fcc9f0550",
        "version": 1,
        "format": "markdown"
      }
    }
  ]
}
```

### /health

This endpoint checks the health of the service.

```bash
curl -X 'GET' \
  'http://localhost:8000/health' \
  -H 'accept: application/json'
```

**Response example**:

```json
{
  "status": "ok",
  "vector_store_loaded": true,
  "celery_healthy": true
}
```

## Integration examples

### OpenWebUI

TODO


### LibreChat

TODO

### LobeHub

TODO

### Anything-LLM

TODO

## 🔧 Development

### Pre-commit hooks

This project uses [prek](https://github.com/j178/prek) (a fast, drop-in alternative to `pre-commit`) to enforce
formatting and linting on every commit.

**Install prek** (once, globally):

```bash
# Using pip
pip install prek

# Or using the standalone installer (Linux/macOS)
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/j178/prek/releases/latest/download/prek-installer.sh | sh
```

**Install the hooks** (once, per clone):

```bash
prek install
```

From that point on, every `git commit` will automatically run:

| Hook | What it does |
|---|---|
| `trailing-whitespace` | Removes trailing whitespace |
| `end-of-file-fixer` | Ensures files end with a newline |
| `check-yaml` | Validates YAML syntax |
| `check-merge-conflict` | Detects unresolved merge conflict markers |
| `ruff` (lint) | Lints Python with auto-fix (pycodestyle, pyflakes, isort, pyupgrade) |
| `ruff-format` | Formats Python code (replaces black) |

**Run hooks manually** (without committing):

```bash
prek run --all-files
```

Ruff configuration is in [pyproject.toml](pyproject.toml) under `[tool.ruff]`.

## ✨ Contributions

Contributions, suggestions, bug reports, and fixes are welcome!

## Star History

<a href="https://www.star-history.com/?repos=wikiteq%2Frag-of-all-trades&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=wikiteq/rag-of-all-trades&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=wikiteq/rag-of-all-trades&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=wikiteq/rag-of-all-trades&type=date&legend=top-left" />
 </picture>
</a>
