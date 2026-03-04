# Rag Of All Trades

[![Image artifact](https://github.com/WikiTeq/rag-of-all-trades/actions/workflows/docker-image.yml/badge.svg)](https://github.com/WikiTeq/rag-of-all-trades/actions/workflows/docker-image.yml)

This project is a complete **RAG (Retrieval-Augmented Generation)** microservice. The service is based on
FastAPI, PGVector, Redis, Celery, and LlamaIndex.

The service is built with extensibility in mind and provides a flexible configuration that allows you to
easily connect to an arbitrary number of data sources with pre-defined ingestion schedules.

## ✨ Features

* Ingestion from S3 buckets with Everything-to-Markdown conversion via [MarkItDown](https://github.com/microsoft/markitdown)
* Ingestion from MediaWiki with Wiki-to-Markdown conversion via [html2text](https://github.com/Alir3z4/html2text)
* SerpAPI ingestion from Google Search results with customizable queries
* Jira ingestion from Cloud and on-premise instances via JQL queries, with optional comment loading
* Flexible configuration supporting an arbitrary number of connectors
* Built with extensibility in mind, allowing for custom connectors with ease

## 🌐 Connectors included

* S3
* MediaWiki
* SerpAPI
* Jira

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

> Environment variables (`${...}`) in the config file are evaluated at runtime.

```yaml
sources: # holds the list of sources to ingest from (Connectors)

  - type: # type of the connector (s3, mediawiki, serpapi, jira)
    name: # arbitrary name for the connector, will be stored in metadata
    config:
      # connector specific configuration
      schedules: "${S3_ACCOUNT1_SCHEDULES}"

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

## ✨ Contributions

Contributions, suggestions, bug reports, and fixes are welcome!
