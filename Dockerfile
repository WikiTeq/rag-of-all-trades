# =========================
#  BUILDER
# =========================
FROM python:3.11-slim AS builder

LABEL maintainers="contact@wikiteq.com,alexey@wikiteq.com"
LABEL org.opencontainers.image.source=https://github.com/WikiTeq/rag-of-all-trades

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set UV environment variables for optimal Docker builds
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Install PyTorch first (CPU version)
RUN uv pip install --system --no-cache torch --index-url https://download.pytorch.org/whl/cpu

# Install HuggingFace stack
RUN uv pip install --system --no-cache \
    transformers \
    tokenizers \
    sentence-transformers \
    huggingface_hub

# LlamaIndex embedding
RUN uv pip install --system --no-cache llama-index-embeddings-huggingface==0.6.1

# Copy project files
COPY pyproject.toml .

# Install project dependencies
RUN uv pip install --system --no-cache .


# =========================
#  FINAL IMAGE
# =========================
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy all Python libraries from builder to final image
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy project
COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_ENV=production

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
