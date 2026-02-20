#!/bin/bash

SERVER_URL="http://host.docker.internal:8000/mcp"
TRANSPORT="streamable-http"

docker run --rm --name "mcp_inspector" \
  -p 127.0.0.1:6274:6274 \
  -p 127.0.0.1:6277:6277 \
  -e HOST=0.0.0.0 \
  -e MCP_AUTO_OPEN_ENABLED=false \
  -e DANGEROUSLY_OMIT_AUTH=true \
  --add-host="host.docker.internal:host-gateway" \
  ghcr.io/modelcontextprotocol/inspector:latest

# visit http://localhost:6274/
