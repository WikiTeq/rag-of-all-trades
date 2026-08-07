from fastapi import APIRouter, Depends

from api.dependencies import require_api_key
from utils.connectors import build_connector_list

from .schema import ConnectorInfo, ConnectorListResponse

router = APIRouter(prefix="/connectors", tags=["Applications APIs"])


@router.get("", response_model=ConnectorListResponse)
async def list_connectors(_auth: None = Depends(require_api_key)):
    """List enabled connectors with their non-sensitive configuration."""
    connectors = build_connector_list()
    return ConnectorListResponse(connectors=[ConnectorInfo(**c) for c in connectors])
