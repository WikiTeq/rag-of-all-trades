from pydantic import BaseModel


class ConnectorInfo(BaseModel):
    type: str | None = None
    name: str | None = None
    config: dict


class ConnectorListResponse(BaseModel):
    connectors: list[ConnectorInfo]
