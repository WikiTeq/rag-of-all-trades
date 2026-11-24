from pydantic import BaseModel, Field


class BaseMetadataSchema(BaseModel):
    source: str = Field(description="The source type of the ingestion job")
    key: str = Field(description="Unique item key used for dedup and versioning")
    checksum: str = Field(description="Checksum or revision identifier of the raw content")
    version: int = Field(description="Monotonically increasing version number")
    format: str = Field(description="Content format, e.g. markdown")
    source_name: str = Field(description="Human-readable name of the source instance")
    file_name: str = Field(description="File name used by the vector store")
    last_modified: str = Field(description="ISO string of the item's last modified timestamp")
