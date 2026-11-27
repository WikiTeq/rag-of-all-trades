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


class PipedriveMetadataSchema(BaseModel):
    entity_type: str = Field(description="Pipedrive entity type, e.g. deals or persons")
    pipedrive_id: str = Field(description="Pipedrive record ID as a string")
    title: str = Field(description="Human-readable record title")
    url: str = Field(description="Pipedrive app URL of the record")
    add_time: str = Field(description="ISO timestamp when the record was created")
    update_time: str = Field(description="ISO timestamp when the record was last updated")
