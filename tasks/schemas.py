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


class JiraMetadataSchema(BaseModel):
    url: str = Field(description="Browse URL of the Jira issue")
    title: str = Field(description="Issue summary")
    id: str = Field(coerce_numbers_to_str=True, description="Numeric Jira issue ID")
    assignee: str = Field(description="Display name of the issue assignee, empty when unassigned")
    reporter: str = Field(description="Display name of the issue reporter")
    status: str = Field(description="Workflow status name")
    labels: list[str] = Field(description="Labels applied to the issue")
    project: str = Field(description="Project name the issue belongs to")
    priority: str = Field(description="Priority name")


class MediaWikiMetadataSchema(BaseModel):
    title: str = Field(description="Page title")
    page_id: int = Field(description="Numeric MediaWiki page ID")
    namespace: int = Field(description="Namespace ID of the page")
    url: str | None = Field(default=None, description="Full URL of the wiki page; omitted when unavailable")


class PipedriveMetadataSchema(BaseModel):
    entity_type: str = Field(description="Pipedrive entity type, e.g. deals or persons")
    pipedrive_id: str = Field(description="Pipedrive record ID as a string")
    title: str = Field(description="Human-readable record title")
    url: str = Field(description="Pipedrive app URL of the record")
    add_time: str = Field(description="ISO timestamp when the record was created")
    update_time: str = Field(description="ISO timestamp when the record was last updated")


class S3MetadataSchema(BaseModel):
    bucket: str = Field(description="Name of the S3 bucket the object was ingested from")
    object_key: str = Field(description="Object key inside the bucket")
    file_extension: str = Field(description="File extension including leading dot, empty when the key has none")


class SerpAPIMetadataSchema(BaseModel):
    query: str = Field(description="Search query that produced this result set")


class WebMetadataSchema(BaseModel):
    url: str = Field(description="URL of the scraped page")
    title: str = Field(description="HTML title of the page, empty when unavailable")
