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
