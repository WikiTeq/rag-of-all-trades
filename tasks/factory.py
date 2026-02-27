from tasks.s3_ingestion import S3IngestionJob
from tasks.mediawiki_ingestion import MediaWikiIngestionJob
from tasks.jira_ingestion import JiraIngestionJob
from tasks.base import IngestionJob

class IngestionJobFactory:
    _registry = {}

    @classmethod
    def register(cls, job_type: str, job_class):
        if not issubclass(job_class, IngestionJob):
            raise ValueError(f"{job_class} must inherit from IngestionJob")
        cls._registry[job_type] = job_class

    @classmethod
    def create(cls, job_type: str, config: dict) -> IngestionJob:
        job_class = cls._registry.get(job_type)
        if not job_class:
            raise ValueError(f"No ingestion job registered for type: {job_type}")
        return job_class(config)

IngestionJobFactory.register("s3", S3IngestionJob)
IngestionJobFactory.register("mediawiki", MediaWikiIngestionJob)
IngestionJobFactory.register("jira", JiraIngestionJob)
