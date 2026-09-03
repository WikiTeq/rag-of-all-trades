def ingestion_task_name(source_config: dict) -> str:
    """Return the Celery task name for a source config entry.

    Mirrors the naming contract in celery_app.create_task_for_source().
    S3 sources with bucket_override get a suffixed name; all others do not.
    """
    bucket_override = source_config["config"].get("bucket_override")
    if bucket_override:
        return f"{source_config['type']}_ingest_{source_config['name']}_{bucket_override}"
    return f"{source_config['type']}_ingest_{source_config['name']}"
