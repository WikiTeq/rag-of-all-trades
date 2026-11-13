def ingestion_task_name(source_config: dict) -> str:
    """Return the Celery task name for a source config entry.

    Mirrors the naming contract in celery_app.create_task_for_source().
    """
    return f"{source_config['type']}_ingest_{source_config['name']}"
