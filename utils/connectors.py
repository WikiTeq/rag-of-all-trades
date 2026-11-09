from utils.config import settings

# Config keys common to every connector type (handled by the IngestionJob base
# class), safe to expose regardless of source type.
COMMON_SAFE_FIELDS: list[str] = ["request_delay"]

# Config keys safe to expose per connector type. Credentials/tokens are
# intentionally omitted (e.g. access_key, secret_key, api_token, password).
CONNECTOR_SAFE_FIELDS: dict[str, list[str]] = {
    "s3": ["buckets", "region", "use_ssl", "endpoint"],
    "mediawiki": ["host", "path", "scheme", "api_url", "namespaces", "filter_redirects", "page_limit"],
    "jira": ["server_url", "jql", "auth_type", "max_results", "load_comments", "max_comments"],
    "serpapi": ["queries"],
    "directory": [
        "path",
        "recursive",
        "required_exts",
        "exclude_hidden",
        "exclude_empty",
        "num_files_limit",
        "encoding",
    ],
    "web": ["urls", "sitemap_url", "include_prefix", "html_to_text"],
    "slack": ["channel_ids", "channel_patterns", "channel_types", "earliest_date", "latest_date"],
    "pipedrive": [
        "load_types",
        "max_items",
        "max_retries",
        "filter_deals_updated_since",
        "filter_activities_updated_since",
        "filter_deals_stages_ids",
        "filter_mail_folders",
        "filter_activities_filter_id",
        "filter_deals_filter_id",
        "filter_organizations_filter_id",
        "filter_persons_filter_id",
        "filter_products_filter_id",
        "filter_projects_filter_id",
        "filter_leads_filter_id",
    ],
}


def build_connector_list() -> list[dict]:
    """Build a list of enabled connectors with non-sensitive config fields only.

    Reads the raw `sources` list from config.yaml rather than settings.SOURCES,
    which is the ingestion-job view and can differ from what's defined in
    config.yaml. This endpoint must reflect connectors as defined in
    config.yaml, 1:1.
    """
    connectors = []
    for source in settings.yaml.get("sources", []):
        if source.get("enabled") is False:
            continue

        src_type = source.get("type")
        safe_fields = COMMON_SAFE_FIELDS + CONNECTOR_SAFE_FIELDS.get(src_type, [])
        config = source.get("config", {})

        connectors.append(
            {
                "type": src_type,
                "name": source.get("name"),
                "config": {field: config[field] for field in safe_fields if field in config},
            }
        )
    return connectors
