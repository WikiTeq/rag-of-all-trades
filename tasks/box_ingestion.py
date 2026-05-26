import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from box_sdk_gen import BoxCCGAuth, BoxClient, BoxJWTAuth, CCGConfig, JWTConfig
from box_sdk_gen.schemas.file import File
from llama_index.readers.box import BoxReader
from llama_index.readers.box.BoxAPI.box_api import get_box_files_details, get_file_content_by_id
from llama_index.readers.box.BoxAPI.box_llama_adaptors import box_file_to_llama_document_metadata

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_bool, parse_list, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)


class BoxIngestionJob(IngestionJob):
    """Ingestion connector for Box cloud storage.

    Uses the LlamaIndex Box reader for file discovery and content extraction.
    Supports CCG (Client Credential Grant) and JWT authentication, folder-based
    ingestion, file-ID-based ingestion, full-text search, and metadata search.

    Configuration (config.yaml):
        - config.auth_type: Authentication type: "ccg" (default) or "jwt"

        CCG auth (auth_type: ccg):
            - config.box_client_id: Box app client ID (required)
            - config.box_client_secret: Box app client secret (required)
            - config.box_enterprise_id: Box enterprise ID (required)
            - config.box_user_id: Box user ID for user-level access (optional)

        JWT auth (auth_type: jwt):
            - config.box_client_id: Box app client ID (required)
            - config.box_client_secret: Box app client secret (required)
            - config.box_jwt_key_id: JWT public key ID (required)
            - config.box_private_key: RSA private key PEM string (required)
            - config.box_private_key_passphrase: Private key passphrase (required)
            - config.box_enterprise_id: Box enterprise ID (optional)
            - config.box_user_id: Box user ID for user-level access (optional)

        Ingestion modes (one or more may be combined):
            - config.folder_id: Box folder ID to ingest (optional)
            - config.file_ids: Comma-separated Box file IDs to ingest (optional)
            - config.is_recursive: Traverse subfolders recursively (optional, default false)

        Search by content (optional):
            - config.search_query: Full-text search query string
            - config.search_file_extensions: Comma-separated file extensions to filter (e.g. "pdf,docx")
            - config.search_ancestor_folder_ids: Comma-separated folder IDs to scope search

        Search by metadata (optional):
            - config.metadata_template: Metadata template key (required for metadata search)
            - config.metadata_ancestor_folder_id: Folder ID to scope metadata search (required)
            - config.metadata_query: Metadata query string (optional)
            - config.metadata_query_params: Metadata query params as "key=value,key2=value2" (optional)
    """

    @property
    def source_type(self) -> str:
        return "box"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.box_client_id = (cfg.get("box_client_id") or "").strip()
        if not self.box_client_id:
            raise ValueError("box_client_id is required in Box connector config")

        self.box_client_secret = (cfg.get("box_client_secret") or "").strip()
        if not self.box_client_secret:
            raise ValueError("box_client_secret is required in Box connector config")

        self.box_enterprise_id: str | None = (cfg.get("box_enterprise_id") or "").strip() or None
        self.box_user_id: str | None = (cfg.get("box_user_id") or "").strip() or None

        auth_type = (cfg.get("auth_type") or "ccg").strip().lower()
        if auth_type == "jwt":
            self.box_client = self._build_jwt_client(cfg)
        elif auth_type == "ccg":
            self.box_client = self._build_ccg_client()
        else:
            raise ValueError(f"Unsupported auth_type {auth_type!r}; expected 'ccg' or 'jwt'")

        del self.box_client_id, self.box_client_secret

        self.folder_id: str | None = (cfg.get("folder_id") or "").strip() or None
        self.file_ids: list[str] | None = parse_list(cfg.get("file_ids")) or None
        self.is_recursive: bool = parse_bool(cfg.get("is_recursive"), default=False)

        self.search_query: str | None = (cfg.get("search_query") or "").strip() or None
        self.search_file_extensions: list[str] | None = parse_list(cfg.get("search_file_extensions")) or None
        self.search_ancestor_folder_ids: list[str] | None = parse_list(cfg.get("search_ancestor_folder_ids")) or None

        self.metadata_template: str | None = (cfg.get("metadata_template") or "").strip() or None
        self.metadata_ancestor_folder_id: str | None = (cfg.get("metadata_ancestor_folder_id") or "").strip() or None
        self.metadata_query: str | None = (cfg.get("metadata_query") or "").strip() or None
        raw_meta_params = (cfg.get("metadata_query_params") or "").strip()
        self.metadata_query_params: dict[str, str] | None = self._parse_kv_pairs(raw_meta_params) or None

        has_folder = self.folder_id is not None
        has_files = self.file_ids is not None
        has_search = self.search_query is not None
        has_meta_search = self.metadata_template is not None and self.metadata_ancestor_folder_id is not None
        if not any([has_folder, has_files, has_search, has_meta_search]):
            raise ValueError(
                "Box connector config requires at least one of: folder_id, file_ids, "
                "search_query, or metadata_template + metadata_ancestor_folder_id"
            )

    def _build_ccg_client(self) -> BoxClient:
        if not self.box_enterprise_id and not self.box_user_id:
            raise ValueError("box_enterprise_id or box_user_id is required for CCG auth")
        ccg_config = CCGConfig(
            client_id=self.box_client_id,
            client_secret=self.box_client_secret,
            enterprise_id=self.box_enterprise_id,
            user_id=self.box_user_id,
        )
        return BoxClient(auth=BoxCCGAuth(config=ccg_config))

    def _build_jwt_client(self, cfg: dict) -> BoxClient:
        jwt_key_id = (cfg.get("box_jwt_key_id") or "").strip()
        private_key = (cfg.get("box_private_key") or "").strip().replace("\\n", "\n")
        private_key_passphrase = (cfg.get("box_private_key_passphrase") or "").strip()
        if not jwt_key_id:
            raise ValueError("box_jwt_key_id is required for JWT auth")
        if not private_key:
            raise ValueError("box_private_key is required for JWT auth")
        if not private_key_passphrase:
            raise ValueError("box_private_key_passphrase is required for JWT auth")
        jwt_config = JWTConfig(
            client_id=self.box_client_id,
            client_secret=self.box_client_secret,
            jwt_key_id=jwt_key_id,
            private_key=private_key,
            private_key_passphrase=private_key_passphrase,
            enterprise_id=self.box_enterprise_id,
            user_id=self.box_user_id,
        )
        return BoxClient(auth=BoxJWTAuth(config=jwt_config))

    @staticmethod
    def _parse_kv_pairs(raw: str) -> dict[str, str]:
        result = {}
        for pair in raw.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, _, v = pair.partition("=")
                result[k.strip()] = v.strip()
        return result

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover Box file IDs via configured modes and yield one IngestionItem per file."""
        reader = BoxReader(box_client=self.box_client)
        file_ids: list[str] = []

        if self.folder_id is not None or self.file_ids is not None:
            logger.info(f"[{self.source_name}] Listing files by folder/file IDs from Box")
            try:
                file_ids += reader.list_resources(
                    folder_id=self.folder_id,
                    file_ids=self.file_ids,
                    is_recursive=self.is_recursive,
                )
            except Exception:
                logger.exception(f"[{self.source_name}] Failed to list files by folder/file IDs")
                raise

        if self.search_query is not None:
            logger.info(f"[{self.source_name}] Searching Box by content query: {self.search_query!r}")
            try:
                file_ids += reader.search_resources(
                    query=self.search_query,
                    file_extensions=self.search_file_extensions,
                    ancestor_folder_ids=self.search_ancestor_folder_ids,
                )
            except Exception:
                logger.exception(f"[{self.source_name}] Failed to search files by content query")
                raise

        if self.metadata_template is not None and self.metadata_ancestor_folder_id is not None:
            logger.info(f"[{self.source_name}] Searching Box by metadata template: {self.metadata_template!r}")
            try:
                file_ids += reader.search_resources_by_metadata(
                    from_=self.metadata_template,
                    ancestor_folder_id=self.metadata_ancestor_folder_id,
                    query=self.metadata_query,
                    query_params=self.metadata_query_params,
                )
            except Exception:
                logger.exception(f"[{self.source_name}] Failed to search files by metadata")
                raise

        file_ids = list(dict.fromkeys(file_ids))  # deduplicate, preserve order

        if not file_ids:
            logger.info(f"[{self.source_name}] Found 0 file(s)")
            return

        logger.info(f"[{self.source_name}] Found {len(file_ids)} file(s), fetching details")

        box_files: list[File] = get_box_files_details(box_client=self.box_client, file_ids=file_ids)

        for box_file in box_files:
            last_modified = parse_timestamp(box_file.modified_at.isoformat() if box_file.modified_at else None)
            if last_modified is None:
                logger.warning(
                    f"[{self.source_name}] Could not parse modified_at for file_id={box_file.id!r}, using now"
                )
                last_modified = datetime.now(UTC)

            yield IngestionItem(
                id=f"box:{box_file.id}",
                source_ref=box_file,
                last_modified=last_modified,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Download and return the text content of the Box file."""
        box_file: File = item.source_ref
        meta = box_file_to_llama_document_metadata(box_file)

        item._metadata_cache["box_file_id"] = meta.get("box_file_id") or ""
        item._metadata_cache["box_file_name"] = meta.get("name") or ""
        item._metadata_cache["path_collection"] = meta.get("path_collection") or ""

        content_bytes = get_file_content_by_id(box_client=self.box_client, box_file_id=box_file.id)
        return content_bytes.decode("utf-8", errors="replace")

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name for the Box file."""
        box_file: File = item.source_ref
        file_id = box_file.id or item.id
        file_name = box_file.name or ""
        return slugify(f"box_{file_id}_{file_name}", max_len=255)

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        """Return Box-specific metadata fields."""
        return {
            "box_file_id": item._metadata_cache.get("box_file_id", ""),
            "box_file_name": item._metadata_cache.get("box_file_name", ""),
            "path_collection": item._metadata_cache.get("path_collection", ""),
        }
