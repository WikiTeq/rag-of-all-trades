import logging
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.graph_client import GraphClient, GraphItemNotFoundError
from utils.parse import parse_bool, parse_list, parse_timestamp

logger = logging.getLogger(__name__)


class OneDriveIngestionJob(IngestionJob):
    """Ingestion connector for Microsoft OneDrive for Business.

    Uses a hand-rolled Microsoft Graph API client (utils.graph_client.GraphClient) for
    file discovery and content fetching. Requires App authentication (client credentials):
    client_id, client_secret, tenant_id, and userprincipalname.

    Configuration (config.yaml):
        - config.client_id: Azure app registration client ID (required)
        - config.client_secret: Azure app registration client secret (required)
        - config.tenant_id: Azure tenant ID (required)
        - config.userprincipalname: User principal name / email of the OneDrive owner (required)
        - config.folder_id: OneDrive folder ID to ingest (optional)
        - config.folder_path: Relative path of the OneDrive folder to ingest (optional)
        - config.file_ids: Comma-separated OneDrive file IDs to ingest (optional)
        - config.file_paths: Comma-separated OneDrive file paths to ingest (optional)
        - config.mime_types: Comma-separated MIME types to filter (optional, default: all)
        - config.recursive: Traverse subfolders recursively (optional, default: true)
        - config.max_file_size_mb: Skip files larger than this (optional, default: 50)

    Item identity: items are keyed as "onedrive:{drive_id}:{item_id}" — Graph item IDs
    are stable across rename/move, unlike file paths. file_path/file_name/web_url are
    stored as metadata only, refreshed on every re-ingest.
    """

    DEFAULT_MAX_FILE_SIZE_MB = 50

    @property
    def source_type(self) -> str:
        return "onedrive"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        # Required App auth credentials for OneDrive for Business
        self.client_id = (cfg.get("client_id") or "").strip()
        if not self.client_id:
            raise ValueError("client_id is required in OneDrive connector config")

        self.client_secret = (cfg.get("client_secret") or "").strip()
        if not self.client_secret:
            raise ValueError("client_secret is required in OneDrive connector config")

        self.tenant_id = (cfg.get("tenant_id") or "").strip()
        if not self.tenant_id:
            raise ValueError("tenant_id is required in OneDrive connector config")

        # The UPN of the user whose OneDrive will be accessed via app credentials
        self.userprincipalname = (cfg.get("userprincipalname") or "").strip()
        if not self.userprincipalname:
            raise ValueError("userprincipalname is required in OneDrive connector config")

        # Optional content selectors — if none are set, all files from root are loaded
        self.folder_id: str | None = (cfg.get("folder_id") or "").strip() or None
        self.folder_path: str | None = (cfg.get("folder_path") or "").strip() or None
        self.file_ids: list[str] = parse_list(cfg.get("file_ids"))
        self.file_paths: list[str] = parse_list(cfg.get("file_paths"))
        self.mime_types: list[str] = parse_list(cfg.get("mime_types"), lower=True)

        self.recursive: bool = parse_bool(cfg.get("recursive"), default=True)

        max_file_size_mb = cfg.get("max_file_size_mb", self.DEFAULT_MAX_FILE_SIZE_MB)
        try:
            max_file_size_mb = float(max_file_size_mb)
        except (TypeError, ValueError) as exc:
            raise ValueError("max_file_size_mb must be a number") from exc
        if max_file_size_mb <= 0:
            raise ValueError("max_file_size_mb must be > 0")

        self._graph = GraphClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            max_file_size_bytes=int(max_file_size_mb * 1024 * 1024),
        )

        # Resolved lazily on first use in list_items() — requires an authenticated call.
        self._drive_id: str | None = None

        logger.info(
            f"Initialized OneDrive connector (tenant=***{self.tenant_id[-4:]}, "
            f"user=***{self.userprincipalname[-4:]}, folder_path={self.folder_path!r}, "
            f"folder_id={self.folder_id!r}, recursive={self.recursive}, "
            f"max_file_size_mb={max_file_size_mb})"
        )

    def _resolve_drive_id(self) -> str:
        """Resolve and cache the drive ID for the configured user's OneDrive."""
        if self._drive_id is None:
            self._drive_id = self._graph.get_user_drive_id(self.userprincipalname)
        return self._drive_id

    def _mime_type_allowed(self, item: dict) -> bool:
        if not self.mime_types:
            return True
        item_mime = (item.get("file", {}).get("mimeType") or "").lower()
        return item_mime in self.mime_types

    def _is_downloadable_file(self, item: dict) -> bool:
        """True for real files with content Graph can serve — false for folders,
        OneNote notebook packages, and other non-downloadable item types."""
        return "file" in item and "@microsoft.graph.downloadUrl" in item

    def _walk_folder(
        self, drive_id: str, root_item_id: str, visited: set[tuple[str, str]]
    ) -> Iterator[tuple[str, dict]]:
        """Yield (drive_id, item) for every downloadable file under root_item_id.

        The drive_id is yielded alongside each item — not assumed constant — because a
        remoteItem (shared-from-another-drive) can surface files that physically live in
        a different drive than the one being walked; every downstream call (download,
        identity key) must use the item's own drive, not the walk's starting drive.

        Uses an explicit stack (not Python call recursion) so deep or wide folder trees
        don't risk hitting the recursion limit, and a visited set guards against cycles
        that a remoteItem loop could otherwise introduce.
        """
        stack: list[tuple[str, str]] = [(drive_id, root_item_id)]

        while stack:
            cur_drive_id, cur_item_id = stack.pop()
            key = (cur_drive_id, cur_item_id)
            if key in visited:
                continue
            visited.add(key)

            try:
                children = list(self._graph.list_children(cur_drive_id, cur_item_id))
            except GraphItemNotFoundError:
                # Folder was deleted/moved between discovery and this walk reaching it —
                # skip this one subtree, don't abort the whole run. Any other failure
                # (auth, 403, 429, 5xx, network) is not caught here and propagates, since
                # those indicate a systemic problem, not a stale reference to one folder.
                logger.warning(
                    f"[{self.source_name}] Folder drive_id={cur_drive_id!r} item_id={cur_item_id!r} "
                    "no longer exists — skipping this subtree"
                )
                continue

            for child in children:
                if "remoteItem" in child:
                    remote_stub = child["remoteItem"]
                    remote_parent = remote_stub.get("parentReference", {})
                    remote_drive_id = remote_parent.get("driveId")
                    remote_item_id = remote_stub.get("id")
                    if not remote_drive_id or not remote_item_id:
                        logger.debug(f"[{self.source_name}] Skipping remoteItem with no resolvable target")
                        continue

                    if self.recursive and "folder" in remote_stub:
                        stack.append((remote_drive_id, remote_item_id))
                        continue

                    # The remoteItem facet on a /children listing is a stub — it does not
                    # reliably carry @microsoft.graph.downloadUrl, which is only
                    # documented on the real DriveItem. Resolve it with the same GET
                    # get_download_url uses before deciding whether it's downloadable.
                    try:
                        remote = self._graph.get_item(remote_drive_id, remote_item_id)
                    except GraphItemNotFoundError:
                        logger.warning(
                            f"[{self.source_name}] remoteItem drive_id={remote_drive_id!r} "
                            f"item_id={remote_item_id!r} no longer resolves — skipping"
                        )
                        continue
                    if self._is_downloadable_file(remote):
                        yield (remote_drive_id, remote)
                    continue

                if "folder" in child:
                    if self.recursive:
                        stack.append((cur_drive_id, child["id"]))
                    continue

                if self._is_downloadable_file(child):
                    yield (cur_drive_id, child)
                else:
                    logger.debug(f"[{self.source_name}] Skipping non-downloadable item: {child.get('name')!r}")

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover OneDrive files via the Graph API and yield one IngestionItem per file.

        Fatal errors (auth failure, the configured folder/file not resolving) propagate —
        tasks.base.IngestionJob.run() lets them raise so the Celery task is marked FAILED
        instead of silently succeeding. A single folder failing mid-walk does not abort
        the whole run; see _walk_folder's per-folder handling.
        """
        logger.info(f"[{self.source_name}] Discovering files from OneDrive")

        drive_id = self._resolve_drive_id()
        seen_ids: set[tuple[str, str]] = set()
        count = 0

        for item_drive_id, item in self._list_configured_scope(drive_id):
            item_key = (item_drive_id, item["id"])
            if item_key in seen_ids:
                # Same file reachable via more than one of file_ids/file_paths/folder walk
                continue
            seen_ids.add(item_key)

            if not self._mime_type_allowed(item):
                continue

            last_modified = parse_timestamp(item.get("lastModifiedDateTime"))
            if last_modified is not None and last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=UTC)
            if last_modified is None:
                logger.warning(
                    f"[{self.source_name}] Could not parse lastModifiedDateTime for item_id={item['id']!r}, using now"
                )
                last_modified = datetime.now(UTC)

            source_ref = {
                "drive_id": item_drive_id,
                "item_id": item["id"],
                "name": item.get("name", ""),
                "parent_path": item.get("parentReference", {}).get("path", ""),
                "web_url": item.get("webUrl", ""),
                "etag": item.get("eTag", ""),
                "size": item.get("size", 0),
            }
            count += 1
            yield IngestionItem(
                id=f"onedrive:{item_drive_id}:{item['id']}",
                source_ref=source_ref,
                last_modified=last_modified,
            )

        logger.info(f"[{self.source_name}] Found {count} file(s)")

    def _list_configured_scope(self, drive_id: str) -> Iterator[tuple[str, dict]]:
        """Yield (drive_id, item) across every configured selector (file_ids, file_paths,
        folder_id/folder_path, or the drive root if none are set).

        file_ids/file_paths are always resolved against the connector's own configured
        drive — only items discovered via folder traversal can point into another drive
        (via remoteItem), so those are the only ones _walk_folder may yield a different
        drive_id for.
        """
        visited: set[tuple[str, str]] = set()

        for file_id in self.file_ids:
            item = self._graph.get_item(drive_id, file_id)
            if self._is_downloadable_file(item):
                yield (drive_id, item)
            else:
                logger.warning(f"[{self.source_name}] file_id={file_id!r} is not a downloadable file, skipping")

        for file_path in self.file_paths:
            item = self._graph.get_item_by_path(drive_id, file_path)
            if self._is_downloadable_file(item):
                yield (drive_id, item)
            else:
                logger.warning(f"[{self.source_name}] file_path={file_path!r} is not a downloadable file, skipping")

        if self.folder_id or self.folder_path or not (self.file_ids or self.file_paths):
            if self.folder_id:
                # Resolve explicitly so a missing/mistyped configured root fails the job
                # (via GraphItemNotFoundError propagating past _walk_folder's own try/
                # except, which only catches 404s discovered *during* the walk, not the
                # very first root lookup) instead of silently completing with 0 files.
                root = self._graph.get_item(drive_id, self.folder_id)
                root_item_id = root["id"]
            elif self.folder_path:
                root = self._graph.get_item_by_path(drive_id, self.folder_path)
                root_item_id = root["id"]
            else:
                root = self._graph.get_drive_root(drive_id)
                root_item_id = root["id"]

            yield from self._walk_folder(drive_id, root_item_id, visited)

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return the Graph eTag as a pre-fetch checksum.

        eTag changes on some metadata-only edits, not just content edits — an accepted
        tradeoff: a false-positive re-embed is wasteful but safe, and it keeps dedup
        working without a second Graph call to fetch a content-specific hash.
        """
        return item.source_ref.get("etag") or None

    def get_raw_content(self, item: IngestionItem) -> str:
        """Download the file's bytes fresh and convert to Markdown via MarkItDown."""
        ref = item.source_ref
        download_url = self._graph.get_download_url(ref["drive_id"], ref["item_id"])
        content_bytes = self._graph.download_content(download_url)

        file_extension = os.path.splitext(ref["name"])[1] or None
        converted = self.convert_to_markdown(content_bytes, file_extension=file_extension)
        return converted or content_bytes.decode("utf-8", errors="ignore")

    def get_item_name(self, item: IngestionItem) -> str:
        """Return the stable, unique key for this OneDrive file.

        drive_id:item_id survives rename/move — unlike a path-based key, which would
        treat a moved file as a brand-new item. The 'onedrive:' prefix already applied
        to item.id is reused directly since both IDs are opaque Graph identifiers with
        no unsafe characters.
        """
        return item.id

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return OneDrive-specific metadata fields.

        onedrive_file_name (not file_name) avoids colliding with BaseMetadataSchema's
        reserved file_name field, which process_item() already sets from get_item_name()
        and would silently drop any extra-metadata value using that key.
        """
        ref = item.source_ref
        return {
            "onedrive_file_name": ref.get("name", ""),
            "file_path": ref.get("parent_path", ""),
            "web_url": ref.get("web_url", ""),
        }
