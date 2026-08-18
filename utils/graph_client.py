from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import msal

from utils.http import RetrySession

logger = logging.getLogger(__name__)


class GraphItemNotFoundError(RuntimeError):
    """Raised when a Graph API call returns 404 — the item/folder no longer exists.

    Distinguished from other Graph failures (auth, 403, 429, 5xx) so callers can choose
    to skip a single missing item without treating it the same as a fatal error.
    """


class GraphClient:
    """Thin Microsoft Graph API client for the OneDrive connector.

    Handles MSAL client-credentials auth, paginated Graph JSON calls, and
    file content download. Graph JSON endpoints require a Bearer token;
    OneDrive's pre-authenticated `@microsoft.graph.downloadUrl` values do
    not accept one, so JSON calls and content downloads use separate
    RetrySession instances with separate timeout budgets.
    """

    BASE_URL = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        max_retries: int = 3,
        max_file_size_bytes: int = 52428800,  # 50 MB
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.max_file_size_bytes = max_file_size_bytes

        self._msal_app = self._build_msal_app()
        self._api_session = RetrySession(max_retries=max_retries, timeout=30)
        self._download_session = RetrySession(max_retries=max_retries, timeout=120)

    def _build_msal_app(self) -> msal.ConfidentialClientApplication:
        return msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
        )

    def _get_access_token(self) -> str:
        result = self._msal_app.acquire_token_for_client(scopes=self.SCOPES)
        if "access_token" not in result:
            error_desc = result.get("error_description") or result.get("error") or "unknown error"
            raise RuntimeError(f"Failed to acquire Microsoft Graph access token: {error_desc}")
        return result["access_token"]

    def _force_token_refresh(self) -> None:
        # MSAL's client-credentials flow has no public "force refresh" call;
        # rebuilding the app discards its internal token cache.
        self._msal_app = self._build_msal_app()

    @staticmethod
    def _safe_error_body(resp: Any) -> str:
        try:
            body = resp.json()
            error = body.get("error", {})
            return error.get("message") or str(body)
        except Exception:
            return getattr(resp, "text", "")[:500]

    def _graph_get(self, url_or_path: str, *, _retried: bool = False) -> dict:
        url = url_or_path if url_or_path.startswith("http") else f"{self.BASE_URL}{url_or_path}"
        token = self._get_access_token()
        resp = self._api_session.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})

        if resp.status_code == 401 and not _retried:
            logger.warning("Graph API returned 401 — forcing token refresh and retrying once")
            self._force_token_refresh()
            return self._graph_get(url_or_path, _retried=True)

        if resp.status_code == 404:
            raise GraphItemNotFoundError(f"Graph item not found (404) for {url}: {self._safe_error_body(resp)}")

        if not resp.ok:
            raise RuntimeError(
                f"Graph API request failed ({resp.status_code}) for {url}: {self._safe_error_body(resp)}"
            )
        return resp.json()

    def get_user_drive_id(self, userprincipalname: str) -> str:
        """Resolve a user's OneDrive for Business drive ID from their user principal name.

        Uses `GET /users/{UPN}/drives` (List drives), not the singular `GET /users/{UPN}/drive`
        (Get drive) — Graph v1.0 documents application permissions as unsupported for the
        singular endpoint, and this client always authenticates via client credentials
        (app-only), so only the plural, app-permission-compatible endpoint matches the
        documented contract.
        """
        drives = list(self._list_user_drives(userprincipalname))

        business_drives = [d for d in drives if isinstance(d, dict) and d.get("driveType") == "business"]
        if len(business_drives) == 1:
            return self._require_drive_id(business_drives[0], userprincipalname)
        if len(business_drives) > 1:
            raise RuntimeError(
                f"Multiple OneDrive for Business drives found for userprincipalname="
                f"{userprincipalname!r} — cannot disambiguate: "
                f"{[d.get('id') for d in business_drives]}"
            )

        # No drive tagged driveType == "business" — fall back to a single unambiguous
        # drive only when its type is missing/empty, since some tenants surface the
        # business drive without that tag. A single drive with an explicit non-business
        # type (e.g. "personal", "documentLibrary") is not a match.
        untyped_drives = [d for d in drives if isinstance(d, dict) and not d.get("driveType")]
        if len(untyped_drives) == 1 and len(drives) == 1:
            return self._require_drive_id(untyped_drives[0], userprincipalname)

        raise RuntimeError(
            f"No OneDrive for Business drive found for userprincipalname={userprincipalname!r} "
            f"(found {len(drives)} drive(s), none tagged driveType=='business')"
        )

    def _list_user_drives(self, userprincipalname: str) -> Iterator[dict]:
        """Yield every drive from `GET /users/{UPN}/drives`, following `@odata.nextLink`.

        Mirrors list_children's pagination: the link is opaque and passed to _graph_get
        unmodified so server-side skip tokens and query params survive intact.
        """
        next_url: str | None = f"/users/{quote(userprincipalname, safe='')}/drives"
        while next_url:
            page = self._graph_get(next_url)
            value = page.get("value")
            if not isinstance(value, list):
                raise RuntimeError(
                    f"Malformed List drives response for userprincipalname={userprincipalname!r}: "
                    f"'value' is {type(value).__name__}, expected a list"
                )
            yield from value
            next_url = page.get("@odata.nextLink")

    @staticmethod
    def _require_drive_id(drive: dict, userprincipalname: str) -> str:
        drive_id = drive.get("id")
        if not drive_id:
            raise RuntimeError(f"Drive entry missing 'id' for userprincipalname={userprincipalname!r}: {drive!r}")
        return drive_id

    def get_drive_root(self, drive_id: str) -> dict:
        """Fetch the root folder item of a drive."""
        return self._graph_get(f"/drives/{quote(drive_id, safe='')}/root")

    def list_children(self, drive_id: str, item_id: str) -> Iterator[dict]:
        """Yield every child item (file or folder) of the given drive item, one page at a time.

        Follows `@odata.nextLink` until Graph stops returning one — the link is an opaque
        absolute URL and is passed to `_graph_get` unmodified (never reconstructed), so
        server-side skip tokens and query params survive intact.
        """
        next_url: str | None = f"/drives/{quote(drive_id, safe='')}/items/{quote(item_id, safe='')}/children"

        while next_url:
            page = self._graph_get(next_url)
            yield from page.get("value", [])
            next_url = page.get("@odata.nextLink")

    def get_item(self, drive_id: str, item_id: str) -> dict:
        """Fetch a single drive item's current metadata by ID (fresh, not cached)."""
        return self._graph_get(f"/drives/{quote(drive_id, safe='')}/items/{quote(item_id, safe='')}")

    def get_item_by_path(self, drive_id: str, path: str) -> dict:
        """Resolve a relative OneDrive path to its current drive item metadata.

        Each path segment is percent-encoded individually (not the path as a whole) so
        segment separators (`/`) are preserved while special characters within a segment
        (`#`, `%`, spaces, apostrophes) are safely encoded.
        """
        encoded_path = "/".join(quote(segment, safe="") for segment in path.strip("/").split("/"))
        return self._graph_get(f"/drives/{quote(drive_id, safe='')}/root:/{encoded_path}")

    def get_download_url(self, drive_id: str, item_id: str) -> str:
        """Fetch a fresh, short-lived pre-authenticated download URL for a file item.

        Called immediately before download rather than cached on a listed item — Graph's
        `@microsoft.graph.downloadUrl` is short-lived and listing may complete well before
        every item is actually downloaded.
        """
        item = self.get_item(drive_id, item_id)
        download_url = item.get("@microsoft.graph.downloadUrl")
        if not download_url:
            raise RuntimeError(f"No download URL available for drive_id={drive_id!r} item_id={item_id!r}")
        return download_url

    def download_content(self, download_url: str) -> bytes:
        """Download file content, enforcing max_file_size_bytes as a hard cap while streaming.

        Streams the response and checks cumulative bytes read against the cap as chunks
        arrive, rather than trusting the server-declared Content-Length (which can be
        absent, wrong, or exceeded by a server sending more than it declared).
        """
        resp = self._download_session.get(download_url, stream=True)
        if not resp.ok:
            raise RuntimeError(f"Download failed ({resp.status_code}) for {download_url}")

        chunks: list[bytes] = []
        total = 0
        try:
            for chunk in resp.iter_content(chunk_size=65536):
                total += len(chunk)
                if total > self.max_file_size_bytes:
                    raise RuntimeError(
                        f"File exceeds max_file_size_bytes ({self.max_file_size_bytes}) while downloading "
                        f"from {download_url}"
                    )
                chunks.append(chunk)
        finally:
            resp.close()

        return b"".join(chunks)
