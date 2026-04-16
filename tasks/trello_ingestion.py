import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from trello import TrelloClient

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class _TrelloClient(TrelloClient):
    """Subclass of TrelloClient that fixes a bug in py-trello where ``fetch_json``
    always sends ``data='{}'`` (an empty JSON body) even on GET requests.
    CloudFront treats GET requests with a non-empty body as malformed and returns 403.
    Overriding ``fetch_json`` to pass ``post_args=None`` for GET requests prevents this.

    Upstream issue: https://github.com/sarumont/py-trello/issues/373
    Fix merged but not yet released in py-trello==0.20.1 (PR #374).
    Remove this subclass once a fixed version is released and pinned.
    """

    def fetch_json(self, uri_path, http_method="GET", headers=None, query_params=None, post_args=None, files=None):
        if http_method == "GET":
            post_args = None
        return super().fetch_json(
            uri_path,
            http_method=http_method,
            headers=headers,
            query_params=query_params,
            post_args=post_args,
            files=files,
        )


class TrelloIngestionJob(IngestionJob):
    """Ingestion connector for Trello boards.

    Fetches cards from configured Trello boards (or all accessible boards if
    none are specified), converts their content to Markdown, and stores them
    in the vector store.

    Uses the ``py-trello`` SDK for all API interactions.

    Configuration (config.yaml):
        - config.api_key: Trello API key (required)
        - config.api_token: Trello API token (required)
        - config.board_ids: Comma-separated list of board IDs to ingest
          (optional; if omitted, all accessible boards are ingested)
        - config.load_comments: Whether to include card comments (optional, default False)
        - config.max_comments: Maximum comments per card, newest first (optional, default 10)
    """

    @property
    def source_type(self) -> str:
        return "trello"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.api_key = cfg.get("api_key", "").strip()
        if not self.api_key:
            raise ValueError("api_key is required in Trello connector config")

        self.api_token = cfg.get("api_token", "").strip()
        if not self.api_token:
            raise ValueError("api_token is required in Trello connector config")

        raw_board_ids = cfg.get("board_ids", "")
        if isinstance(raw_board_ids, list):
            self.board_ids: list[str] = [b.strip() for b in raw_board_ids if str(b).strip()]
        elif raw_board_ids:
            self.board_ids = [b.strip() for b in str(raw_board_ids).split(",") if b.strip()]
        else:
            self.board_ids = []

        self.load_comments = bool(cfg.get("load_comments", False))
        self.max_comments = int(cfg.get("max_comments", 10))
        if self.max_comments <= 0:
            raise ValueError("max_comments must be positive")

        self._client = _TrelloClient(api_key=self.api_key, token=self.api_token)

        logger.info(
            f"Initialized Trello connector "
            f"(board_ids={self.board_ids or 'all'}, "
            f"load_comments={self.load_comments}, max_comments={self.max_comments})"
        )

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per Trello card across configured boards."""
        boards = self._get_boards()
        logger.info(f"[{self.source_name}] Fetching cards from {len(boards)} board(s)")

        total = 0
        for board in boards:
            try:
                lists_by_id = {lst.id: lst for lst in board.list_lists()}
                cards = board.get_cards()
            except Exception as e:
                logger.error(f"[{self.source_name}] Failed to fetch cards from board {board.id!r}: {e}")
                continue

            for card in cards:
                if card.closed:
                    continue

                raw_ts = card.dateLastActivity
                if isinstance(raw_ts, str):
                    last_modified = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                else:
                    last_modified = raw_ts
                trello_list = lists_by_id.get(card.list_id)

                item = IngestionItem(
                    id=f"trello:card:{card.id}",
                    source_ref=(board, card, trello_list),
                    last_modified=last_modified,
                )
                yield item
                total += 1

        logger.info(f"[{self.source_name}] Found {total} card(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Build Markdown content from a Trello card."""
        board, card, trello_list = item.source_ref

        parts: list[str] = []

        # Title
        parts.append(f"# {card.name}\n")

        # Description
        desc = (card.description or "").strip()
        if desc:
            parts.append(desc)

        # List membership
        if trello_list:
            parts.append(f"**List:** {trello_list.name}")

        # Labels
        label_names = [lbl.name for lbl in (card.labels or []) if lbl.name]
        if label_names:
            parts.append(f"**Labels:** {', '.join(label_names)}")

        # Due date
        due = getattr(card, "due_date", None) or getattr(card, "due", None)
        if due:
            parts.append(f"**Due:** {due}")

        # Comments
        if self.load_comments:
            comments_md = self._build_comments_section(card)
            if comments_md:
                parts.append(comments_md)

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe identifier for the card."""
        _, card, _ = item.source_ref
        safe = re.sub(r"[^\w\-]", "_", card.id)
        return f"trello_card_{safe}"[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> dict[str, Any]:
        """Build metadata dict with Trello-specific fields."""
        board, card, trello_list = item.source_ref

        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)

        label_names = [lbl.name for lbl in (card.labels or []) if lbl.name]
        creation_date = self._id_to_creation_date(card.id)

        metadata.update(
            {
                "card_id": card.id,
                "card_title": card.name,
                "board_id": board.id,
                "board_name": board.name,
                "url": card.url,
                "labels": label_names,
                "due_date": str(getattr(card, "due_date", None) or getattr(card, "due", None) or ""),
                "creation_date": str(creation_date) if creation_date else "",
                "list_id": trello_list.id if trello_list else "",
                "list_name": trello_list.name if trello_list else "",
            }
        )
        return metadata

    def _get_boards(self):
        """Return the list of Board objects to ingest."""
        if self.board_ids:
            boards = []
            for board_id in self.board_ids:
                try:
                    boards.append(self._client.get_board(board_id))
                except Exception as e:
                    logger.error(f"[{self.source_name}] Failed to fetch board {board_id!r}: {e}")
            return boards
        try:
            return self._client.list_boards()
        except Exception as e:
            logger.error(f"[{self.source_name}] Failed to list boards: {e}")
            return []

    def _build_comments_section(self, card) -> str:
        """Fetch and format the latest N comments for a card as Markdown."""
        try:
            actions = card.fetch_actions(action_filter="commentCard")
        except Exception as e:
            logger.warning(f"[{self.source_name}] Failed to fetch comments for card {card.id}: {e}")
            return ""

        if not actions:
            return ""

        # Actions are returned newest-first from the Trello API
        top = actions[: self.max_comments]
        lines: list[str] = ["## Comments"]
        for action in top:
            member_creator = action.get("memberCreator", {})
            author = member_creator.get("fullName") or member_creator.get("username", "Unknown")
            date = action.get("date", "")
            text = action.get("data", {}).get("text", "")
            lines.append(f"**{author}** ({date}):\n{text}")

        return "\n\n".join(lines)

    @staticmethod
    def _id_to_creation_date(card_id: str) -> datetime | None:
        """Derive the card creation date from its Trello object ID (first 8 hex chars = Unix timestamp)."""
        try:
            ts = int(card_id[:8], 16)
            return datetime.fromtimestamp(ts, tz=UTC)
        except (ValueError, TypeError):
            return None
