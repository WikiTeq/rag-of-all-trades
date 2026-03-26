import unittest
from datetime import UTC
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.trello_ingestion import TrelloIngestionJob


def _make_config(
    api_key="test_api_key",
    api_token="test_api_token",
    board_ids="",
    load_comments=False,
    max_comments=10,
):
    return {
        "name": "test_trello",
        "config": {
            "api_key": api_key,
            "api_token": api_token,
            "board_ids": board_ids,
            "load_comments": load_comments,
            "max_comments": max_comments,
        },
    }


def _make_label(name="bug"):
    lbl = Mock()
    lbl.name = name
    return lbl


def _make_card(
    card_id="507f1f77bcf86cd799439011",
    name="Test Card",
    description="Card description",
    url="https://trello.com/c/abc123",
    list_id="list001",
    due_date=None,
    labels=None,
    closed=False,
    date_last_activity="2024-06-01T12:00:00.000Z",
):
    card = Mock()
    card.id = card_id
    card.name = name
    card.description = description
    card.url = url
    card.list_id = list_id
    card.due_date = due_date
    card.due = due_date
    card.labels = labels or []
    card.closed = closed
    card.dateLastActivity = date_last_activity
    return card


def _make_list(list_id="list001", name="To Do"):
    lst = Mock()
    lst.id = list_id
    lst.name = name
    return lst


def _make_board(board_id="board001", name="Test Board", cards=None, lists=None):
    board = Mock()
    board.id = board_id
    board.name = name
    board.get_cards.return_value = cards or []
    board.list_lists.return_value = lists or []
    return board


def _make_job(config=None, **kwargs):
    """Create a TrelloIngestionJob with the TrelloClient patched out."""
    cfg = config or _make_config(**kwargs)
    with patch("tasks.trello_ingestion._TrelloClient"):
        job = TrelloIngestionJob(cfg)
    return job


class TestTrelloIngestionInit(unittest.TestCase):
    def test_init_success(self):
        job = _make_job()
        self.assertEqual(job.api_key, "test_api_key")
        self.assertEqual(job.api_token, "test_api_token")
        self.assertEqual(job.board_ids, [])
        self.assertFalse(job.load_comments)
        self.assertEqual(job.max_comments, 10)

    def test_init_with_board_ids_string(self):
        job = _make_job(board_ids="abc,def,ghi")
        self.assertEqual(job.board_ids, ["abc", "def", "ghi"])

    def test_init_with_board_ids_list(self):
        cfg = _make_config()
        cfg["config"]["board_ids"] = ["abc", "def"]
        job = _make_job(config=cfg)
        self.assertEqual(job.board_ids, ["abc", "def"])

    def test_init_with_comments(self):
        job = _make_job(load_comments=True, max_comments=5)
        self.assertTrue(job.load_comments)
        self.assertEqual(job.max_comments, 5)

    def test_missing_api_key_raises(self):
        with patch("tasks.trello_ingestion._TrelloClient"):
            with self.assertRaises(ValueError, msg="api_key is required"):
                TrelloIngestionJob({"name": "t", "config": {"api_key": "", "api_token": "tok"}})

    def test_missing_api_token_raises(self):
        with patch("tasks.trello_ingestion._TrelloClient"):
            with self.assertRaises(ValueError, msg="api_token is required"):
                TrelloIngestionJob({"name": "t", "config": {"api_key": "key", "api_token": ""}})

    def test_invalid_max_comments_raises(self):
        with patch("tasks.trello_ingestion._TrelloClient"):
            with self.assertRaises(ValueError):
                TrelloIngestionJob(_make_config(max_comments=0))

    def test_source_type(self):
        job = _make_job()
        self.assertEqual(job.source_type, "trello")


class TestTrelloGetBoards(unittest.TestCase):
    def test_returns_all_boards_when_no_ids_configured(self):
        job = _make_job()
        b1 = _make_board("b1", "Board 1")
        b2 = _make_board("b2", "Board 2")
        job._client.list_boards.return_value = [b1, b2]
        boards = job._get_boards()
        self.assertEqual(len(boards), 2)

    def test_returns_specific_boards_by_id(self):
        job = _make_job(board_ids="b1,b2")
        b1 = _make_board("b1", "Board 1")
        b2 = _make_board("b2", "Board 2")
        job._client.get_board.side_effect = [b1, b2]
        boards = job._get_boards()
        self.assertEqual(len(boards), 2)
        job._client.get_board.assert_any_call("b1")
        job._client.get_board.assert_any_call("b2")

    def test_skips_failed_board_fetch(self):
        job = _make_job(board_ids="b1,b2")
        job._client.get_board.side_effect = [Exception("Not found"), _make_board("b2")]
        boards = job._get_boards()
        self.assertEqual(len(boards), 1)

    def test_returns_empty_on_list_boards_failure(self):
        job = _make_job()
        job._client.list_boards.side_effect = Exception("API error")
        boards = job._get_boards()
        self.assertEqual(boards, [])


class TestTrelloListItems(unittest.TestCase):
    def test_yields_cards_from_board(self):
        job = _make_job()
        lst = _make_list("list1", "To Do")
        card = _make_card(list_id="list1")
        board = _make_board("b1", "Board 1", cards=[card], lists=[lst])
        job._client.list_boards.return_value = [board]

        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, f"trello:card:{card.id}")

    def test_skips_closed_cards(self):
        job = _make_job()
        open_card = _make_card(card_id="aaa", closed=False)
        closed_card = _make_card(card_id="bbb", closed=True)
        board = _make_board(cards=[open_card, closed_card])
        job._client.list_boards.return_value = [board]

        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "trello:card:aaa")

    def test_item_contains_board_card_and_list(self):
        job = _make_job()
        lst = _make_list("list1", "In Progress")
        card = _make_card(list_id="list1")
        board = _make_board(cards=[card], lists=[lst])
        job._client.list_boards.return_value = [board]

        items = list(job.list_items())
        b, c, trello_list = items[0].source_ref
        self.assertIs(b, board)
        self.assertIs(c, card)
        self.assertIs(trello_list, lst)

    def test_list_ref_is_none_when_list_not_found(self):
        job = _make_job()
        card = _make_card(list_id="unknown_list")
        board = _make_board(cards=[card], lists=[])
        job._client.list_boards.return_value = [board]

        items = list(job.list_items())
        _, _, trello_list = items[0].source_ref
        self.assertIsNone(trello_list)

    def test_skips_board_on_exception(self):
        job = _make_job()
        board = _make_board()
        board.get_cards.side_effect = Exception("API error")
        job._client.list_boards.return_value = [board]

        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_last_modified_parsed(self):
        job = _make_job()
        card = _make_card(date_last_activity="2024-06-01T12:00:00.000Z")
        board = _make_board(cards=[card])
        job._client.list_boards.return_value = [board]

        items = list(job.list_items())
        self.assertIsNotNone(items[0].last_modified)


class TestTrelloGetRawContent(unittest.TestCase):
    def _make_item(self, card=None, board=None, trello_list=None):
        card = card or _make_card()
        board = board or _make_board()
        return IngestionItem(
            id=f"trello:card:{card.id}",
            source_ref=(board, card, trello_list),
        )

    def test_includes_card_title(self):
        job = _make_job()
        item = self._make_item(_make_card(name="My Card"))
        content = job.get_raw_content(item)
        self.assertIn("# My Card", content)

    def test_includes_description(self):
        job = _make_job()
        item = self._make_item(_make_card(description="Some details here"))
        content = job.get_raw_content(item)
        self.assertIn("Some details here", content)

    def test_includes_list_name(self):
        job = _make_job()
        lst = _make_list(name="Backlog")
        item = self._make_item(trello_list=lst)
        content = job.get_raw_content(item)
        self.assertIn("Backlog", content)

    def test_includes_labels(self):
        job = _make_job()
        card = _make_card(labels=[_make_label("urgent"), _make_label("backend")])
        item = self._make_item(card)
        content = job.get_raw_content(item)
        self.assertIn("urgent", content)
        self.assertIn("backend", content)

    def test_includes_due_date(self):
        job = _make_job()
        card = _make_card(due_date="2024-12-31T23:59:59.000Z")
        item = self._make_item(card)
        content = job.get_raw_content(item)
        self.assertIn("Due:", content)
        self.assertIn("2024-12-31", content)

    def test_excludes_empty_description(self):
        job = _make_job()
        item = self._make_item(_make_card(description=""))
        content = job.get_raw_content(item)
        self.assertNotIn("None", content)

    def test_no_comments_when_disabled(self):
        job = _make_job(load_comments=False)
        card = _make_card()
        card.fetch_actions = Mock()
        item = self._make_item(card)
        job.get_raw_content(item)
        card.fetch_actions.assert_not_called()

    def test_includes_comments_when_enabled(self):
        job = _make_job(load_comments=True, max_comments=2)
        card = _make_card()
        card.fetch_actions.return_value = [
            {"memberCreator": {"fullName": "Alice"}, "date": "2024-06-01", "data": {"text": "First comment"}},
            {"memberCreator": {"fullName": "Bob"}, "date": "2024-05-01", "data": {"text": "Second comment"}},
        ]
        item = self._make_item(card)
        content = job.get_raw_content(item)
        self.assertIn("Alice", content)
        self.assertIn("First comment", content)

    def test_max_comments_limit(self):
        job = _make_job(load_comments=True, max_comments=1)
        card = _make_card()
        card.fetch_actions.return_value = [
            {"memberCreator": {"fullName": "Alice"}, "date": "2024-06-01", "data": {"text": "First"}},
            {"memberCreator": {"fullName": "Bob"}, "date": "2024-05-01", "data": {"text": "Second"}},
        ]
        item = self._make_item(card)
        content = job.get_raw_content(item)
        self.assertIn("First", content)
        self.assertNotIn("Second", content)


class TestTrelloGetItemName(unittest.TestCase):
    def test_returns_safe_name(self):
        job = _make_job()
        card = _make_card(card_id="507f1f77bcf86cd799439011")
        item = IngestionItem(
            id="trello:card:507f1f77bcf86cd799439011",
            source_ref=(_make_board(), card, None),
        )
        name = job.get_item_name(item)
        self.assertTrue(name.startswith("trello_card_"))
        self.assertIn("507f1f77bcf86cd799439011", name)

    def test_name_max_255_chars(self):
        job = _make_job()
        card = _make_card(card_id="a" * 300)
        item = IngestionItem(id="trello:card:x", source_ref=(_make_board(), card, None))
        self.assertLessEqual(len(job.get_item_name(item)), 255)


class TestTrelloGetDocumentMetadata(unittest.TestCase):
    def test_metadata_contains_required_fields(self):
        job = _make_job()
        lst = _make_list("list1", "Done")
        card = _make_card(
            card_id="507f1f77bcf86cd799439011",
            name="My Card",
            url="https://trello.com/c/abc",
            list_id="list1",
            labels=[_make_label("feature")],
        )
        board = _make_board("b1", "My Board")
        item = IngestionItem(
            id="trello:card:507f1f77bcf86cd799439011",
            source_ref=(board, card, lst),
        )

        meta = job.get_document_metadata(item, "trello_card_abc", "checksum123", 1, None)

        self.assertEqual(meta["card_id"], card.id)
        self.assertEqual(meta["card_title"], "My Card")
        self.assertEqual(meta["board_id"], "b1")
        self.assertEqual(meta["board_name"], "My Board")
        self.assertEqual(meta["url"], "https://trello.com/c/abc")
        self.assertEqual(meta["labels"], ["feature"])
        self.assertEqual(meta["list_id"], "list1")
        self.assertEqual(meta["list_name"], "Done")
        self.assertIn("creation_date", meta)
        self.assertEqual(meta["source"], "trello")

    def test_metadata_with_no_list(self):
        job = _make_job()
        card = _make_card()
        board = _make_board()
        item = IngestionItem(id="trello:card:x", source_ref=(board, card, None))
        meta = job.get_document_metadata(item, "name", "cs", 1, None)
        self.assertEqual(meta["list_id"], "")
        self.assertEqual(meta["list_name"], "")


class TestTrelloHelpers(unittest.TestCase):
    def test_parse_card_date_valid(self):
        result = TrelloIngestionJob._parse_card_date("2024-06-01T12:00:00.000Z")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2024)

    def test_parse_card_date_none(self):
        self.assertIsNone(TrelloIngestionJob._parse_card_date(None))

    def test_parse_card_date_invalid(self):
        self.assertIsNone(TrelloIngestionJob._parse_card_date("not-a-date"))

    def test_id_to_creation_date(self):
        # 507f1f77 = 1350922103 Unix timestamp = 2012-10-22
        result = TrelloIngestionJob._id_to_creation_date("507f1f77bcf86cd799439011")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2012)
        self.assertEqual(result.tzinfo, UTC)

    def test_id_to_creation_date_invalid(self):
        self.assertIsNone(TrelloIngestionJob._id_to_creation_date("zzzzzzzz"))

    def test_build_comments_section_empty(self):
        job = _make_job()
        card = Mock()
        card.fetch_actions.return_value = []
        result = job._build_comments_section(card)
        self.assertEqual(result, "")

    def test_build_comments_section_api_error(self):
        job = _make_job()
        card = Mock()
        card.fetch_actions.side_effect = Exception("API error")
        result = job._build_comments_section(card)
        self.assertEqual(result, "")

    def test_build_comments_uses_username_fallback(self):
        job = _make_job()
        card = Mock()
        card.id = "abc"
        card.fetch_actions.return_value = [
            {"memberCreator": {"username": "johndoe"}, "date": "2024-01-01", "data": {"text": "Hi"}}
        ]
        result = job._build_comments_section(card)
        self.assertIn("johndoe", result)


if __name__ == "__main__":
    unittest.main()
