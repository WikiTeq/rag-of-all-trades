import unittest
from unittest.mock import MagicMock, patch

import utils.observability
from utils.observability import get_instrumentor, setup_observability


class TestSetupObservability(unittest.TestCase):
    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_disabled(self, mock_instrumentor_cls):
        setup_observability({"enabled": False})

        mock_instrumentor_cls.assert_not_called()

    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_enabled(self, mock_instrumentor_cls):
        mock_instrumentor = MagicMock()
        mock_instrumentor_cls.return_value = mock_instrumentor

        setup_observability({"enabled": True})

        mock_instrumentor_cls.assert_called_once_with()
        mock_instrumentor.start.assert_called_once()

    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_missing_enabled_key(self, mock_instrumentor_cls):
        setup_observability({})

        mock_instrumentor_cls.assert_not_called()

    def test_get_instrumentor_returns_none_when_disabled(self):
        utils.observability._instrumentor = None
        self.assertIsNone(get_instrumentor())

    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_get_instrumentor_returns_instance_when_enabled(self, mock_instrumentor_cls):
        mock_instrumentor = MagicMock()
        mock_instrumentor_cls.return_value = mock_instrumentor
        utils.observability._instrumentor = None

        setup_observability({"enabled": True})

        self.assertIs(get_instrumentor(), mock_instrumentor)
        utils.observability._instrumentor = None


if __name__ == "__main__":
    unittest.main()
