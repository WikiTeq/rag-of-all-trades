import unittest
from unittest.mock import MagicMock, patch

import utils.observability
from utils.observability import setup_observability


class TestSetupObservability(unittest.TestCase):
    def setUp(self):
        utils.observability._instrumentor = MagicMock(is_instrumented_by_opentelemetry=False)

    @patch("utils.observability._instrumentor")
    def test_setup_observability_disabled(self, mock_instrumentor):
        setup_observability({"tracing_enabled": False})

        mock_instrumentor.instrument.assert_not_called()

    @patch("utils.observability._instrumentor")
    def test_setup_observability_disabled_string(self, mock_instrumentor):
        setup_observability({"tracing_enabled": "false"})

        mock_instrumentor.instrument.assert_not_called()

    @patch("utils.observability._instrumentor")
    def test_setup_observability_enabled(self, mock_instrumentor):
        setup_observability({"tracing_enabled": True})

        mock_instrumentor.instrument.assert_called_once()

    @patch("utils.observability._instrumentor")
    def test_setup_observability_missing_enabled_key(self, mock_instrumentor):
        setup_observability({})

        mock_instrumentor.instrument.assert_not_called()


if __name__ == "__main__":
    unittest.main()
