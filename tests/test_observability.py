import unittest
from unittest.mock import MagicMock, patch

import utils.observability
from utils.observability import is_enabled, setup_observability


class TestSetupObservability(unittest.TestCase):
    def setUp(self):
        utils.observability._instrumentor = MagicMock(is_instrumented_by_opentelemetry=False)

    @patch("utils.observability._instrumentor")
    def test_setup_observability_disabled(self, mock_instrumentor):
        mock_instrumentor.is_instrumented_by_opentelemetry = False

        setup_observability({"tracing_enabled": False})

        mock_instrumentor.instrument.assert_not_called()
        self.assertFalse(is_enabled())

    @patch("utils.observability._instrumentor")
    def test_setup_observability_disabled_string(self, mock_instrumentor):
        mock_instrumentor.is_instrumented_by_opentelemetry = False

        setup_observability({"tracing_enabled": "false"})

        mock_instrumentor.instrument.assert_not_called()
        self.assertFalse(is_enabled())

    @patch("utils.observability._instrumentor")
    def test_setup_observability_enabled(self, mock_instrumentor):
        mock_instrumentor.is_instrumented_by_opentelemetry = True

        setup_observability({"tracing_enabled": True})

        mock_instrumentor.instrument.assert_called_once()
        self.assertTrue(is_enabled())

    @patch("utils.observability._instrumentor")
    def test_setup_observability_missing_enabled_key(self, mock_instrumentor):
        mock_instrumentor.is_instrumented_by_opentelemetry = False

        setup_observability({})

        mock_instrumentor.instrument.assert_not_called()
        self.assertFalse(is_enabled())


if __name__ == "__main__":
    unittest.main()
