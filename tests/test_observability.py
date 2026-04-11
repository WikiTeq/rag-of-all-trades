import unittest
from unittest.mock import MagicMock, patch

from utils.observability import setup_observability


class TestSetupObservability(unittest.TestCase):
    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_disabled(self, mock_instrumentor_cls):
        setup_observability({"enabled": False})

        mock_instrumentor_cls.assert_not_called()

    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_enabled(self, mock_instrumentor_cls):
        mock_instrumentor = MagicMock()
        mock_instrumentor_cls.return_value = mock_instrumentor

        config = {
            "enabled": True,
            "public_key": "pk-lf-test",
            "secret_key": "sk-lf-test",
            "host": "https://cloud.langfuse.com",
        }
        setup_observability(config)

        mock_instrumentor_cls.assert_called_once_with(
            public_key="pk-lf-test",
            secret_key="sk-lf-test",
            host="https://cloud.langfuse.com",
        )
        mock_instrumentor.start.assert_called_once()

    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_missing_enabled_key(self, mock_instrumentor_cls):
        setup_observability({})

        mock_instrumentor_cls.assert_not_called()

    @patch("utils.observability.LlamaIndexInstrumentor")
    def test_setup_observability_raises_on_missing_credentials(self, mock_instrumentor_cls):
        with self.assertRaises(ValueError):
            setup_observability({"enabled": True, "public_key": "", "secret_key": "", "host": ""})

        mock_instrumentor_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main()
