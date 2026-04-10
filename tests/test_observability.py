import os
import unittest
from unittest.mock import patch

import llama_index.core as li_core
from llama_index.core import Settings

from utils.observability import setup_observability


class TestSetupObservability(unittest.TestCase):
    def setUp(self):
        # Ensure a clean LlamaIndex global handler state before each test
        li_core.global_handler = None
        Settings._callback_manager = None
        for key in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST"):
            os.environ.pop(key, None)

    def tearDown(self):
        li_core.global_handler = None
        Settings._callback_manager = None
        for key in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST"):
            os.environ.pop(key, None)

    @patch("utils.observability.set_global_handler")
    def test_setup_observability_disabled(self, mock_set_handler):
        setup_observability({"enabled": False})

        mock_set_handler.assert_not_called()

    @patch("utils.observability.set_global_handler")
    def test_setup_observability_enabled(self, mock_set_handler):
        config = {
            "enabled": True,
            "public_key": "pk-lf-test",
            "secret_key": "sk-lf-test",
            "host": "https://cloud.langfuse.com",
        }
        setup_observability(config)

        mock_set_handler.assert_called_once_with(
            "langfuse",
            public_key="pk-lf-test",
            secret_key="sk-lf-test",
            host="https://cloud.langfuse.com",
        )
        self.assertIsNotNone(Settings.callback_manager)

    @patch("utils.observability.set_global_handler")
    def test_setup_observability_sets_env_vars(self, mock_set_handler):
        config = {
            "enabled": True,
            "public_key": "pk-lf-abc",
            "secret_key": "sk-lf-xyz",
            "host": "http://localhost:3000",
        }
        setup_observability(config)

        self.assertEqual(os.environ.get("LANGFUSE_PUBLIC_KEY"), "pk-lf-abc")
        self.assertEqual(os.environ.get("LANGFUSE_SECRET_KEY"), "sk-lf-xyz")
        self.assertEqual(os.environ.get("LANGFUSE_HOST"), "http://localhost:3000")

    @patch("utils.observability.set_global_handler")
    def test_setup_observability_missing_enabled_key(self, mock_set_handler):
        setup_observability({})

        mock_set_handler.assert_not_called()

    @patch("utils.observability.set_global_handler")
    def test_setup_observability_raises_on_missing_credentials(self, mock_set_handler):
        with self.assertRaises(ValueError):
            setup_observability({"enabled": True, "public_key": "", "secret_key": "", "host": ""})

        mock_set_handler.assert_not_called()


if __name__ == "__main__":
    unittest.main()
