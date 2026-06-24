import unittest
from unittest.mock import MagicMock, patch

import utils.observability
from utils.observability import setup_observability


class TestSetupObservability(unittest.TestCase):
    def setUp(self):
        utils.observability._instrumentor = MagicMock()

    def test_setup_observability_calls_instrument(self):
        with patch("utils.observability.langfuse_client") as mock_client:
            mock_client.auth_check.return_value = True
            setup_observability()

        utils.observability._instrumentor.instrument.assert_called_once()

    def test_setup_observability_auth_check_success(self):
        with patch("utils.observability.langfuse_client") as mock_client:
            mock_client.auth_check.return_value = True
            with self.assertLogs("utils.observability", level="INFO") as cm:
                setup_observability()

        self.assertTrue(any("authenticated" in line for line in cm.output))

    def test_setup_observability_auth_check_failure(self):
        with patch("utils.observability.langfuse_client") as mock_client:
            mock_client.auth_check.return_value = False
            with self.assertLogs("utils.observability", level="WARNING") as cm:
                setup_observability()

        self.assertTrue(any("authentication failed" in line for line in cm.output))

    def test_setup_observability_auth_check_exception(self):
        with patch("utils.observability.langfuse_client") as mock_client:
            mock_client.auth_check.side_effect = Exception("unreachable")
            with self.assertLogs("utils.observability", level="WARNING") as cm:
                setup_observability()

        self.assertTrue(any("auth check failed" in line for line in cm.output))


if __name__ == "__main__":
    unittest.main()
