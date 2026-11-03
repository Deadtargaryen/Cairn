from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import config


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class ConfigTests(unittest.TestCase):
    def test_dotenv_values_load_for_entry_point(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("TEST_DOTENV_VALUE=from-file\n", encoding="utf-8")
            os.environ.pop("TEST_DOTENV_VALUE", None)
            config.load_environment(env_file)
            self.assertEqual(os.environ["TEST_DOTENV_VALUE"], "from-file")

    def test_host_environment_takes_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("TEST_DOTENV_VALUE=from-file\n", encoding="utf-8")
            os.environ["TEST_DOTENV_VALUE"] = "from-host"
            config.load_environment(env_file)
            self.assertEqual(os.environ["TEST_DOTENV_VALUE"], "from-host")
            os.environ.pop("TEST_DOTENV_VALUE", None)

    def test_env_example_has_safe_live_defaults(self):
        template = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("ENGINE_MODE=PAPER", template)
        self.assertIn("BINANCE_ORDER_MODE=observe", template)
        self.assertIn("BINANCE_ENABLE_LIVE_ORDERS=false", template)
        self.assertIn("BINANCE_API_KEY=", template)
        self.assertIn("BINANCE_API_SECRET=", template)


if __name__ == "__main__":
    unittest.main()
