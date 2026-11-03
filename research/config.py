"""Shared process configuration bootstrap."""

from __future__ import annotations

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError as exc:  # pragma: no cover - only occurs before installation
    raise RuntimeError(
        "python-dotenv is required; install with `python3 -m pip install -r requirements.txt`"
    ) from exc


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"


def load_environment(env_file: Path = ENV_FILE) -> None:
    """Load dotenv values without overriding host-provided variables."""
    load_dotenv(env_file, override=False)


load_environment()
