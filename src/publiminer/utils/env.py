"""Environment variable loading utilities."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


def load_env(env_path: str | Path | None = None) -> None:
    """Load environment variables from .env file.

    Args:
        env_path: Path to .env file. If None, searches current dir and parents.
    """
    if env_path is not None:
        load_dotenv(env_path)
    else:
        load_dotenv()


def get_env(key: str, default: str | None = None, required: bool = False) -> str | None:
    """Get an environment variable with optional requirement check.

    Args:
        key: Environment variable name.
        default: Default value if not set.
        required: If True, raise ValueError when missing.

    Returns:
        The environment variable value or default.

    Raises:
        ValueError: If required and not set.
    """
    value = os.getenv(key, default)
    if required and value is None:
        raise ValueError(f"Required environment variable {key!r} is not set")
    return value
