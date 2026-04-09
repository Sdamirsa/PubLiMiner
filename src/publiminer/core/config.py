"""Configuration loader: step defaults -> user YAML -> runtime overrides."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from publiminer.core.global_schema import GlobalConfig
from publiminer.exceptions import ConfigError
from publiminer.utils.env import load_env


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base, returning a new dict."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dict."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def load_step_defaults(step_name: str) -> dict[str, Any]:
    """Load a step's default.yaml from its package directory.

    Args:
        step_name: Step name (e.g. 'fetch', 'parse').

    Returns:
        Dict of default configuration values.
    """
    step_dir = Path(__file__).parent.parent / "steps" / step_name
    return _load_yaml(step_dir / "default.yaml")


def load_config(
    user_config_path: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
    env_path: str | Path | None = None,
) -> GlobalConfig:
    """Load merged configuration.

    Merge order (lowest to highest priority):
        Global default.yaml -> User publiminer.yaml -> Runtime overrides

    Args:
        user_config_path: Path to user's publiminer.yaml.
        overrides: Runtime overrides dict.
        env_path: Path to .env file.

    Returns:
        Validated GlobalConfig instance.
    """
    # Load environment variables
    load_env(env_path)

    # 1. Load global defaults
    global_defaults = _load_yaml(Path(__file__).parent / "default.yaml")

    # 2. Load user config
    user_config: dict[str, Any] = {}
    if user_config_path is not None:
        user_path = Path(user_config_path)
        if not user_path.exists():
            raise ConfigError(f"Config file not found: {user_path}")
        user_config = _load_yaml(user_path)

    # 3. Merge: defaults -> user -> overrides
    merged = _deep_merge(global_defaults, user_config)
    if overrides:
        merged = _deep_merge(merged, overrides)

    # 4. Validate
    return GlobalConfig(**merged)


def load_step_config(
    step_name: str,
    schema_cls: type,
    global_config: GlobalConfig,
    user_config_path: str | Path | None = None,
) -> Any:
    """Load configuration for a specific step.

    Merge order: step default.yaml -> user YAML[step_name] section -> global overrides

    Args:
        step_name: Step name (e.g. 'fetch').
        schema_cls: Pydantic model class for this step's config.
        global_config: Already-loaded global config.
        user_config_path: Path to user's publiminer.yaml.

    Returns:
        Validated step config instance.
    """
    # Step defaults
    step_defaults = load_step_defaults(step_name)

    # User overrides for this step
    user_step: dict[str, Any] = {}
    if user_config_path is not None:
        full_user = _load_yaml(Path(user_config_path))
        user_step = full_user.get(step_name, {})
        if not isinstance(user_step, dict):
            user_step = {}

    # Merge
    merged = _deep_merge(step_defaults, user_step)
    return schema_cls(**merged)
