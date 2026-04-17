"""Global configuration schema."""

from __future__ import annotations

from pydantic import BaseModel, Field

from publiminer.constants import DEFAULT_OUTPUT_DIR


class GeneralConfig(BaseModel):
    """Top-level general settings."""

    output_dir: str = DEFAULT_OUTPUT_DIR
    log_level: str = "INFO"
    seed: int = 42
    on_error: str = "skip"  # "skip" or "fail"
    max_error_rate: float = 0.05  # 5% threshold


class CacheConfig(BaseModel):
    """Cache settings."""

    ttl_days: int = 90


class GlobalConfig(BaseModel):
    """Full user-facing configuration."""

    general: GeneralConfig = Field(default_factory=GeneralConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    steps: list[str] = Field(
        default_factory=lambda: [
            "fetch",
            "parse",
            "deduplicate",
            "embed",
            "reduce",
            "cluster",
            "sample",
            "extract",
            "score",
            "trend",
            "rag",
            "patent",
            "export",
        ]
    )
