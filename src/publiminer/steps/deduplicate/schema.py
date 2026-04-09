"""Deduplicate step configuration schema."""

from __future__ import annotations

from pydantic import BaseModel


class DeduplicateConfig(BaseModel):
    """Configuration for the deduplicate step."""

    check_doi: bool = True
    check_title_fuzzy: bool = True
    fuzzy_threshold: int = 90  # 0-100, higher = stricter matching
    remove_retracted: bool = True
