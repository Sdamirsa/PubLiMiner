"""Pydantic config for the filter step."""

from __future__ import annotations

from pydantic import BaseModel, field_validator


class FilterConfig(BaseModel):
    """Keyword-based post-parse filter that tags papers with a boolean column.

    Reads the parsed `authors` JSON column and checks each author's `affiliation`
    string for any of the configured keywords (case-insensitive). Papers with at
    least `min_author_matches` matching authors get tagged True in `output_column`.

    Use `filter_column` in subsequent steps (e.g. extract) to skip untagged rows.
    """

    keywords: list[str] = []
    min_author_matches: int = 1
    output_column: str = "filter_match"
    also_check_columns: list[str] = []  # additional plain-text columns to check (e.g. "affiliation_raw")
    case_sensitive: bool = False
    drop_non_matching: bool = False  # if True: remove rows from parquet instead of tagging

    @field_validator("min_author_matches")
    @classmethod
    def min_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("min_author_matches must be >= 1")
        return v
