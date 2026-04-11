"""Fetch step configuration schema."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


def _none_to_empty(v: object) -> str:
    return "" if v is None else str(v)


class FetchConfig(BaseModel):
    """Configuration for the fetch step."""

    query: str = ""
    start_date: str = ""  # YYYY/MM/DD or "auto"
    end_date: str = ""  # YYYY/MM/DD (empty = today)
    email: str = ""
    api_key: str = ""  # NCBI API key (also checked from NCBI_API_KEY env var)

    @field_validator("query", "start_date", "end_date", "email", "api_key", mode="before")
    @classmethod
    def _coerce_none(cls, v: object) -> str:
        return _none_to_empty(v)

    max_results: int = 0  # 0 = no limit
    batch_size: int = 500
    retry_attempts: int = 3
    rate_limit_per_second: float = 3.0
    download_mode: str = "full"  # "full" or "summary"
    ret_mode: str = "xml"
    ret_type: str = ""
