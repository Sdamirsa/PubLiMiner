"""Parse step configuration schema."""

from __future__ import annotations

from pydantic import BaseModel


class ParseConfig(BaseModel):
    """Configuration for the parse step."""

    min_abstract_length: int = 0  # Minimum abstract length to keep (0 = no filter)
    language_filter: str = ""  # If set, only keep papers in this language (e.g. "eng")
    remove_html: bool = True  # Strip HTML tags from text fields
    prepare_llm_input: bool = True  # Generate llm_input field
    flag_exclusions: bool = True  # Flag reviews, case reports, letters for exclusion
