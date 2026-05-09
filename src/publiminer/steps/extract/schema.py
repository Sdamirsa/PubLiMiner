"""Pydantic config model for the extract step."""

from __future__ import annotations

from pydantic import BaseModel, field_validator


class RepairConfig(BaseModel):
    pattern_fix: bool = True
    llm_fix: bool = True
    fix_model: str = ""  # empty = use same as main model


class ProviderConfig(BaseModel):
    order: list[str] = []
    allow_fallbacks: bool = True
    require_parameters: bool = True  # critical: prevents silent stripping of response_format
    data_collection: str = "deny"
    sort: str = "price"


class ReasoningConfig(BaseModel):
    enabled: bool = False
    effort: str = "medium"
    exclude: bool = False


class FieldDef(BaseModel):
    """One extraction field as defined by the user in YAML."""

    name: str
    type: str
    description: str
    required: bool = True
    parent: str | None = None
    values: list[str] = []


class ExtractConfig(BaseModel):
    schema_name: str = ""
    run_id: str = ""
    model: str = "openai/gpt-oss-120b"
    fallback_models: list[str] = ["anthropic/claude-haiku-4-5", "openai/gpt-4o-mini"]
    max_tokens: int = 2048
    temperature: float = 0.0
    seed: int = 42
    provider: ProviderConfig = ProviderConfig()
    reasoning: ReasoningConfig = ReasoningConfig()
    include_title: bool = True
    include_abstract: bool = True
    include_author_block: bool = True
    extra_columns: list[str] = []
    user_instruction: str = ""
    fields: list[FieldDef] = []
    repair: RepairConfig = RepairConfig()
    filter_column: str = ""
    max_cost_usd: float = 25.0
    concurrency: int = 20

    @field_validator("temperature")
    @classmethod
    def temp_range(cls, v: float) -> float:
        if not (0.0 <= v <= 2.0):
            raise ValueError(f"temperature must be between 0.0 and 2.0, got {v}")
        return v
