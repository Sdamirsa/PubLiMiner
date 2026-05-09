"""Custom exception hierarchy for PubLiMiner."""

from __future__ import annotations


class PubLiMinerError(Exception):
    """Base exception for all PubLiMiner errors."""


class ConfigError(PubLiMinerError):
    """Configuration loading or validation error."""


class StepError(PubLiMinerError):
    """Error during step execution."""

    def __init__(self, step_name: str, message: str) -> None:
        self.step_name = step_name
        super().__init__(f"[{step_name}] {message}")


class APIError(PubLiMinerError):
    """External API call error."""

    def __init__(self, service: str, message: str, status_code: int | None = None) -> None:
        self.service = service
        self.status_code = status_code
        suffix = f" (HTTP {status_code})" if status_code else ""
        super().__init__(f"[{service}] {message}" + suffix)


class CacheError(PubLiMinerError):
    """Cache read/write error."""


class SpineError(PubLiMinerError):
    """Parquet backbone error."""


class ValidationError(PubLiMinerError):
    """Data validation error."""


class OpenRouterError(PubLiMinerError):
    """OpenRouter API error."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        self.status_code = status_code
        super().__init__(message)


class InsufficientCreditsError(OpenRouterError):
    """OpenRouter account has insufficient credits (HTTP 402)."""


class NoProviderError(OpenRouterError):
    """No OpenRouter provider available for the requested model/params (HTTP 503)."""


class CostCapExceededError(PubLiMinerError):
    """Extraction halted because max_cost_usd was reached."""

    def __init__(self, cap: float, actual: float) -> None:
        self.cap = cap
        self.actual = actual
        super().__init__(f"Cost cap ${cap:.4f} exceeded (actual ${actual:.4f}); extraction halted")
