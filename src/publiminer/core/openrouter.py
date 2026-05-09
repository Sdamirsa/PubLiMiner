"""Async OpenRouter API client for LLM extraction."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

from publiminer.exceptions import InsufficientCreditsError, NoProviderError, OpenRouterError

_BASE_URL = "https://openrouter.ai/api/v1"
_MAX_RETRIES = 5
_RETRY_CODES = {408, 429, 502}
_STRUCTURED_OUTPUT_VERSION = "structured-outputs-2025-11-13"


@dataclass
class GenerationStats:
    """Cost and latency data from /api/v1/generation."""

    generation_id: str
    model: str
    provider_name: str
    cost_usd: float
    prompt_tokens: int
    completion_tokens: int
    reasoning_tokens: int
    cached_tokens: int
    latency_ms: int
    created_at: str
    finish_reason: str


@dataclass
class ExtractionResponse:
    """Response from a single /chat/completions call."""

    content: str
    generation_id: str
    usage: dict[str, Any]
    finish_reason: str
    model_used: str


class OpenRouterClient:
    """Async OpenRouter client with retry, structured output, and cost tracking."""

    def __init__(
        self,
        api_key: str,
        app_url: str = "https://github.com/sdamirsa/PubLiMiner",
        app_title: str = "PubLiMiner",
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": app_url,
                "X-OpenRouter-Title": app_title,
                "OpenRouter-Version": _STRUCTURED_OUTPUT_VERSION,
                "Content-Type": "application/json",
            },
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
            timeout=httpx.Timeout(120.0),
        )

    async def extract(
        self,
        messages: list[dict[str, str]],
        model: str,
        response_format: dict[str, Any],
        provider: dict[str, Any] | None = None,
        fallback_models: list[str] | None = None,
        reasoning: dict[str, Any] | None = None,
        max_tokens: int = 2048,
        temperature: float = 0.0,
        seed: int = 42,
    ) -> ExtractionResponse:
        """Send a chat completion request and return structured response."""
        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "response_format": response_format,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "seed": seed,
        }
        if provider:
            body["provider"] = provider
        if fallback_models:
            body["models"] = fallback_models
        if reasoning:
            body["reasoning"] = reasoning

        resp = await self._post_with_retry("/chat/completions", body)
        data = resp.json()

        generation_id = resp.headers.get("X-Generation-Id", data.get("id", ""))
        choice = data["choices"][0]
        content = choice["message"].get("content") or ""
        finish_reason = choice.get("finish_reason", "")
        model_used = data.get("model", model)
        usage = data.get("usage", {})

        return ExtractionResponse(
            content=content,
            generation_id=generation_id,
            usage=usage,
            finish_reason=finish_reason,
            model_used=model_used,
        )

    async def get_generation_stats(self, generation_id: str) -> GenerationStats:
        """Fetch cost and latency from /api/v1/generation — the only source of provider_name and cost."""
        resp = await self._get_with_retry(f"/generation?id={generation_id}")
        d = resp.json().get("data", {})
        return GenerationStats(
            generation_id=d.get("id", generation_id),
            model=d.get("model", ""),
            provider_name=d.get("provider_name", ""),
            cost_usd=float(d.get("total_cost") or 0.0),
            prompt_tokens=int(d.get("tokens_prompt") or 0),
            completion_tokens=int(d.get("tokens_completion") or 0),
            reasoning_tokens=int(d.get("native_tokens_reasoning") or 0),
            cached_tokens=int(d.get("cached_tokens") or 0),
            latency_ms=int(d.get("latency") or 0),
            created_at=d.get("created_at", ""),
            finish_reason=d.get("finish_reason", ""),
        )

    async def fix_json(self, raw: str, schema_dict: dict[str, Any], fix_model: str) -> str:
        """Ask LLM to repair malformed JSON. Returns repaired string or '{"_unrecoverable": true}'."""
        body: dict[str, Any] = {
            "model": fix_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a JSON repair specialist. "
                        "Return ONLY the repaired JSON. "
                        'If unrecoverable, return exactly: {"_unrecoverable": true}'
                    ),
                },
                {
                    "role": "user",
                    "content": f"SCHEMA:\n{schema_dict}\n\nMALFORMED OUTPUT:\n{raw}",
                },
            ],
            "max_tokens": 4096,
            "temperature": 0.0,
        }
        try:
            resp = await self._post_with_retry("/chat/completions", body)
            data = resp.json()
            return data["choices"][0]["message"].get("content") or '{"_unrecoverable": true}'
        except Exception:
            return '{"_unrecoverable": true}'

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> OpenRouterClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def _post_with_retry(self, path: str, body: dict[str, Any]) -> httpx.Response:
        delay = 1.0
        for attempt in range(_MAX_RETRIES):
            resp = await self._client.post(path, json=body)
            if resp.status_code == 402:
                raise InsufficientCreditsError("Insufficient OpenRouter credits", status_code=402)
            if resp.status_code == 503:
                raise NoProviderError("No provider available for this request", status_code=503)
            if resp.status_code in _RETRY_CODES:
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(min(delay, 60.0))
                    delay *= 2
                    continue
                raise OpenRouterError(
                    f"OpenRouter request failed after {_MAX_RETRIES} retries (HTTP {resp.status_code})",
                    status_code=resp.status_code,
                )
            if resp.status_code >= 400:
                raise OpenRouterError(
                    f"OpenRouter error: {resp.text[:500]}",
                    status_code=resp.status_code,
                )
            return resp
        raise OpenRouterError(f"OpenRouter request failed after {_MAX_RETRIES} retries")

    async def _get_with_retry(self, path: str) -> httpx.Response:
        delay = 1.0
        for attempt in range(_MAX_RETRIES):
            resp = await self._client.get(path)
            if resp.status_code in _RETRY_CODES:
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(min(delay, 60.0))
                    delay *= 2
                    continue
                raise OpenRouterError(
                    f"OpenRouter GET failed after {_MAX_RETRIES} retries (HTTP {resp.status_code})",
                    status_code=resp.status_code,
                )
            if resp.status_code >= 400:
                raise OpenRouterError(
                    f"OpenRouter GET error: {resp.text[:500]}",
                    status_code=resp.status_code,
                )
            return resp
        raise OpenRouterError(f"OpenRouter GET failed after {_MAX_RETRIES} retries")
