"""Tests for core/openrouter.py (async OpenRouter client)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from publiminer.core.openrouter import ExtractionResponse, GenerationStats, OpenRouterClient
from publiminer.exceptions import InsufficientCreditsError, NoProviderError, OpenRouterError


def _make_response(status: int = 200, body: dict | None = None, headers: dict | None = None) -> httpx.Response:
    """Build a fake httpx.Response."""
    body = body or {
        "id": "gen-abc123",
        "model": "openai/gpt-oss-120b",
        "choices": [{"message": {"content": '{"x": 1}'}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50},
    }
    resp = httpx.Response(
        status_code=status,
        content=json.dumps(body).encode(),
        headers={"X-Generation-Id": "gen-abc123", **(headers or {})},
    )
    return resp


@pytest.fixture()
async def client() -> OpenRouterClient:
    c = OpenRouterClient(api_key="sk-or-v1-test")
    yield c
    await c.aclose()


class TestExtract:
    async def test_successful_extract(self, client: OpenRouterClient) -> None:
        mock_resp = _make_response()
        with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)):
            result = await client.extract(
                messages=[{"role": "user", "content": "test"}],
                model="openai/gpt-oss-120b",
                response_format={"type": "json_schema", "json_schema": {"name": "t", "strict": True, "schema": {}}},
            )
        assert isinstance(result, ExtractionResponse)
        assert result.content == '{"x": 1}'
        assert result.generation_id == "gen-abc123"
        assert result.model_used == "openai/gpt-oss-120b"
        assert result.usage["prompt_tokens"] == 100

    async def test_generation_id_from_header(self, client: OpenRouterClient) -> None:
        body = {
            "id": "body-id",
            "model": "openai/gpt-oss-120b",
            "choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}],
            "usage": {},
        }
        resp = httpx.Response(
            200,
            content=json.dumps(body).encode(),
            headers={"X-Generation-Id": "header-id"},
        )
        with patch.object(client._client, "post", new=AsyncMock(return_value=resp)):
            result = await client.extract(
                messages=[],
                model="m",
                response_format={"type": "json_object"},
            )
        assert result.generation_id == "header-id"

    async def test_402_raises_insufficient_credits(self, client: OpenRouterClient) -> None:
        resp_402 = httpx.Response(402, content=b'{"error": "no credits"}')
        with patch.object(client._client, "post", new=AsyncMock(return_value=resp_402)):
            with pytest.raises(InsufficientCreditsError):
                await client.extract(messages=[], model="m", response_format={})

    async def test_503_raises_no_provider(self, client: OpenRouterClient) -> None:
        resp_503 = httpx.Response(503, content=b'{"error": "no provider"}')
        with patch.object(client._client, "post", new=AsyncMock(return_value=resp_503)):
            with pytest.raises(NoProviderError):
                await client.extract(messages=[], model="m", response_format={})

    async def test_retry_on_429(self, client: OpenRouterClient) -> None:
        resp_429 = httpx.Response(429, content=b'{"error": "rate limit"}')
        resp_ok = _make_response()
        calls = [resp_429, resp_429, resp_ok]
        call_iter = iter(calls)

        async def mock_post(*args, **kwargs):  # type: ignore[no-untyped-def]
            return next(call_iter)

        with patch.object(client._client, "post", new=AsyncMock(side_effect=mock_post)):
            with patch("asyncio.sleep", new=AsyncMock()):
                result = await client.extract(messages=[], model="m", response_format={})
        assert result.content == '{"x": 1}'

    async def test_exhausted_retries_raises(self, client: OpenRouterClient) -> None:
        resp_429 = httpx.Response(429, content=b'{}')
        with patch.object(client._client, "post", new=AsyncMock(return_value=resp_429)):
            with patch("asyncio.sleep", new=AsyncMock()):
                with pytest.raises(OpenRouterError):
                    await client.extract(messages=[], model="m", response_format={})


class TestGetGenerationStats:
    async def test_parses_stats(self, client: OpenRouterClient) -> None:
        stats_body = {
            "data": {
                "id": "gen-abc123",
                "model": "openai/gpt-oss-120b",
                "provider_name": "OpenAI",
                "total_cost": 0.0012,
                "tokens_prompt": 100,
                "tokens_completion": 50,
                "latency": 800,
                "created_at": "2026-05-05T12:00:00Z",
                "finish_reason": "stop",
            }
        }
        resp = httpx.Response(200, content=json.dumps(stats_body).encode())
        with patch.object(client._client, "get", new=AsyncMock(return_value=resp)):
            stats = await client.get_generation_stats("gen-abc123")
        assert isinstance(stats, GenerationStats)
        assert stats.provider_name == "OpenAI"
        assert stats.cost_usd == pytest.approx(0.0012)
        assert stats.latency_ms == 800


class TestFixJson:
    async def test_returns_repaired(self, client: OpenRouterClient) -> None:
        body = {
            "choices": [{"message": {"content": '{"fixed": true}'}}],
        }
        resp = httpx.Response(200, content=json.dumps(body).encode(), headers={})
        with patch.object(client._client, "post", new=AsyncMock(return_value=resp)):
            result = await client.fix_json("bad{json", {}, "openai/gpt-4o-mini")
        assert result == '{"fixed": true}'

    async def test_returns_unrecoverable_on_error(self, client: OpenRouterClient) -> None:
        with patch.object(client._client, "post", new=AsyncMock(side_effect=Exception("boom"))):
            result = await client.fix_json("bad", {}, "model")
        assert "_unrecoverable" in result


class TestContextManager:
    async def test_async_context_manager(self) -> None:
        async with OpenRouterClient(api_key="sk-or-v1-test") as c:
            assert c._client is not None
        # After exit, client should be closed (no error)
