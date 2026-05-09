"""Integration tests for ExtractStep (mocked OpenRouter)."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from publiminer.core.extraction_db import ExtractionDB
from publiminer.core.openrouter import ExtractionResponse, GenerationStats
from publiminer.exceptions import CostCapExceededError
from publiminer.steps.extract.schema import ExtractConfig, FieldDef, ProviderConfig
from publiminer.steps.extract.step import ExtractStep


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_global_config(output_dir: Path) -> Any:
    from publiminer.core.global_schema import GeneralConfig, GlobalConfig

    return GlobalConfig(general=GeneralConfig(output_dir=str(output_dir), on_error="fail"))


def _make_extract_config(**overrides: Any) -> ExtractConfig:
    base = dict(
        schema_name="test_schema",
        run_id="run_test",
        model="openai/gpt-oss-120b",
        fallback_models=[],
        fields=[
            FieldDef(name="study_type", type="string", description="Study type"),
        ],
        include_author_block=False,
        max_cost_usd=100.0,
        concurrency=2,
        repair={"pattern_fix": False, "llm_fix": False, "fix_model": ""},
    )
    base.update(overrides)
    return ExtractConfig(**base)


def _write_parquet(output_dir: Path, rows: list[dict]) -> None:
    df = pl.DataFrame(rows)
    df.write_parquet(output_dir / "papers.parquet")


def _make_ok_resp(content: str = '{"study_type": "RCT"}') -> ExtractionResponse:
    return ExtractionResponse(
        content=content,
        generation_id="gen-123",
        usage={"prompt_tokens": 100, "completion_tokens": 50},
        finish_reason="stop",
        model_used="openai/gpt-oss-120b",
    )


def _make_stats(cost: float = 0.001) -> GenerationStats:
    return GenerationStats(
        generation_id="gen-123",
        model="openai/gpt-oss-120b",
        provider_name="OpenAI",
        cost_usd=cost,
        prompt_tokens=100,
        completion_tokens=50,
        reasoning_tokens=0,
        cached_tokens=0,
        latency_ms=500,
        created_at="2026-05-05T12:00:00Z",
        finish_reason="stop",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestValidateInput:
    def test_raises_when_parquet_missing(self, tmp_path: Path) -> None:
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config()
        step = ExtractStep(global_cfg, cfg, tmp_path)
        with pytest.raises(Exception, match="papers.parquet not found"):
            step.validate_input()

    def test_raises_when_api_key_missing(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path, [{"pmid": "1", "title": "t", "abstract": "a"}])
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config()
        step = ExtractStep(global_cfg, cfg, tmp_path)
        env = {k: v for k, v in os.environ.items() if k != "OPENROUTER_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(Exception, match="OPENROUTER_API_KEY"):
                step.validate_input()

    def test_raises_when_schema_name_empty(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path, [{"pmid": "1"}])
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config(schema_name="")
        step = ExtractStep(global_cfg, cfg, tmp_path)
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with pytest.raises(Exception, match="schema_name"):
                step.validate_input()

    def test_raises_when_fields_empty(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path, [{"pmid": "1"}])
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config(fields=[])
        step = ExtractStep(global_cfg, cfg, tmp_path)
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with pytest.raises(Exception, match="fields"):
                step.validate_input()


def _run_step(step: ExtractStep, mock_client: Any) -> Any:
    """Run the async step in the test's event loop by calling _async_run directly."""
    import asyncio

    async def _go() -> Any:
        return await step._async_run()

    return asyncio.get_event_loop().run_until_complete(_go())


class TestRunBasic:
    async def test_extracts_3_papers(self, tmp_path: Path) -> None:
        papers = [
            {"pmid": f"p{i}", "title": f"Title {i}", "abstract": f"Abstract {i}"}
            for i in range(3)
        ]
        _write_parquet(tmp_path, papers)
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config()
        step = ExtractStep(global_cfg, cfg, tmp_path)

        mock_client = AsyncMock()
        mock_client.extract = AsyncMock(return_value=_make_ok_resp())
        mock_client.get_generation_stats = AsyncMock(return_value=_make_stats())
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with patch("publiminer.steps.extract.step.OpenRouterClient", return_value=mock_client):
                meta = await step._async_run()

        assert meta.extra["n_success"] == 3
        assert meta.extra["n_failed"] == 0
        db = ExtractionDB(tmp_path / "extractions.db")
        assert len(db.get_pending_pmids(["p0", "p1", "p2"], "test_schema", "run_test")) == 0

    async def test_resume_skips_done(self, tmp_path: Path) -> None:
        papers = [{"pmid": f"p{i}", "title": f"T{i}", "abstract": "a"} for i in range(3)]
        _write_parquet(tmp_path, papers)
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config()

        # Pre-populate one as done
        db = ExtractionDB(tmp_path / "extractions.db")
        from publiminer.core.extraction_db import ExtractionRecord
        db.write(ExtractionRecord(
            pmid="p0", schema_name="test_schema", run_id="run_test",
            raw_response="{}", extracted_json='{"study_type": "done"}',
            created_at="2026-05-05T00:00:00Z",
        ))

        mock_client = AsyncMock()
        mock_client.extract = AsyncMock(return_value=_make_ok_resp())
        mock_client.get_generation_stats = AsyncMock(return_value=_make_stats())
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        step = ExtractStep(global_cfg, cfg, tmp_path)
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with patch("publiminer.steps.extract.step.OpenRouterClient", return_value=mock_client):
                meta = await step._async_run()

        # Only 2 new extractions (p1 and p2)
        assert mock_client.extract.call_count == 2
        assert meta.extra["n_success"] == 2

    async def test_repair_path_applies_pattern_fix(self, tmp_path: Path) -> None:
        papers = [{"pmid": "r1", "title": "T", "abstract": "A"}]
        _write_parquet(tmp_path, papers)
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config(
            repair={"pattern_fix": True, "llm_fix": False, "fix_model": ""}
        )

        # Return malformed JSON that PatternFixer can fix (markdown fence)
        bad = '```json\n{"study_type": "RCT"}\n```'
        mock_client = AsyncMock()
        mock_client.extract = AsyncMock(return_value=ExtractionResponse(
            content=bad, generation_id="g1", usage={}, finish_reason="stop", model_used="m"
        ))
        mock_client.get_generation_stats = AsyncMock(return_value=_make_stats())
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        step = ExtractStep(global_cfg, cfg, tmp_path)
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with patch("publiminer.steps.extract.step.OpenRouterClient", return_value=mock_client):
                meta = await step._async_run()

        assert meta.extra["n_success"] == 1
        assert meta.extra["n_repaired"] == 1

    async def test_cost_cap_halts_extraction(self, tmp_path: Path) -> None:
        papers = [{"pmid": f"c{i}", "title": "T", "abstract": "A"} for i in range(10)]
        _write_parquet(tmp_path, papers)
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config(max_cost_usd=0.0005, concurrency=1)

        mock_client = AsyncMock()
        mock_client.extract = AsyncMock(return_value=_make_ok_resp())
        mock_client.get_generation_stats = AsyncMock(return_value=_make_stats(cost=0.001))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        step = ExtractStep(global_cfg, cfg, tmp_path)
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with patch("publiminer.steps.extract.step.OpenRouterClient", return_value=mock_client):
                with pytest.raises(CostCapExceededError):
                    await step._async_run()

    async def test_filter_column_limits_papers(self, tmp_path: Path) -> None:
        papers = [
            {"pmid": "f1", "title": "T1", "abstract": "A", "keep": "yes"},
            {"pmid": "f2", "title": "T2", "abstract": "A", "keep": None},
            {"pmid": "f3", "title": "T3", "abstract": "A", "keep": "yes"},
        ]
        _write_parquet(tmp_path, papers)
        global_cfg = _make_global_config(tmp_path)
        cfg = _make_extract_config(filter_column="keep")

        mock_client = AsyncMock()
        mock_client.extract = AsyncMock(return_value=_make_ok_resp())
        mock_client.get_generation_stats = AsyncMock(return_value=_make_stats())
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        step = ExtractStep(global_cfg, cfg, tmp_path)
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-test"}):
            with patch("publiminer.steps.extract.step.OpenRouterClient", return_value=mock_client):
                meta = await step._async_run()

        # Only 2 papers have keep != null
        assert mock_client.extract.call_count == 2
        assert meta.extra["n_success"] == 2
