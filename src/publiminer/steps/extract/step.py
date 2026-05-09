"""ExtractStep — LLM extraction with per-paper async OpenRouter calls."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import polars as pl

from publiminer.core.base_step import StepBase
from publiminer.core.extraction_db import ExtractionDB, ExtractionRecord
from publiminer.core.io import StepMeta
from publiminer.core.openrouter import OpenRouterClient
from publiminer.exceptions import CostCapExceededError, StepError
from publiminer.steps.extract.author_mapper import build_author_block
from publiminer.steps.extract.prompt import build_messages
from publiminer.steps.extract.repair import repair
from publiminer.steps.extract.schema import ExtractConfig
from publiminer.steps.extract.schema_builder import (
    FieldDef as SchemaFieldDef,
    build_json_schema,
    validate_fields,
)
from publiminer.utils.progress import ProgressReporter


class ExtractStep(StepBase):
    """LLM extraction step using OpenRouter with async concurrent requests."""

    name = "extract"

    def __init__(self, global_config: Any, step_config: ExtractConfig, output_dir: Any = None) -> None:
        super().__init__(global_config, step_config, output_dir)
        self.config: ExtractConfig = step_config

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_input(self) -> None:
        if not self.spine.exists:
            raise StepError(self.name, "papers.parquet not found — run fetch+parse first")
        if not os.environ.get("OPENROUTER_API_KEY"):
            raise StepError(self.name, "OPENROUTER_API_KEY environment variable is not set")
        if not self.config.schema_name:
            raise StepError(self.name, "extract.schema_name must not be empty")
        if not self.config.fields:
            raise StepError(self.name, "extract.fields must not be empty")
        # Validate field definitions
        field_defs = [SchemaFieldDef(**f.model_dump()) for f in self.config.fields]
        validate_fields(field_defs)
        # Check requested columns exist in parquet
        import pyarrow.parquet as pq
        schema_names = set(pq.ParquetFile(self.spine.parquet_path).schema_arrow.names)
        if self.config.include_author_block and "authors" not in schema_names:
            self.logger.warning("include_author_block=true but 'authors' column not found; skipping author block")
        if self.config.filter_column and self.config.filter_column not in schema_names:
            raise StepError(self.name, f"filter_column '{self.config.filter_column}' not in parquet schema")

    def validate_output(self) -> None:
        db = ExtractionDB(self.output_dir / "extractions.db")
        summary = db.get_summary(self.config.schema_name, self._last_run_id)
        n_total = summary.get("n_success", 0) + summary.get("n_failed", 0)
        if n_total == 0:
            return
        failure_rate = summary.get("n_failed", 0) / n_total
        max_rate = self.global_config.general.max_error_rate
        if failure_rate > max_rate:
            raise StepError(
                self.name,
                f"Extraction failure rate {failure_rate:.1%} exceeds max_error_rate {max_rate:.1%}",
            )

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> StepMeta:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        return asyncio.run(self._async_run())

    async def _async_run(self) -> StepMeta:
        meta = self.meta
        meta.rows_before = self.spine.count() if self.spine.exists else 0

        run_id = self.config.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._last_run_id = run_id

        # Build JSON schema + field defs
        field_defs = [SchemaFieldDef(**f.model_dump()) for f in self.config.fields]
        response_format = build_json_schema(field_defs, self.config.schema_name)
        schema_dict = response_format["json_schema"]["schema"]

        # Determine which columns to read
        read_cols: list[str] = ["pmid"]
        import pyarrow.parquet as pq
        schema_names = set(pq.ParquetFile(self.spine.parquet_path).schema_arrow.names)
        if self.config.include_title and "title" in schema_names:
            read_cols.append("title")
        if self.config.include_abstract and "abstract" in schema_names:
            read_cols.append("abstract")
        if self.config.include_author_block and "authors" in schema_names:
            read_cols.append("authors")
        for col in self.config.extra_columns:
            if col in schema_names:
                read_cols.append(col)
        if self.config.filter_column and self.config.filter_column in schema_names:
            read_cols.append(self.config.filter_column)

        df = self.spine.read(columns=list(dict.fromkeys(read_cols)))
        if self.config.filter_column and self.config.filter_column in df.columns:
            df = df.filter(pl.col(self.config.filter_column) == True)  # noqa: E712
        all_pmids: list[str] = df["pmid"].to_list()

        db = ExtractionDB(self.output_dir / "extractions.db")
        pending_pmids = db.get_pending_pmids(all_pmids, self.config.schema_name, run_id)
        paper_lookup: dict[str, dict[str, Any]] = {row["pmid"]: row for row in df.to_dicts()}

        self.logger.info(
            f"Extract [{self.config.schema_name}]: {len(pending_pmids)} pending / {len(all_pmids)} total"
        )

        api_key = os.environ["OPENROUTER_API_KEY"]
        n_success = 0
        n_failed = 0
        n_repaired = 0
        total_cost = 0.0
        fix_model = self.config.repair.fix_model or self.config.model
        cost_cap_hit = False

        provider_dict: dict[str, Any] = {
            "require_parameters": self.config.provider.require_parameters,
            "allow_fallbacks": self.config.provider.allow_fallbacks,
            "data_collection": self.config.provider.data_collection,
        }
        if self.config.provider.order:
            provider_dict["order"] = self.config.provider.order
        if self.config.provider.sort:
            provider_dict["sort"] = self.config.provider.sort

        reasoning_dict: dict[str, Any] | None = None
        if self.config.reasoning.enabled:
            reasoning_dict = {
                "effort": self.config.reasoning.effort,
                "exclude": self.config.reasoning.exclude,
            }

        async with OpenRouterClient(api_key=api_key) as client:
            semaphore = asyncio.Semaphore(self.config.concurrency)

            async def process_one(pmid: str) -> None:
                nonlocal n_success, n_failed, n_repaired, total_cost, cost_cap_hit

                async with semaphore:
                    if cost_cap_hit:
                        return

                    paper_row = paper_lookup[pmid]
                    author_block = ""
                    if self.config.include_author_block:
                        author_block = build_author_block(paper_row.get("authors"))

                    messages = build_messages(paper_row, author_block, self.config)

                    try:
                        resp = await client.extract(
                            messages=messages,
                            model=self.config.model,
                            response_format=response_format,
                            provider=provider_dict,
                            fallback_models=self.config.fallback_models or None,
                            reasoning=reasoning_dict,
                            max_tokens=self.config.max_tokens,
                            temperature=self.config.temperature,
                            seed=self.config.seed,
                        )
                    except Exception as exc:
                        self.logger.warning(f"PMID {pmid}: API error — {exc}")
                        db.write(
                            ExtractionRecord(
                                pmid=pmid,
                                schema_name=self.config.schema_name,
                                run_id=run_id,
                                raw_response="",
                                error_label=f"api_error: {type(exc).__name__}",
                                created_at=_now_iso(),
                            )
                        )
                        n_failed += 1
                        return

                    # Parse / repair
                    extracted_json: str | None = None
                    repair_result = None
                    try:
                        json.loads(resp.content)
                        extracted_json = resp.content
                    except (json.JSONDecodeError, TypeError):
                        repair_result = await repair(
                            raw=resp.content,
                            schema_dict=schema_dict,
                            pattern_fix=self.config.repair.pattern_fix,
                            llm_fix=self.config.repair.llm_fix,
                            client=client,
                            fix_model=fix_model,
                        )
                        if repair_result.success:
                            extracted_json = repair_result.content

                    # Fetch generation stats (best-effort — don't fail the extraction)
                    stats = None
                    if resp.generation_id:
                        try:
                            stats = await client.get_generation_stats(resp.generation_id)
                        except Exception:
                            pass

                    record = ExtractionRecord(
                        pmid=pmid,
                        schema_name=self.config.schema_name,
                        run_id=run_id,
                        raw_response=resp.content,
                        extracted_json=extracted_json,
                        fix_applied=repair_result.fix_applied if repair_result else None,
                        fix_history=json.dumps(repair_result.fix_history if repair_result else []),
                        error_label=(repair_result.error_label if repair_result and not repair_result.success else None),
                        generation_id=resp.generation_id,
                        model_used=resp.model_used,
                        provider_used=stats.provider_name if stats else None,
                        cost_usd=stats.cost_usd if stats else None,
                        prompt_tokens=resp.usage.get("prompt_tokens"),
                        completion_tokens=resp.usage.get("completion_tokens"),
                        reasoning_tokens=resp.usage.get("reasoning_tokens"),
                        cached_tokens=resp.usage.get("cached_tokens"),
                        latency_ms=stats.latency_ms if stats else None,
                        created_at=_now_iso(),
                    )
                    db.write(record)

                    if extracted_json:
                        n_success += 1
                        if repair_result and repair_result.success:
                            n_repaired += 1
                    else:
                        n_failed += 1

                    if stats and stats.cost_usd:
                        total_cost += stats.cost_usd
                        if total_cost >= self.config.max_cost_usd:
                            cost_cap_hit = True
                            self.logger.warning(
                                f"Cost cap ${self.config.max_cost_usd:.4f} reached "
                                f"(actual ${total_cost:.4f}); halting extraction"
                            )

            with ProgressReporter(
                "extract",
                total=len(pending_pmids),
                desc=f"Extracting ({self.config.schema_name})",
            ) as progress:
                tasks = [asyncio.create_task(process_one(pmid)) for pmid in pending_pmids]
                for coro in asyncio.as_completed(tasks):
                    await coro
                    progress.advance(1)

        if cost_cap_hit:
            raise CostCapExceededError(self.config.max_cost_usd, total_cost)

        meta.rows_after = meta.rows_before  # extract doesn't modify parquet
        meta.extra.update(
            {
                "schema_name": self.config.schema_name,
                "run_id": run_id,
                "n_success": n_success,
                "n_failed": n_failed,
                "n_repaired": n_repaired,
                "total_cost_usd": round(total_cost, 6),
                "model": self.config.model,
            }
        )
        return meta


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
