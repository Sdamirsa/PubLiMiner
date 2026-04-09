"""Fetch step — retrieve papers from PubMed and stream raw XML to a staging file.

Memory model
------------
The fetch step never holds more than one in-flight batch (~5 MB) plus a small
buffer of extracted article rows (~50 MB) before flushing to a staging parquet.
At the end of the run the staging file is stream-merged into the main parquet
in row groups via pyarrow, so peak memory stays bounded regardless of the
total fetch size (10K papers or 1M papers — same memory footprint).

Resume model
------------
- The staging parquet on disk is the resume checkpoint. If the process is
  killed mid-fetch, every batch already flushed survives.
- On the next run, the staging file is merged first, then PMIDs from BOTH
  the main parquet and the staging file are loaded into the dedup set, so
  no batch is ever re-extracted twice.
- `start_date: "auto"` resolves to (max existing fetch_date − 7 days),
  letting nightly cron runs pick up where the previous one left off without
  any user intervention.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

import polars as pl

from publiminer.core.base_step import StepBase
from publiminer.core.cache import ResponseCache
from publiminer.core.config import GlobalConfig
from publiminer.core.io import StepMeta
from publiminer.exceptions import StepError
from publiminer.steps.fetch.pubmed_client import PubMedClient
from publiminer.steps.fetch.schema import FetchConfig
from publiminer.utils.env import get_env
from publiminer.utils.progress import ProgressReporter

# PubMed WebEnv esearch+efetch hard limit (retstart + retmax must stay under this)
PUBMED_WEBENV_LIMIT = 9999

# Flush every N extracted articles to the staging parquet.
FLUSH_EVERY_ARTICLES = 5000


class FetchStep(StepBase):
    """Fetch papers from PubMed API with streaming, chunked, resumable I/O."""

    name = "fetch"

    def __init__(
        self,
        global_config: GlobalConfig,
        step_config: FetchConfig,
        output_dir: str | None = None,
    ) -> None:
        super().__init__(global_config, step_config, output_dir)
        self.config: FetchConfig = step_config

    def validate_input(self) -> None:
        if not self.config.query:
            raise StepError(self.name, "No query specified in fetch config")
        email = self.config.email or get_env("PUBMED_EMAIL", "")
        if not email:
            raise StepError(self.name, "Email required: set fetch.email or PUBMED_EMAIL env var")

    def _resolve_start_date(self) -> str:
        """Resolve `start_date: 'auto'` to (last fetch − 7 days), or a sane default."""
        sd = self.config.start_date
        if sd and sd.lower() != "auto":
            return sd
        if not self.spine.exists:
            return "2000/01/01"
        try:
            df = self.spine.read(columns=["fetch_date"])
            if len(df) == 0:
                return "2000/01/01"
            latest_str = df["fetch_date"].max()
            latest = datetime.fromisoformat(str(latest_str).split(".")[0])
            resume = latest - timedelta(days=7)
            resolved = resume.strftime("%Y/%m/%d")
            self.logger.info(f"Auto start_date resolved to {resolved} (latest fetch: {latest_str})")
            return resolved
        except Exception as e:
            self.logger.warning(f"Auto start_date resolution failed ({e}); using 2000/01/01")
            return "2000/01/01"

    def run(self) -> StepMeta:
        meta = StepMeta(step_name=self.name)
        meta.start()
        meta.rows_before = self.spine.count() if self.spine.exists else 0
        meta.config_snapshot = self.config.model_dump()

        # Step 0: merge any leftover staging from a previous crashed run
        if self.spine.staging_exists:
            merged = self.spine.merge_staging()
            self.logger.info(f"Resumed: merged {merged:,} rows from prior staging file")

        email = self.config.email or get_env("PUBMED_EMAIL", "")
        api_key = self.config.api_key or get_env("NCBI_API_KEY", "")

        # NCBI raises rate limit to 10/sec when an api_key is supplied.
        rate = self.config.rate_limit_per_second
        if api_key and rate < 10.0:
            rate = 10.0

        cache = ResponseCache(self.output_dir / "cache", self.global_config.cache.ttl_days)
        client = PubMedClient(email=email, api_key=api_key, rate_limit=rate, cache=cache)

        # Existing PMIDs (from main + staging) — used to skip articles we already have
        existing_pmids = self.spine.get_pmids() | self.spine.get_staging_pmids()
        self.logger.info(f"Skipping {len(existing_pmids):,} already-fetched PMIDs")

        buffer: list[dict] = []
        added = 0
        skipped = 0
        batches = 0
        start_date = self._resolve_start_date()
        end_date = self.config.end_date or datetime.now().strftime("%Y/%m/%d")

        try:
            if start_date and end_date:
                # Phase 1: plan (count every month, build optimized queries).
                # Doing this BEFORE opening the progress bar lets us show the
                # real total instead of a "0/1" placeholder.
                optimized, expected_total = client.plan_date_batched(
                    base_query=self.config.query,
                    start_date=start_date,
                    end_date=end_date,
                )
                stream = client.iter_planned(
                    optimized=optimized,
                    batch_size=self.config.batch_size,
                    download_mode=self.config.download_mode,
                    ret_mode=self.config.ret_mode,
                    ret_type=self.config.ret_type,
                )
            else:
                stream = self._simple_stream(client)
                expected_total = 0
                if self.config.max_results and self.config.max_results > 0:
                    expected_total = self.config.max_results

            batch_errors = 0
            with ProgressReporter(
                "fetch", total=expected_total, desc="Fetching PubMed"
            ) as progress:
                # Iterate the generator in a try/except loop so a single
                # transient failure doesn't kill the whole fetch — we log and
                # continue to the next batch (staging is already on disk).
                it = iter(stream)
                while True:
                    try:
                        batch = next(it)
                    except StopIteration:
                        break
                    except Exception as e:
                        batch_errors += 1
                        self.logger.error(
                            f"Batch failed ({batch_errors} total): {type(e).__name__}: {e}"
                        )
                        if batch_errors >= 20:
                            self.logger.error("Too many batch errors — aborting fetch")
                            raise
                        continue
                    batches += 1
                    new_rows, dup = _extract_articles(batch, existing_pmids)
                    buffer.extend(new_rows)
                    added += len(new_rows)
                    skipped += dup
                    progress.advance(len(new_rows) + dup)
                    if len(buffer) >= FLUSH_EVERY_ARTICLES:
                        self._flush(buffer)
                        buffer.clear()

            if buffer:
                self._flush(buffer)
                buffer.clear()

            # Final merge: stream staging into main parquet
            if self.spine.staging_exists:
                merged = self.spine.merge_staging()
                self.logger.info(f"Merged {merged:,} new rows into main parquet")

            meta.rows_added = added
            meta.extra["batches"] = batches
            meta.extra["articles_added"] = added
            meta.extra["articles_skipped_duplicate"] = skipped
            meta.extra["start_date"] = start_date
            meta.extra["end_date"] = end_date
            self.logger.info(
                f"Fetch complete: {added:,} new, {skipped:,} duplicates skipped, "
                f"{batches} batches"
            )
        finally:
            client.close()
            cache.close()

        return meta

    def _simple_stream(self, client: PubMedClient):
        """Single-shot fetch without date partitioning (for ≤9999 results)."""
        web_env, query_key, total = client.search(self.config.query)
        target = total
        if self.config.max_results and self.config.max_results > 0:
            target = min(target, self.config.max_results)
        if target > PUBMED_WEBENV_LIMIT:
            raise StepError(
                self.name,
                f"Query returned {total:,} results and target is {target:,}, "
                f"but PubMed WebEnv supports only {PUBMED_WEBENV_LIMIT:,} per session. "
                f"Set fetch.max_results <= {PUBMED_WEBENV_LIMIT} or supply "
                f"fetch.start_date / fetch.end_date for date partitioning.",
            )
        batch_size = self.config.batch_size
        num_batches = (target + batch_size - 1) // batch_size
        for i in range(num_batches):
            retstart = i * batch_size
            retmax = min(batch_size, target - retstart)
            data = client.fetch_batch(
                web_env, query_key, retstart, retmax,
                self.config.download_mode, self.config.ret_mode, self.config.ret_type,
            )
            yield {
                "query": self.config.query,
                "batch_id": str(i),
                "retstart": retstart,
                "retmax": retmax,
                "total_count": total,
                "timestamp": datetime.now().isoformat(),
                "data": data,
            }

    def _flush(self, rows: list[dict]) -> None:
        df = pl.DataFrame(rows)
        self.spine.append_staging(df)
        self.logger.info(f"Flushed {len(rows):,} articles to staging")


def _extract_articles(
    batch: dict, existing_pmids: set[str]
) -> tuple[list[dict], int]:
    """Extract per-article rows from one batch's XML; mutate existing_pmids in-place.

    Returns (new_rows, duplicates_skipped).
    """
    rows: list[dict] = []
    dup = 0
    xml_data = batch.get("data", "")
    query = batch.get("query", "")
    batch_id = batch.get("batch_id", "")
    timestamp = batch.get("timestamp", datetime.now().isoformat())

    article_pattern = re.compile(r"(<PubmedArticle>.*?</PubmedArticle>)", re.DOTALL)
    for match in article_pattern.finditer(xml_data):
        article_xml = match.group(1)
        pmid_match = re.search(r"<PMID[^>]*>(\d+)</PMID>", article_xml)
        if not pmid_match:
            continue
        pmid = pmid_match.group(1)
        if pmid in existing_pmids:
            dup += 1
            continue
        existing_pmids.add(pmid)
        rows.append({
            "pmid": pmid,
            "raw_xml": article_xml,
            "fetch_date": timestamp,
            "fetch_query": query,
            "fetch_batch": str(batch_id),
        })
    return rows, dup
