"""Parse step — extract structured data from raw XML in Parquet.

Streaming model
---------------
The parquet is read in row-group-sized batches (via ``Spine.iter_batches``)
instead of loading every row eagerly. Two streams: a cheap ``(pmid, title)``
pass builds the "already parsed" skip set, then a ``(pmid, raw_xml)`` pass
extracts structured fields row-by-row. Each batch is released before the
next is read, so peak memory during the parse loop stays bounded (~500 MB
per batch + ~1 GB accumulator for a full 500K-row parse, vs ~4 GB for the
old eager read).

Note: the final ``spine.add_columns`` write still reads the full parquet
once; that step's memory cost is out of scope for the streaming-parse
change and is tracked as a follow-up (raw_xml sidecar).
"""

from __future__ import annotations

import gc
import json

import polars as pl
import pyarrow.parquet as pq

from publiminer.core.base_step import StepBase
from publiminer.core.config import GlobalConfig
from publiminer.core.io import StepMeta
from publiminer.exceptions import StepError
from publiminer.steps.parse.schema import ParseConfig
from publiminer.steps.parse.xml_parser import (
    compute_exclusion_flags,
    parse_article_xml,
    prepare_llm_input,
)
from publiminer.utils.progress import ProgressReporter


class ParseStep(StepBase):
    """Parse raw PubMed XML into structured columns.

    Reads: pmid, raw_xml
    Writes: title, abstract, authors, journal, year, doi, pub_type,
            mesh_terms, keywords, language, grants, publication_status,
            article_ids, llm_input, exclude_flag, exclude_reason
    """

    name = "parse"

    def __init__(
        self,
        global_config: GlobalConfig,
        step_config: ParseConfig,
        output_dir: str | None = None,
    ) -> None:
        super().__init__(global_config, step_config, output_dir)
        self.config: ParseConfig = step_config

    def validate_input(self) -> None:
        """Verify that raw_xml column exists in Parquet."""
        if not self.spine.exists:
            raise StepError(self.name, "Parquet file does not exist. Run fetch first.")
        df = self.spine.read(columns=["pmid"])
        if len(df) == 0:
            raise StepError(self.name, "No articles in Parquet. Run fetch first.")

    def run(self) -> StepMeta:
        """Execute the parse step (streaming)."""
        meta = StepMeta(step_name=self.name)
        meta.start()
        meta.rows_before = self.spine.count()
        meta.config_snapshot = self.config.model_dump()

        # Probe the parquet schema via the footer (cheap — no row data loaded).
        schema_names = pq.ParquetFile(self.spine.parquet_path).schema_arrow.names

        # Build the "already parsed" skip-set by streaming (pmid, title) in
        # 100K-row batches. Keeps peak memory for this phase under ~60 MB
        # on a 500K-row parquet. On a fresh parquet (no title column yet)
        # the set stays empty and every row will be parsed.
        parsed_pmids: set[str] = set()
        if "title" in schema_names:
            for batch in self.spine.iter_batches(
                columns=["pmid", "title"], batch_size=100_000,
            ):
                pmids = batch.column("pmid").to_pylist()
                titles = batch.column("title").to_pylist()
                for pmid, title in zip(pmids, titles, strict=True):
                    if title:  # non-None and non-empty
                        parsed_pmids.add(pmid)
                del pmids, titles, batch

        to_parse = meta.rows_before - len(parsed_pmids)
        if to_parse == 0:
            self.logger.info("Nothing to parse — all rows already have titles.")
            meta.extra["parsed"] = 0
            meta.extra["parse_errors"] = 0
            return meta

        if "title" in schema_names:
            self.logger.info(
                f"Incremental parse: {len(parsed_pmids):,} already parsed, "
                f"{to_parse:,} to parse"
            )
        else:
            self.logger.info(f"Fresh parquet: parsing all {to_parse:,} rows")

        parsed_rows: list[dict] = []
        errors = 0
        update_every = max(1, to_parse // 200)  # ~200 progress events max

        # Stream (pmid, raw_xml) in 5K-row batches — each batch decodes to
        # ~500 MB peak before release. Actual batch size is capped by the
        # parquet row-group size, so this assumes papers.parquet was
        # written/migrated with PARQUET_ROW_GROUP_SIZE.
        with ProgressReporter(
            "parse", total=to_parse, desc="Parsing XML", update_every=update_every,
        ) as progress:
            for batch in self.spine.iter_batches(
                columns=["pmid", "raw_xml"], batch_size=5_000,
            ):
                pmids = batch.column("pmid").to_pylist()
                raw_xmls = batch.column("raw_xml").to_pylist()
                for pmid, raw_xml in zip(pmids, raw_xmls, strict=True):
                    if pmid in parsed_pmids:
                        # Already-parsed rows aren't counted in `to_parse`,
                        # so don't advance the progress bar for them.
                        continue
                    raw_xml = raw_xml or ""
                    if not raw_xml:
                        errors += 1
                        progress.advance()
                        continue
                    try:
                        article = parse_article_xml(raw_xml)
                        if not article:
                            errors += 1
                            progress.advance()
                            continue
                        parsed = _article_to_flat_row(article, pmid, self.config)
                        parsed_rows.append(parsed)
                    except Exception as e:
                        self.logger.warning(f"Failed to parse PMID {pmid}: {e}")
                        errors += 1
                    progress.advance()
                del pmids, raw_xmls, batch

        if parsed_rows:
            new_df = pl.DataFrame(parsed_rows)
            # Mandatory on Windows: any pyarrow ParquetFile handle held by
            # the iter_batches generator must be released before
            # spine.add_columns → spine.write → os.replace runs (or the
            # rename will fail with Access Denied).
            gc.collect()
            self.spine.add_columns(new_df, on="pmid")
            self.logger.info(f"Parsed {len(parsed_rows)} articles ({errors} errors)")
        else:
            self.logger.warning("No articles parsed successfully")

        meta.errors = errors
        meta.extra["parsed"] = len(parsed_rows)
        meta.extra["parse_errors"] = errors

        return meta


def _article_to_flat_row(
    article: dict, pmid: str, config: ParseConfig
) -> dict:
    """Convert a parsed article dict to a flat row for Parquet.

    Complex fields (authors, journal, mesh, keywords, etc.) are stored
    as JSON strings in the Parquet file.

    Args:
        article: Parsed article dict from xml_parser.
        pmid: Article PMID.
        config: Parse step configuration.

    Returns:
        Flat dict ready for Parquet row.
    """
    # Extract year from publication_date
    pub_date = article.get("publication_date", {})
    year = pub_date.get("year")
    if isinstance(year, str):
        try:
            year = int(year)
        except ValueError:
            year = None

    # Build row
    row: dict = {
        "pmid": pmid,
        "title": article.get("title", ""),
        "abstract": article.get("abstract", ""),
        "authors": json.dumps(article.get("authors", []), ensure_ascii=False),
        "journal": json.dumps(article.get("journal", {}), ensure_ascii=False),
        "year": year,
        "doi": article.get("doi", ""),
        "pub_type": json.dumps(article.get("publication_types", []), ensure_ascii=False),
        "mesh_terms": json.dumps(article.get("mesh_headings", []), ensure_ascii=False),
        "keywords": json.dumps(article.get("keywords", []), ensure_ascii=False),
        "language": article.get("language", ""),
        "grants": json.dumps(article.get("grants", []), ensure_ascii=False),
        "publication_status": article.get("publication_status", ""),
        "article_ids": json.dumps(article.get("article_ids", []), ensure_ascii=False),
    }

    # LLM input preparation
    if config.prepare_llm_input:
        row["llm_input"] = prepare_llm_input(article)

    # Exclusion flags
    if config.flag_exclusions:
        exclude, reason = compute_exclusion_flags(article)
        row["exclude_flag"] = exclude
        row["exclude_reason"] = reason

    return row
