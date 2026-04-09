"""Parse step — extract structured data from raw XML in Parquet.

Reads raw_xml column from Parquet, parses each article's XML,
and writes structured columns back.
"""

from __future__ import annotations

import json

import polars as pl

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
        """Execute the parse step."""
        meta = StepMeta(step_name=self.name)
        meta.start()
        meta.rows_before = self.spine.count()
        meta.config_snapshot = self.config.model_dump()

        # Incremental: only parse rows that don't yet have a title.
        # On a fresh parquet (post-fetch) every row has raw_xml but no title;
        # on subsequent runs only newly fetched rows are picked up.
        # Probe schema (1 row) to know whether `title` column exists yet
        full_schema = pl.read_parquet(
            self.spine.parquet_path, memory_map=False, n_rows=1,
        ).columns
        if "title" in full_schema:
            df = self.spine.read(columns=["pmid", "raw_xml", "title"])
            df = df.filter(pl.col("title").is_null() | (pl.col("title") == ""))
            df = df.select(["pmid", "raw_xml"])
            self.logger.info(f"Incremental parse: {len(df):,} unparsed rows")
        else:
            df = self.spine.read(columns=["pmid", "raw_xml"])

        if len(df) == 0:
            self.logger.info("Nothing to parse — all rows already have titles.")
            meta.extra["parsed"] = 0
            meta.extra["parse_errors"] = 0
            return meta

        parsed_rows = []
        errors = 0

        total = len(df)
        update_every = max(1, total // 200)  # ~200 progress events max
        with ProgressReporter("parse", total=total, desc="Parsing XML",
                              update_every=update_every) as progress:
            for row in df.iter_rows(named=True):
                pmid = row["pmid"]
                raw_xml = row.get("raw_xml", "")

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

        if parsed_rows:
            new_df = pl.DataFrame(parsed_rows)
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
