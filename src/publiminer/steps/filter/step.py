"""FilterStep — keyword-based post-parse affiliation filter."""

from __future__ import annotations

import json

import polars as pl

from publiminer.core.base_step import StepBase
from publiminer.core.io import StepMeta
from publiminer.exceptions import StepError
from publiminer.steps.filter.schema import FilterConfig
from publiminer.utils.progress import ProgressReporter


class FilterStep(StepBase):
    """Tag papers whose author affiliations match any configured keyword.

    Reads the `authors` JSON column (written by ParseStep). For each paper,
    checks if at least `min_author_matches` authors have an affiliation string
    that contains at least one keyword. Writes a boolean column `output_column`
    back to parquet.

    If `drop_non_matching` is True, removes non-matching rows entirely instead
    of tagging them.
    """

    name = "filter"

    def __init__(self, global_config, step_config: FilterConfig, output_dir=None) -> None:
        super().__init__(global_config, step_config, output_dir)
        self.config: FilterConfig = step_config

    def validate_input(self) -> None:
        if not self.spine.exists:
            raise StepError(self.name, "papers.parquet not found — run parse first")
        if not self.config.keywords:
            raise StepError(self.name, "filter.keywords must not be empty")

    def run(self) -> StepMeta:
        meta = self.meta
        meta.rows_before = self.spine.count()

        df = self.spine.read()

        keywords = self.config.keywords
        if not self.config.case_sensitive:
            keywords = [k.lower() for k in keywords]

        def _paper_matches(row: dict) -> bool:
            matches = 0

            # Check author affiliations
            authors_raw = row.get("authors")
            if authors_raw:
                try:
                    authors = json.loads(authors_raw)
                    for author in authors:
                        aff = author.get("affiliation", "") or ""
                        text = aff if self.config.case_sensitive else aff.lower()
                        if any(kw in text for kw in keywords):
                            matches += 1
                            if matches >= self.config.min_author_matches:
                                return True
                except Exception:
                    pass

            # Also check any additional plain-text columns
            for col in self.config.also_check_columns:
                val = row.get(col, "") or ""
                text = str(val) if self.config.case_sensitive else str(val).lower()
                if any(kw in text for kw in keywords):
                    return True

            return matches >= self.config.min_author_matches

        rows = df.to_dicts()
        match_flags: list[bool] = []

        with ProgressReporter(
            "filter",
            total=len(rows),
            desc=f"Filtering ({self.config.output_column})",
            update_every=500,
        ) as progress:
            for row in rows:
                match_flags.append(_paper_matches(row))
                progress.advance(1)

        n_match = sum(match_flags)
        n_total = len(match_flags)
        self.logger.info(f"Filter: {n_match:,}/{n_total:,} papers matched keywords")

        if self.config.drop_non_matching:
            df_out = df.filter(pl.Series(match_flags))
            self.spine.write(df_out)
            meta.rows_after = len(df_out)
            meta.rows_removed = n_total - n_match
        else:
            match_series = pl.Series(self.config.output_column, match_flags)
            df_tagged = df.with_columns(match_series)
            self.spine.write(df_tagged)
            meta.rows_after = n_total
            meta.rows_added = 0  # column added, not rows

        meta.extra.update(
            {
                "n_matched": n_match,
                "n_total": n_total,
                "match_rate": round(n_match / max(n_total, 1), 4),
                "output_column": self.config.output_column,
                "keywords_count": len(self.config.keywords),
            }
        )
        return meta

    def validate_output(self) -> None:
        import pyarrow.parquet as pq
        schema_names = pq.ParquetFile(self.spine.parquet_path).schema_arrow.names
        if not self.config.drop_non_matching and self.config.output_column not in schema_names:
            raise StepError(self.name, f"Output column '{self.config.output_column}' not found after filter")
