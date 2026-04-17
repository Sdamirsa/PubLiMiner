"""Deduplicate step — remove duplicate and retracted papers.

Three-layer deduplication:
1. PMID exact match (already unique in Parquet, but handles multi-source imports)
2. DOI exact match (different PMIDs, same DOI)
3. Title fuzzy match (catches near-duplicates with no DOI)
"""

from __future__ import annotations

import logging
from collections import defaultdict

import polars as pl
from thefuzz import fuzz

from publiminer.core.base_step import StepBase
from publiminer.core.config import GlobalConfig
from publiminer.core.io import StepMeta
from publiminer.exceptions import StepError
from publiminer.steps.deduplicate.schema import DeduplicateConfig
from publiminer.utils.progress import ProgressReporter

logger = logging.getLogger("publiminer.deduplicate")


class DeduplicateStep(StepBase):
    """Remove duplicate papers from the Parquet spine.

    Deduplication layers:
    1. PMID: group by PMID, keep first occurrence
    2. DOI: group by DOI, keep first occurrence (by PMID order)
    3. Title fuzzy: pairwise fuzzy matching within same-year groups
    """

    name = "deduplicate"

    def __init__(
        self,
        global_config: GlobalConfig,
        step_config: DeduplicateConfig,
        output_dir: str | None = None,
    ) -> None:
        super().__init__(global_config, step_config, output_dir)
        self.config: DeduplicateConfig = step_config

    def validate_input(self) -> None:
        """Verify parsed data exists."""
        if not self.spine.exists:
            raise StepError(self.name, "Parquet file does not exist. Run fetch and parse first.")

    def run(self) -> StepMeta:
        """Execute the deduplicate step.

        Memory model: each layer reads only the columns it needs (pmid +
        one or two discriminator columns) instead of loading the entire
        schema (including raw_xml, ~90% of file size) into a single
        DataFrame held across all four passes. PMIDs flagged by layers
        2/3/4 are accumulated into one set and removed in a single
        ``remove_rows`` call at the end. Layer 1 (exact PMID duplicates)
        cannot use this pattern — ``remove_rows`` drops ALL copies of a
        pmid, not just duplicates — so it falls back to a one-off full
        rewrite in the rare case duplicates are found.
        """
        meta = StepMeta(step_name=self.name)
        meta.start()
        meta.config_snapshot = self.config.model_dump()

        meta.rows_before = self.spine.count()
        removed: list[dict[str, str]] = []
        pmids_to_remove: set[str] = set()
        pmid_dupes = 0

        # Layer 1: PMID exact duplicates.
        # Normal pipeline already dedupes by PMID (fetch's existing_pmids
        # set, import-legacy's upsert). This is a sanity check; in practice
        # it finds zero. When it does find duplicates we cannot express
        # "keep first, drop rest" via remove_rows(pmids) (which removes ALL
        # copies), so we fall back to a full read + unique() rewrite.
        with ProgressReporter("dedup_pmid", total=1, desc="Layer 1/4: PMID exact") as p:
            pmid_df = self.spine.read(columns=["pmid"])
            before = len(pmid_df)
            pmid_dupes = before - pmid_df.unique(subset=["pmid"]).height
            p.advance()
        del pmid_df
        if pmid_dupes > 0:
            self.logger.warning(
                f"Layer 1: {pmid_dupes} PMID duplicates detected — collapsing via "
                f"full rewrite (unusual; fetch and import-legacy should prevent this)"
            )
            full_df = pl.read_parquet(self.spine.parquet_path, memory_map=False)
            full_df = full_df.unique(subset=["pmid"], keep="first")
            self.spine.write(full_df)
            del full_df
            removed.extend(
                [{"pmid": "unknown", "reason": "pmid_duplicate"} for _ in range(pmid_dupes)]
            )

        # Layer 2: DOI exact duplicates (projected read: pmid + doi).
        if self.config.check_doi:
            with ProgressReporter("dedup_doi", total=1, desc="Layer 2/4: DOI exact") as p:
                doi_df = self.spine.read(columns=["pmid", "doi"])
                to_remove = _find_doi_duplicates(doi_df) if "doi" in doi_df.columns else []
                p.advance()
            del doi_df
            if to_remove:
                pmids_to_remove.update(to_remove)
                self.logger.info(f"Layer 2: flagged {len(to_remove)} DOI duplicates")
                removed.extend([{"pmid": pmid, "reason": "doi_duplicate"} for pmid in to_remove])

        # Layer 3: Title fuzzy match (projected read: pmid + title + year).
        if self.config.check_title_fuzzy:
            title_df = self.spine.read(columns=["pmid", "title", "year"])
            if "title" in title_df.columns:
                # 3a: cheap exact-title collapse — equivalent to the original
                # `unique(subset=["_title_norm"], keep="first")` but returns the
                # set of PMIDs dropped (for logging + remove_rows) instead of
                # just the count.
                norm_df = title_df.with_columns(
                    pl.col("title").str.to_lowercase().str.strip_chars().alias("_title_norm")
                )
                valid = norm_df.filter(
                    pl.col("_title_norm").is_not_null() & (pl.col("_title_norm") != "")
                )
                kept_pmids = set(
                    valid.unique(subset=["_title_norm"], keep="first")["pmid"].to_list()
                )
                all_valid_pmids = set(valid["pmid"].to_list())
                exact_dupes = list(all_valid_pmids - kept_pmids)
                del norm_df, valid
                if exact_dupes:
                    pmids_to_remove.update(exact_dupes)
                    self.logger.info(f"Layer 3a: flagged {len(exact_dupes)} exact-title duplicates")
                    removed.extend(
                        [{"pmid": pmid, "reason": "exact_title_duplicate"} for pmid in exact_dupes]
                    )

                # 3b: blocked fuzzy match — exclude already-flagged PMIDs so we
                # don't waste comparisons on rows destined for removal.
                remaining = title_df.filter(~pl.col("pmid").is_in(list(pmids_to_remove)))
                fuzzy_dupes = _find_fuzzy_title_duplicates(
                    remaining,
                    threshold=self.config.fuzzy_threshold,
                    logger=self.logger,
                )
                if fuzzy_dupes:
                    pmids_to_remove.update(fuzzy_dupes)
                    self.logger.info(f"Layer 3b: flagged {len(fuzzy_dupes)} fuzzy title duplicates")
                    removed.extend(
                        [{"pmid": pmid, "reason": "fuzzy_title_duplicate"} for pmid in fuzzy_dupes]
                    )
                del remaining
            del title_df

        # Layer 4: Retracted papers (projected read: pmid + publication_status).
        if self.config.remove_retracted:
            ret_df = self.spine.read(columns=["pmid", "publication_status"])
            if "publication_status" in ret_df.columns:
                with ProgressReporter("dedup_retracted", total=1, desc="Layer 4/4: Retracted") as p:
                    retracted_mask = (
                        pl.col("publication_status")
                        .str.to_lowercase()
                        .str.contains(r"\bretracted?\b")
                    )
                    retracted = ret_df.filter(retracted_mask)
                    p.advance()
                if len(retracted) > 0:
                    retracted_pmids = retracted["pmid"].to_list()
                    pmids_to_remove.update(retracted_pmids)
                    self.logger.info(f"Layer 4: flagged {len(retracted_pmids)} retracted papers")
                    removed.extend(
                        [{"pmid": pmid, "reason": "retracted"} for pmid in retracted_pmids]
                    )
            del ret_df

        # CRITICAL: do NOT replace this with `self.spine.write(df)`. The dedup
        # layers above read a projected subset of columns; writing that subset
        # would silently DELETE every unprojected column from the spine
        # (raw_xml, abstract, mesh_terms, etc.). `remove_rows` re-reads the
        # full parquet, filters by pmid, writes back — preserving all columns
        # by construction.
        if pmids_to_remove:
            self.spine.remove_rows(list(pmids_to_remove))

        rows_after = self.spine.count()
        total_removed = meta.rows_before - rows_after
        meta.rows_removed = total_removed
        meta.extra["removed_details"] = removed
        meta.extra["pmid_duplicates"] = pmid_dupes
        meta.extra["doi_duplicates"] = len([r for r in removed if r["reason"] == "doi_duplicate"])
        meta.extra["fuzzy_duplicates"] = len(
            [r for r in removed if r["reason"] == "fuzzy_title_duplicate"]
        )
        meta.extra["retracted"] = len([r for r in removed if r["reason"] == "retracted"])

        self.logger.info(
            f"Deduplication complete: {meta.rows_before} -> {rows_after} rows "
            f"({total_removed} removed)"
        )
        return meta


def _find_doi_duplicates(df: pl.DataFrame) -> list[str]:
    """Find PMIDs to remove based on DOI duplicates.

    For each DOI with multiple PMIDs, keeps the first PMID and marks the rest.

    Args:
        df: DataFrame with pmid and doi columns.

    Returns:
        List of PMIDs to remove.
    """
    to_remove: list[str] = []

    # Only consider rows with non-empty DOI
    has_doi = df.filter(pl.col("doi").is_not_null() & (pl.col("doi") != ""))
    if len(has_doi) == 0:
        return to_remove

    # Group by DOI, find duplicates
    doi_groups = has_doi.group_by("doi").agg(pl.col("pmid"))
    for row in doi_groups.iter_rows(named=True):
        pmids = row["pmid"]
        if len(pmids) > 1:
            # Keep first, remove rest
            to_remove.extend(pmids[1:])

    return to_remove


def _title_block_key(title: str) -> str:
    """Block key = first 3 words of normalized title.

    Two titles can only be near-duplicates if they share their first words.
    This drops the comparison count from O(n²) per year to O(k²) per small block.
    """
    words = title.split()
    return " ".join(words[:3]) if len(words) >= 3 else title


def _find_fuzzy_title_duplicates(
    df: pl.DataFrame,
    threshold: int = 90,
    logger: logging.Logger | None = None,
) -> list[str]:
    """Find PMIDs to remove based on fuzzy title matching.

    Optimization strategy:
    1. Group by (year, first 3 words of title) → small blocks
    2. Within each block, do pairwise fuzz.ratio
    3. Skip blocks of size 1
    4. Length pre-filter: skip pairs whose length ratio alone disqualifies them

    Args:
        df: DataFrame with pmid, title, and optionally year columns.
        threshold: Fuzzy match threshold (0-100).
        logger: Optional logger for stats.

    Returns:
        List of PMIDs to remove.
    """
    to_remove: set[str] = set()

    has_year = "year" in df.columns
    cols = ["pmid", "title"] + (["year"] if has_year else [])

    # Build (year, block_key) → list of (pmid, normalized_title)
    blocks: dict[tuple, list[tuple[str, str]]] = defaultdict(list)
    for row in df.select(cols).iter_rows(named=True):
        title = row.get("title") or ""
        if not title:
            continue
        norm = title.lower().strip()
        year = row.get("year") if has_year else None
        key = (year, _title_block_key(norm))
        blocks[key].append((row["pmid"], norm))

    # Filter to blocks worth comparing
    work_blocks = [b for b in blocks.values() if len(b) >= 2]
    total_pairs = sum(len(b) * (len(b) - 1) // 2 for b in work_blocks)

    if logger:
        logger.info(
            f"Layer 3b: {len(work_blocks):,} title blocks, "
            f"{total_pairs:,} candidate pairs (vs naive O(n²))"
        )

    if total_pairs == 0:
        return []

    # Length ratio bound: if shorter/longer < threshold/100, fuzz.ratio can't reach threshold
    min_len_ratio = threshold / 100.0
    update_every = max(1, total_pairs // 200)
    pair_idx = 0

    with ProgressReporter(
        "dedup_fuzzy",
        total=total_pairs,
        desc="Layer 3/4: Fuzzy title",
        update_every=update_every,
    ) as progress:
        for papers in work_blocks:
            n = len(papers)
            for i in range(n):
                if papers[i][0] in to_remove:
                    pair_idx += n - i - 1
                    progress.advance(n - i - 1)
                    continue
                len_i = len(papers[i][1])
                for j in range(i + 1, n):
                    pair_idx += 1
                    progress.advance()
                    if papers[j][0] in to_remove:
                        continue
                    len_j = len(papers[j][1])
                    # Length pre-filter — bound on fuzz.ratio
                    if min(len_i, len_j) / max(len_i, len_j, 1) < min_len_ratio:
                        continue
                    score = fuzz.ratio(papers[i][1], papers[j][1])
                    if score >= threshold:
                        to_remove.add(papers[j][0])

    return list(to_remove)
