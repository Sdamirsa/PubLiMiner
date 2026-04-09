"""Legacy import utility — import AI-in-Med-Trend batch JSON files into PubLiMiner Parquet.

One-time utility to migrate existing pubmed_batch_*.json files from
the S1_output directory into the PubLiMiner papers.parquet format.

Each JSON file has this structure:
{
    "query": "...",
    "batch_id": "0_0",
    "retstart": 0,
    "retmax": 500,
    "total_count": 9400,
    "timestamp": "2025-03-23T18:03:27.801684",
    "data": "<?xml ...>...</xml>"  # Full PubMed XML batch
}

The "data" field contains XML with multiple <PubmedArticle> elements.
We split these into individual rows for the Parquet file.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import polars as pl

from publiminer.core.spine import Spine
from publiminer.constants import DEFAULT_OUTPUT_DIR
from publiminer.utils.progress import ProgressReporter

logger = logging.getLogger("publiminer.legacy_import")


def find_batch_files(source_dir: str | Path) -> list[Path]:
    """Find all pubmed_batch_*.json files in a directory.

    Args:
        source_dir: Directory to search.

    Returns:
        Sorted list of batch file paths.
    """
    source = Path(source_dir)
    if not source.exists():
        logger.warning(f"Source directory does not exist: {source}")
        return []

    files = sorted(
        f for f in source.iterdir()
        if f.name.startswith("pubmed_batch") and f.suffix == ".json"
    )
    logger.info(f"Found {len(files)} batch files in {source}")
    return files


def import_batch_file(file_path: Path) -> list[dict]:
    """Import a single batch JSON file into row dicts.

    Args:
        file_path: Path to a pubmed_batch_*.json file.

    Returns:
        List of row dicts with pmid, raw_xml, fetch_date, fetch_query, fetch_batch.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            batch = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to read {file_path}: {e}")
        return []

    xml_data = batch.get("data", "")
    if not xml_data:
        logger.warning(f"No XML data in {file_path.name}")
        return []

    query = batch.get("query", "")
    batch_id = str(batch.get("batch_id", ""))
    timestamp = batch.get("timestamp", datetime.now().isoformat())

    rows = []
    article_pattern = re.compile(r"(<PubmedArticle>.*?</PubmedArticle>)", re.DOTALL)

    for match in article_pattern.finditer(xml_data):
        article_xml = match.group(1)
        pmid_match = re.search(r"<PMID[^>]*>(\d+)</PMID>", article_xml)
        if not pmid_match:
            continue

        rows.append({
            "pmid": pmid_match.group(1),
            "raw_xml": article_xml,
            "fetch_date": timestamp,
            "fetch_query": query,
            "fetch_batch": batch_id,
        })

    return rows


def import_legacy_data(
    source_dir: str | Path,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    max_files: int | None = None,
) -> dict:
    """Import all legacy batch files into a PubLiMiner Parquet file.

    Args:
        source_dir: Directory containing pubmed_batch_*.json files.
        output_dir: PubLiMiner output directory for papers.parquet.
        max_files: Maximum number of files to process (None = all).

    Returns:
        Dict with import statistics.
    """
    batch_files = find_batch_files(source_dir)
    if max_files is not None:
        batch_files = batch_files[:max_files]

    if not batch_files:
        logger.warning("No batch files found to import")
        return {"files": 0, "articles": 0, "duplicates": 0}

    spine = Spine(output_dir)
    existing_pmids = spine.get_pmids() if spine.exists else set()
    seen_pmids: set[str] = set(existing_pmids)

    all_rows: list[dict] = []
    total_files = 0
    duplicates = 0

    with ProgressReporter("import_legacy", total=len(batch_files),
                          desc="Importing batches") as progress:
        for file_path in batch_files:
            rows = import_batch_file(file_path)
            total_files += 1

            for row in rows:
                pmid = row["pmid"]
                if pmid in seen_pmids:
                    duplicates += 1
                    continue
                seen_pmids.add(pmid)
                all_rows.append(row)

            # Write in batches of 50 files to avoid excessive memory
            if len(all_rows) >= 50000:
                df = pl.DataFrame(all_rows)
                spine.append(df)
                logger.info(f"Written {len(all_rows)} rows ({total_files}/{len(batch_files)} files)")
                all_rows = []
            progress.advance()

    # Write remaining rows
    if all_rows:
        df = pl.DataFrame(all_rows)
        spine.append(df)

    total_articles = spine.count()
    logger.info(
        f"Import complete: {total_files} files, "
        f"{total_articles} total articles, "
        f"{duplicates} duplicates skipped"
    )

    return {
        "files": total_files,
        "articles": total_articles,
        "duplicates": duplicates,
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
    }
