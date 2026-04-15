"""Tests for the Parquet spine."""

from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq

from publiminer import Spine
from publiminer.core.spine import PARQUET_COMPRESSION, PARQUET_ROW_GROUP_SIZE


def test_spine_append_merge_count(tmp_output):
    spine = Spine(tmp_output)
    assert not spine.exists

    df = pl.DataFrame(
        {
            "pmid": ["1", "2"],
            "raw_xml": ["<x/>", "<y/>"],
            "fetch_date": ["2025-01-01", "2025-01-01"],
            "fetch_query": ["q", "q"],
            "fetch_batch": ["0", "0"],
        }
    )
    spine.append_staging(df)
    assert spine.staging_exists

    merged = spine.merge_staging()
    assert merged == 2
    assert spine.exists
    assert spine.count() == 2
    assert spine.get_pmids() == {"1", "2"}


def test_spine_write_preserves_zstd(tmp_output):
    """Every writer in Spine must emit zstd — the pyarrow ParquetWriter
    default is snappy, which silently downgrades the main parquet on every
    merge_staging cycle. Pinning compression at the writer level is the
    regression prevention for that bug."""
    spine = Spine(tmp_output)
    df = pl.DataFrame({"pmid": [str(i) for i in range(1000)], "data": ["x"] * 1000})
    spine.append(df)

    pf = pq.ParquetFile(spine.parquet_path)
    compression = pf.metadata.row_group(0).column(0).compression
    assert compression.upper() == PARQUET_COMPRESSION.upper(), (
        f"Expected {PARQUET_COMPRESSION.upper()} compression, got {compression}"
    )


def test_spine_write_honors_row_group_size(tmp_output):
    """Writing enough rows to span multiple groups must produce ≥ 3 row
    groups at PARQUET_ROW_GROUP_SIZE. This is the prerequisite for
    iter_batches to actually stream — a single giant row group forces
    pyarrow to materialise it whole before any slicing happens."""
    spine = Spine(tmp_output)
    n_rows = PARQUET_ROW_GROUP_SIZE * 3
    df = pl.DataFrame({"pmid": [str(i) for i in range(n_rows)]})
    spine.append(df)

    pf = pq.ParquetFile(spine.parquet_path)
    assert pf.metadata.num_row_groups >= 3, (
        f"Expected ≥ 3 row groups for {n_rows} rows, got {pf.metadata.num_row_groups}"
    )


def test_spine_iter_batches_roundtrip(tmp_output):
    """iter_batches must yield every row exactly once. With batch_size
    smaller than PARQUET_ROW_GROUP_SIZE, multiple batches per row group
    confirm the streaming slice actually kicks in."""
    spine = Spine(tmp_output)
    n_rows = 100_000
    df = pl.DataFrame({"pmid": [str(i) for i in range(n_rows)]})
    spine.append(df)

    batches = list(spine.iter_batches(columns=["pmid"], batch_size=10_000))
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == n_rows
    # row_group_size=50K, batch_size=10K → 5 batches per row group × 2 groups
    assert len(batches) >= 10, f"Expected ≥ 10 batches, got {len(batches)}"
