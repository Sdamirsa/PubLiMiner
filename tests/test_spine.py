"""Tests for the Parquet spine."""

from __future__ import annotations

import polars as pl

from publiminer import Spine


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
