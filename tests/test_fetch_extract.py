"""Tests for the XML article extractor used by the fetch step."""

from __future__ import annotations

from publiminer.steps.fetch.step import _extract_articles


def test_extract_articles_basic(sample_xml):
    batch = {
        "data": sample_xml,
        "query": "test",
        "batch_id": "0",
        "timestamp": "2025-01-01T00:00:00",
    }
    rows, dup = _extract_articles(batch, existing_pmids=set())
    assert dup == 0
    assert len(rows) == 2
    pmids = {r["pmid"] for r in rows}
    assert pmids == {"11111111", "22222222"}
    assert all("PubmedArticle" in r["raw_xml"] for r in rows)


def test_extract_articles_dedup(sample_xml):
    batch = {"data": sample_xml, "query": "q", "batch_id": "0", "timestamp": "t"}
    existing = {"11111111"}
    rows, dup = _extract_articles(batch, existing_pmids=existing)
    assert dup == 1
    assert len(rows) == 1
    assert rows[0]["pmid"] == "22222222"
    assert "22222222" in existing  # mutated in place
