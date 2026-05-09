"""Tests for core/extraction_db.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from publiminer.core.extraction_db import ExtractionDB, ExtractionRecord


@pytest.fixture()
def db(tmp_path: Path) -> ExtractionDB:
    return ExtractionDB(tmp_path / "test.db")


def _record(pmid: str = "12345", schema: str = "test_schema", run: str = "run1", success: bool = True) -> ExtractionRecord:
    return ExtractionRecord(
        pmid=pmid,
        schema_name=schema,
        run_id=run,
        raw_response='{"x": 1}',
        extracted_json='{"x": 1}' if success else None,
        error_label=None if success else "parse_failed",
        created_at="2026-05-05T12:00:00Z",
    )


class TestWrite:
    def test_write_and_exists(self, db: ExtractionDB) -> None:
        db.write(_record("111"))
        assert db.exists("111", "test_schema", "run1")

    def test_exists_returns_false_for_unknown(self, db: ExtractionDB) -> None:
        assert not db.exists("999", "test_schema", "run1")

    def test_failed_record_not_counted_as_success(self, db: ExtractionDB) -> None:
        db.write(_record("222", success=False))
        assert not db.exists("222", "test_schema", "run1")

    def test_upsert_replaces_failed_with_success(self, db: ExtractionDB) -> None:
        db.write(_record("333", success=False))
        assert not db.exists("333", "test_schema", "run1")
        db.write(_record("333", success=True))
        assert db.exists("333", "test_schema", "run1")


class TestGetPendingPmids:
    def test_returns_all_when_empty(self, db: ExtractionDB) -> None:
        pmids = ["a", "b", "c"]
        pending = db.get_pending_pmids(pmids, "test_schema", "run1")
        assert pending == ["a", "b", "c"]

    def test_excludes_done(self, db: ExtractionDB) -> None:
        db.write(_record("a"))
        pending = db.get_pending_pmids(["a", "b", "c"], "test_schema", "run1")
        assert "a" not in pending
        assert set(pending) == {"b", "c"}

    def test_failed_still_pending(self, db: ExtractionDB) -> None:
        db.write(_record("a", success=False))
        pending = db.get_pending_pmids(["a", "b"], "test_schema", "run1")
        assert "a" in pending

    def test_empty_input(self, db: ExtractionDB) -> None:
        assert db.get_pending_pmids([], "test_schema", "run1") == []


class TestSummary:
    def test_summary_counts(self, db: ExtractionDB) -> None:
        db.write(_record("1", success=True))
        db.write(_record("2", success=False))
        s = db.get_summary("test_schema", "run1")
        assert s["n_success"] == 1
        assert s["n_failed"] == 1
        assert s["total"] == 2

    def test_summary_repaired(self, db: ExtractionDB) -> None:
        rec = _record("3", success=True)
        rec.fix_applied = "pattern"
        db.write(rec)
        s = db.get_summary("test_schema", "run1")
        assert s["n_repaired"] == 1


class TestListMethods:
    def test_list_runs(self, db: ExtractionDB) -> None:
        db.write(_record(schema="s1", run="r1"))
        db.write(_record(pmid="2", schema="s1", run="r2"))
        runs = db.list_runs("s1")
        assert set(runs) == {"r1", "r2"}

    def test_list_schemas(self, db: ExtractionDB) -> None:
        db.write(_record(schema="alpha"))
        db.write(_record(pmid="2", schema="beta"))
        schemas = db.list_schemas()
        assert "alpha" in schemas
        assert "beta" in schemas


class TestExportJsonl:
    def test_export_writes_jsonl(self, db: ExtractionDB, tmp_path: Path) -> None:
        db.write(_record("p1"))
        db.write(_record("p2"))
        out = tmp_path / "out.jsonl"
        count = db.export_jsonl("test_schema", "run1", out)
        assert count == 2
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            obj = json.loads(line)
            assert "pmid" in obj
            assert "extracted" in obj

    def test_failed_rows_not_exported(self, db: ExtractionDB, tmp_path: Path) -> None:
        db.write(_record("p1", success=True))
        db.write(_record("p2", success=False))
        out = tmp_path / "out.jsonl"
        count = db.export_jsonl("test_schema", "run1", out)
        assert count == 1
