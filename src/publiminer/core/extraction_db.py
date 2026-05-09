"""SQLite handler for per-paper LLM extraction results."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS extractions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    extracted_json TEXT,
    raw_response TEXT NOT NULL,
    fix_applied TEXT,
    fix_history TEXT,
    error_label TEXT,
    generation_id TEXT,
    model_used TEXT,
    provider_used TEXT,
    cost_usd REAL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    reasoning_tokens INTEGER,
    cached_tokens INTEGER,
    latency_ms INTEGER,
    created_at TEXT,
    UNIQUE(pmid, schema_name, run_id)
);
"""


@dataclass
class ExtractionRecord:
    """One row in the extractions table."""

    pmid: str
    schema_name: str
    run_id: str
    raw_response: str
    extracted_json: str | None = None
    fix_applied: str | None = None
    fix_history: str = "[]"
    error_label: str | None = None
    generation_id: str | None = None
    model_used: str | None = None
    provider_used: str | None = None
    cost_usd: float | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    reasoning_tokens: int | None = None
    cached_tokens: int | None = None
    latency_ms: int | None = None
    created_at: str = ""


class ExtractionDB:
    """SQLite-backed store for extraction results with full audit trail."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._path = db_path
        conn = self._connect()
        conn.execute(_CREATE_TABLE)
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, record: ExtractionRecord) -> None:
        """Upsert a record (INSERT OR REPLACE) and commit immediately."""
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO extractions (
                pmid, schema_name, run_id,
                extracted_json, raw_response,
                fix_applied, fix_history, error_label,
                generation_id, model_used, provider_used,
                cost_usd, prompt_tokens, completion_tokens,
                reasoning_tokens, cached_tokens, latency_ms, created_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                record.pmid,
                record.schema_name,
                record.run_id,
                record.extracted_json,
                record.raw_response,
                record.fix_applied,
                record.fix_history,
                record.error_label,
                record.generation_id,
                record.model_used,
                record.provider_used,
                record.cost_usd,
                record.prompt_tokens,
                record.completion_tokens,
                record.reasoning_tokens,
                record.cached_tokens,
                record.latency_ms,
                record.created_at,
            ),
        )
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def exists(self, pmid: str, schema_name: str, run_id: str) -> bool:
        """Return True if a successful extraction already exists for this (pmid, schema, run)."""
        conn = self._connect()
        row = conn.execute(
            "SELECT 1 FROM extractions WHERE pmid=? AND schema_name=? AND run_id=? AND error_label IS NULL LIMIT 1",
            (pmid, schema_name, run_id),
        ).fetchone()
        conn.close()
        return row is not None

    def get_pending_pmids(
        self, all_pmids: list[str], schema_name: str, run_id: str
    ) -> list[str]:
        """Return PMIDs not yet successfully extracted for (schema_name, run_id)."""
        if not all_pmids:
            return []
        conn = self._connect()
        done_rows = conn.execute(
            "SELECT pmid FROM extractions WHERE schema_name=? AND run_id=? AND error_label IS NULL",
            (schema_name, run_id),
        ).fetchall()
        conn.close()
        done = {r[0] for r in done_rows}
        return [p for p in all_pmids if p not in done]

    def list_runs(self, schema_name: str) -> list[str]:
        """Return distinct run_ids for a schema, most-recent first."""
        conn = self._connect()
        rows = conn.execute(
            "SELECT DISTINCT run_id FROM extractions WHERE schema_name=? ORDER BY run_id DESC",
            (schema_name,),
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]

    def list_schemas(self) -> list[str]:
        """Return all distinct schema_names in the DB."""
        conn = self._connect()
        rows = conn.execute(
            "SELECT DISTINCT schema_name FROM extractions ORDER BY schema_name"
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]

    def get_summary(self, schema_name: str, run_id: str) -> dict[str, Any]:
        """Return counts and aggregate stats for a (schema, run) pair."""
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN error_label IS NULL THEN 1 ELSE 0 END) AS n_success,
                SUM(CASE WHEN error_label IS NOT NULL THEN 1 ELSE 0 END) AS n_failed,
                SUM(CASE WHEN fix_applied IS NOT NULL AND error_label IS NULL THEN 1 ELSE 0 END) AS n_repaired,
                SUM(cost_usd) AS total_cost_usd,
                MIN(created_at) AS first_at,
                MAX(created_at) AS last_at
            FROM extractions
            WHERE schema_name=? AND run_id=?
            """,
            (schema_name, run_id),
        ).fetchone()
        conn.close()
        if not rows:
            return {}
        return {
            "total": rows[0] or 0,
            "n_success": rows[1] or 0,
            "n_failed": rows[2] or 0,
            "n_repaired": rows[3] or 0,
            "total_cost_usd": round(rows[4] or 0.0, 6),
            "first_at": rows[5] or "",
            "last_at": rows[6] or "",
        }

    def export_jsonl(self, schema_name: str, run_id: str, out_path: Path) -> int:
        """Write successful extractions as JSONL. Returns row count."""
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT pmid, extracted_json, model_used, provider_used, cost_usd, created_at
            FROM extractions
            WHERE schema_name=? AND run_id=? AND extracted_json IS NOT NULL
            ORDER BY pmid
            """,
            (schema_name, run_id),
        ).fetchall()
        conn.close()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with out_path.open("w", encoding="utf-8") as fh:
            for pmid, extracted_json, model_used, provider_used, cost_usd, created_at in rows:
                try:
                    payload = json.loads(extracted_json)
                except Exception:
                    payload = {"_raw": extracted_json}
                record = {
                    "pmid": pmid,
                    "schema_name": schema_name,
                    "run_id": run_id,
                    "extracted": payload,
                    "model_used": model_used,
                    "provider_used": provider_used,
                    "cost_usd": cost_usd,
                    "created_at": created_at,
                }
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
        return count

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
