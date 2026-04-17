"""SQLite cache for raw external API responses."""

from __future__ import annotations

import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Any

from publiminer.constants import CACHE_DEFAULT_TTL_DAYS, CACHE_FILENAME


class ResponseCache:
    """SQLite-backed cache for raw API responses.

    Keys are MD5 hashes of (namespace + request params).
    Values are raw response strings with TTL-based expiry.

    Args:
        cache_dir: Directory to store cache.db.
        ttl_days: Time-to-live in days for cache entries.
    """

    def __init__(self, cache_dir: str | Path, ttl_days: int = CACHE_DEFAULT_TTL_DAYS) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.cache_dir / CACHE_FILENAME
        self.ttl_seconds = ttl_days * 86400
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _init_db(self) -> None:
        """Create the cache table if it doesn't exist."""
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                value TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_namespace ON cache(namespace)")
        conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create a database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
        return self._conn

    @staticmethod
    def _make_key(namespace: str, params: str) -> str:
        """Generate an MD5 cache key from namespace + params."""
        raw = f"{namespace}:{params}"
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, namespace: str, params: str) -> str | None:
        """Retrieve a cached response.

        Args:
            namespace: Cache namespace (e.g. 'pubmed', 'openrouter').
            params: Request parameters string.

        Returns:
            Cached response string, or None if not found or expired.
        """
        key = self._make_key(namespace, params)
        conn = self._get_conn()
        row = conn.execute("SELECT value, created_at FROM cache WHERE key = ?", (key,)).fetchone()

        if row is None:
            return None

        value, created_at = row
        if time.time() - created_at > self.ttl_seconds:
            # Expired — clean up
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()
            return None

        return value

    def put(self, namespace: str, params: str, value: str) -> None:
        """Store a response in the cache.

        Args:
            namespace: Cache namespace.
            params: Request parameters string.
            value: Response string to cache.
        """
        key = self._make_key(namespace, params)
        conn = self._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, namespace, value, created_at) VALUES (?, ?, ?, ?)",
            (key, namespace, value, time.time()),
        )
        conn.commit()

    def has(self, namespace: str, params: str) -> bool:
        """Check if a non-expired entry exists."""
        return self.get(namespace, params) is not None

    def clear(self, namespace: str | None = None, older_than_days: int | None = None) -> int:
        """Clear cache entries.

        Args:
            namespace: If set, only clear this namespace. If None, clear all.
            older_than_days: If set, only clear entries older than this.

        Returns:
            Number of entries deleted.
        """
        conn = self._get_conn()
        conditions: list[str] = []
        params: list[Any] = []

        if namespace is not None:
            conditions.append("namespace = ?")
            params.append(namespace)
        if older_than_days is not None:
            cutoff = time.time() - (older_than_days * 86400)
            conditions.append("created_at < ?")
            params.append(cutoff)

        where = " AND ".join(conditions) if conditions else "1=1"
        cursor = conn.execute(f"DELETE FROM cache WHERE {where}", params)
        conn.commit()
        return cursor.rowcount

    def stats(self) -> dict[str, Any]:
        """Return cache statistics by namespace."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT namespace, COUNT(*), SUM(LENGTH(value)) FROM cache GROUP BY namespace"
        ).fetchall()
        return {row[0]: {"count": row[1], "total_bytes": row[2] or 0} for row in rows}

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
