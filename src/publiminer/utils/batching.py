"""Batch iteration and progress tracking for resume-from-crash support."""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")


def batched(items: Sequence[T], batch_size: int) -> Iterator[list[T]]:
    """Yield successive batches from items.

    Args:
        items: Sequence to batch.
        batch_size: Maximum items per batch.

    Yields:
        Lists of at most batch_size items.
    """
    for i in range(0, len(items), batch_size):
        yield list(items[i : i + batch_size])


class ProgressTracker:
    """Tracks processed items for resume-from-crash support.

    Saves a set of processed IDs to a JSON file after each batch.
    On restart, already-processed IDs are skipped.

    Args:
        tracker_path: Path to the JSON tracker file.
    """

    def __init__(self, tracker_path: str | Path) -> None:
        self.path = Path(tracker_path)
        self._processed: set[str] = set()
        self._load()

    def _load(self) -> None:
        """Load previously processed IDs from file."""
        if self.path.exists():
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self._processed = set(data.get("processed", []))

    def save(self) -> None:
        """Persist current state to file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps({"processed": sorted(self._processed)}, indent=2),
            encoding="utf-8",
        )

    def is_done(self, item_id: str) -> bool:
        """Check if an item has already been processed."""
        return item_id in self._processed

    def mark_done(self, item_id: str) -> None:
        """Mark an item as processed."""
        self._processed.add(item_id)

    def mark_batch_done(self, item_ids: Sequence[str]) -> None:
        """Mark a batch of items as processed and save."""
        self._processed.update(item_ids)
        self.save()

    @property
    def count(self) -> int:
        """Number of processed items."""
        return len(self._processed)

    def reset(self) -> None:
        """Clear all progress."""
        self._processed.clear()
        if self.path.exists():
            self.path.unlink()
