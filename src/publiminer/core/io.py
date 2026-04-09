"""Step metadata I/O — JSON read/write for step logs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from publiminer.constants import STEP_LOG_DIR


@dataclass
class StepMeta:
    """Metadata for a single step run."""

    step_name: str
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    status: str = "pending"  # pending, running, completed, failed
    rows_before: int = 0
    rows_after: int = 0
    rows_added: int = 0
    rows_removed: int = 0
    errors: int = 0
    config_snapshot: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        """Mark step as started."""
        self.started_at = datetime.now().isoformat()
        self.status = "running"

    def finish(self, status: str = "completed") -> None:
        """Mark step as finished."""
        self.finished_at = datetime.now().isoformat()
        self.status = status
        if self.started_at:
            start = datetime.fromisoformat(self.started_at)
            end = datetime.fromisoformat(self.finished_at)
            self.duration_seconds = round((end - start).total_seconds(), 2)


def save_step_meta(meta: StepMeta, output_dir: str | Path) -> Path:
    """Save step metadata to JSON file.

    Args:
        meta: StepMeta instance.
        output_dir: Pipeline output directory.

    Returns:
        Path to the saved JSON file.
    """
    log_dir = Path(output_dir) / STEP_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{meta.step_name}_meta.json"
    path.write_text(json.dumps(asdict(meta), indent=2), encoding="utf-8")
    return path


def load_step_meta(step_name: str, output_dir: str | Path) -> StepMeta | None:
    """Load step metadata from JSON file.

    Args:
        step_name: Name of the step.
        output_dir: Pipeline output directory.

    Returns:
        StepMeta instance or None if not found.
    """
    path = Path(output_dir) / STEP_LOG_DIR / f"{step_name}_meta.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return StepMeta(**data)
