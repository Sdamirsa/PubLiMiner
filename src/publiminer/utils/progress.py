"""Dual-mode progress reporter.

Auto-switches based on whether stdout is a TTY:

- **TTY (terminal)**: animated Rich progress bar.
- **Non-TTY (subprocess / Streamlit)**: emits structured JSON-line events
  on stdout, prefixed with the sentinel `__PROGRESS__ `, that the UI can
  parse without polluting the regular log stream.

Event format (JSON):
    __PROGRESS__ {"step": "parse", "current": 50, "total": 1000,
                  "desc": "Parsing XML", "phase": "update"}

Phases: "start", "update", "end".

Usage:
    from publiminer.utils.progress import ProgressReporter

    with ProgressReporter("parse", total=1000, desc="Parsing XML") as p:
        for item in items:
            ...
            p.advance()
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from types import TracebackType
from typing import Any

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

PROGRESS_SENTINEL = "__PROGRESS__"


def _emit_event(payload: dict[str, Any]) -> None:
    """Write a single progress event line to stdout."""
    sys.stdout.write(f"{PROGRESS_SENTINEL} {json.dumps(payload)}\n")
    sys.stdout.flush()


class ProgressReporter:
    """Dual-mode progress reporter.

    Args:
        step: Step name (e.g. "parse", "fetch").
        total: Total number of items.
        desc: Human-readable description.
        update_every: Emit a JSON event every N items in non-TTY mode
            (avoids flooding stdout for large loops).
    """

    def __init__(
        self,
        step: str,
        total: int,
        desc: str = "",
        update_every: int = 1,
    ) -> None:
        self.step = step
        self._real_total = total  # may be 0 = unknown
        self.total = max(total, 1)
        self.desc = desc or step
        self.update_every = max(update_every, 1)
        self.current = 0
        self._is_tty = sys.stdout.isatty()
        # Modes: "tty" (rich bar), "json" (for UI subprocess), "log" (nightly)
        mode_env = os.environ.get("PUBLIMINER_PROGRESS", "").lower()
        if mode_env in ("log", "json", "tty"):
            self._mode = mode_env
        else:
            self._mode = "tty" if self._is_tty else "json"
        self._progress: Progress | None = None
        self._task_id: int | None = None
        self._start_ts: float = 0.0
        self._last_log_ts: float = 0.0
        self._log_every_sec: float = 30.0  # throttle log-mode to 1 line / 30s

    def _log_line(self, phase: str) -> None:
        now = time.time()
        elapsed = now - self._start_ts if self._start_ts else 0.0
        rate = self.current / elapsed if elapsed > 0 else 0.0
        ts = datetime.now().strftime("%H:%M:%S")
        if self._real_total > 0:
            pct = 100.0 * self.current / self._real_total
            total_str = f"{self._real_total:,}"
            pct_str = f"({pct:.1f}%) "
        else:
            total_str = "?"
            pct_str = ""
        if phase == "start":
            msg = f"[{ts}] {self.desc}: started (total={total_str})"
        elif phase == "end":
            msg = (
                f"[{ts}] {self.desc}: done {self.current:,}/{total_str} "
                f"{pct_str}in {elapsed:.0f}s ({rate:.0f}/s)"
            )
        else:
            msg = (
                f"[{ts}] {self.desc}: {self.current:,}/{total_str} "
                f"{pct_str}{rate:.0f}/s"
            )
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()

    def __enter__(self) -> ProgressReporter:
        self._start_ts = time.time()
        self._last_log_ts = self._start_ts
        if self._mode == "tty":
            self._progress = Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            )
            self._progress.start()
            self._task_id = self._progress.add_task(self.desc, total=self.total)
        elif self._mode == "log":
            self._log_line("start")
        else:
            _emit_event({
                "step": self.step,
                "phase": "start",
                "current": 0,
                "total": self.total,
                "desc": self.desc,
            })
        return self

    def advance(self, n: int = 1) -> None:
        """Advance progress by n items."""
        self.current += n
        if self._mode == "tty":
            assert self._progress is not None and self._task_id is not None
            self._progress.update(self._task_id, advance=n)
        elif self._mode == "log":
            now = time.time()
            reached_end = self._real_total > 0 and self.current >= self._real_total
            if now - self._last_log_ts >= self._log_every_sec or reached_end:
                self._log_line("update")
                self._last_log_ts = now
        else:
            # Throttle: emit only every N items, plus the final tick
            if self.current % self.update_every == 0 or self.current >= self.total:
                _emit_event({
                    "step": self.step,
                    "phase": "update",
                    "current": min(self.current, self.total),
                    "total": self.total,
                    "desc": self.desc,
                })

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._mode == "tty":
            assert self._progress is not None
            self._progress.stop()
        elif self._mode == "log":
            self._log_line("end")
        else:
            _emit_event({
                "step": self.step,
                "phase": "end",
                "current": min(self.current, self.total),
                "total": self.total,
                "desc": self.desc,
            })
