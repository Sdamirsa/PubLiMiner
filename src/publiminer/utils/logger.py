"""Structured logging with step context."""

from __future__ import annotations

import logging
from pathlib import Path

from rich.logging import RichHandler


def setup_logger(
    name: str = "publiminer",
    level: str = "INFO",
    log_dir: str | Path | None = None,
    log_file: str | None = None,
) -> logging.Logger:
    """Configure and return a logger with rich console + optional file output.

    Args:
        name: Logger name.
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        log_dir: Directory for log files. If None, file logging is skipped.
        log_file: Log file name. Defaults to "{name}.log".

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Rich console handler
    console_handler = RichHandler(
        rich_tracebacks=True,
        show_time=True,
        show_path=False,
        markup=True,
    )
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_dir is not None:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        file_name = log_file or f"{name}.log"
        file_handler = logging.FileHandler(log_dir / file_name, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "publiminer") -> logging.Logger:
    """Get an existing logger by name."""
    return logging.getLogger(name)


def get_step_logger(step_name: str) -> logging.Logger:
    """Get a logger scoped to a specific step."""
    return logging.getLogger(f"publiminer.{step_name}")
