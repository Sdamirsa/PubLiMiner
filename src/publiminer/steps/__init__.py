"""Step registry — maps step names to their classes."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from publiminer.core.base_step import StepBase

_STEP_REGISTRY: dict[str, type[StepBase]] = {}


def register_step(name: str, cls: type[StepBase]) -> None:
    """Register a step class by name."""
    _STEP_REGISTRY[name] = cls


def get_step(name: str) -> type[StepBase]:
    """Get a step class by name."""
    if name not in _STEP_REGISTRY:
        raise KeyError(f"Unknown step: {name!r}. Available: {list(_STEP_REGISTRY)}")
    return _STEP_REGISTRY[name]


def list_steps() -> list[str]:
    """List all registered step names."""
    return list(_STEP_REGISTRY)
