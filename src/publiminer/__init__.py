"""PubLiMiner — Publication Literature Miner.

Public API:
    GlobalConfig, Spine, FetchStep, ParseStep, DeduplicateStep

Example:
    >>> from publiminer import FetchStep, ParseStep, DeduplicateStep, Spine, GlobalConfig
"""

from __future__ import annotations

from publiminer.core.global_schema import GlobalConfig
from publiminer.core.spine import Spine
from publiminer.steps.deduplicate.step import DeduplicateStep
from publiminer.steps.fetch.step import FetchStep
from publiminer.steps.parse.step import ParseStep

__version__ = "0.1.0"

__all__ = [
    "DeduplicateStep",
    "FetchStep",
    "GlobalConfig",
    "ParseStep",
    "Spine",
    "__version__",
]
