"""StepBase ABC — the contract all pipeline steps follow."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from publiminer.core.config import GlobalConfig
from publiminer.core.io import StepMeta, save_step_meta
from publiminer.core.spine import Spine
from publiminer.exceptions import StepError
from publiminer.utils.logger import get_step_logger


class StepBase(ABC):
    """Abstract base class for all pipeline steps.

    Each step:
    1. Reads from the Parquet spine
    2. Performs its work
    3. Writes results back to the spine
    4. Records metadata

    Args:
        name: Step name (e.g. 'fetch', 'parse').
        global_config: Global pipeline configuration.
        step_config: Step-specific configuration (Pydantic model).
        output_dir: Pipeline output directory.
    """

    name: str = ""

    def __init__(
        self,
        global_config: GlobalConfig,
        step_config: Any,
        output_dir: str | Path | None = None,
    ) -> None:
        self.global_config = global_config
        self.step_config = step_config
        self.output_dir = Path(output_dir or global_config.general.output_dir)
        self.spine = Spine(self.output_dir)
        self.logger = get_step_logger(self.name)
        self.meta = StepMeta(step_name=self.name)

    @abstractmethod
    def run(self) -> StepMeta:
        """Execute the step. Must be implemented by subclasses.

        Returns:
            StepMeta with run statistics.
        """

    def validate_input(self) -> None:  # noqa: B027
        """Validate that required input data exists. Override in subclasses."""

    def validate_output(self) -> None:  # noqa: B027
        """Validate that output data was written correctly. Override in subclasses."""

    def execute(self) -> StepMeta:
        """Full step execution with validation and metadata tracking.

        Returns:
            StepMeta with run statistics.
        """
        self.logger.info(f"Starting step: {self.name}")
        self.meta.start()

        try:
            self.validate_input()
            self.meta.rows_before = self.spine.count() if self.spine.exists else 0
            self.meta = self.run()
            self.meta.rows_after = self.spine.count()
            self.validate_output()
            self.meta.finish("completed")
            self.logger.info(
                f"Step {self.name} completed: "
                f"{self.meta.rows_before} -> {self.meta.rows_after} rows "
                f"({self.meta.duration_seconds}s)"
            )
        except Exception as e:
            self.meta.finish("failed")
            self.meta.extra["error"] = str(e)
            self.logger.error(f"Step {self.name} failed: {e}")
            save_step_meta(self.meta, self.output_dir)
            if self.global_config.general.on_error == "fail":
                raise StepError(self.name, str(e)) from e
        finally:
            save_step_meta(self.meta, self.output_dir)

        return self.meta
