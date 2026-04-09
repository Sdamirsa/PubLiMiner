"""Parquet backbone — read, write, update, append, inspect.

The single `papers.parquet` file is the source of truth for all paper data.
Each pipeline step reads columns, adds columns, and writes back.
"""

from __future__ import annotations

import gc
import os
from pathlib import Path
from typing import Sequence

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from publiminer.constants import PARQUET_FILENAME
from publiminer.exceptions import SpineError

STAGING_FILENAME = PARQUET_FILENAME + ".staging"


class Spine:
    """Parquet backbone for paper data.

    Args:
        output_dir: Directory containing papers.parquet.
    """

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_path = self.output_dir / PARQUET_FILENAME
        self.staging_path = self.output_dir / STAGING_FILENAME

    @property
    def staging_exists(self) -> bool:
        return self.staging_path.exists()

    def append_staging(self, df: pl.DataFrame) -> None:
        """Append rows to a small staging parquet (cheap; doesn't touch main file).

        Crash-safe checkpoint: if a fetch dies mid-run, the staging file holds
        all completed batches. The next run merges it before continuing.
        """
        if not self.staging_exists:
            tmp = self.staging_path.with_suffix(".staging.tmp")
            df.write_parquet(tmp)
            os.replace(tmp, self.staging_path)
            return
        existing = pl.read_parquet(self.staging_path, memory_map=False)
        all_columns = list(dict.fromkeys(existing.columns + df.columns))
        for col in all_columns:
            if col not in existing.columns:
                existing = existing.with_columns(pl.lit(None).alias(col))
            if col not in df.columns:
                dtype = existing.schema[col]
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        existing = existing.select(all_columns)
        df = df.select(all_columns)
        combined = pl.concat([existing, df], how="vertical_relaxed")
        tmp = self.staging_path.with_suffix(".staging.tmp")
        combined.write_parquet(tmp)
        gc.collect()
        os.replace(tmp, self.staging_path)

    def get_staging_pmids(self) -> set[str]:
        if not self.staging_exists:
            return set()
        return set(
            pl.read_parquet(self.staging_path, memory_map=False, columns=["pmid"])
            ["pmid"].to_list()
        )

    def merge_staging(self) -> int:
        """Stream-merge staging file into main parquet using pyarrow row groups.

        Memory cap = one row group (~50 MB) regardless of file sizes.
        Returns number of rows merged.
        """
        if not self.staging_exists:
            return 0
        if not self.exists:
            os.replace(self.staging_path, self.parquet_path)
            return self.count()

        main_pf = pq.ParquetFile(self.parquet_path)
        stage_pf = pq.ParquetFile(self.staging_path)
        merged_rows = stage_pf.metadata.num_rows

        # Build union schema (main fields first, then any new staging fields)
        seen: dict[str, pa.Field] = {}
        for f in main_pf.schema_arrow:
            seen[f.name] = f
        for f in stage_pf.schema_arrow:
            if f.name not in seen:
                seen[f.name] = f
        union_schema = pa.schema(list(seen.values()))

        tmp = self.parquet_path.with_suffix(".parquet.tmp")
        writer = pq.ParquetWriter(tmp, union_schema)

        def _stream(pf: pq.ParquetFile) -> None:
            for i in range(pf.num_row_groups):
                tbl = pf.read_row_group(i)
                # Add missing columns as nulls, reorder to union schema
                for name in union_schema.names:
                    if name not in tbl.schema.names:
                        field = union_schema.field(name)
                        tbl = tbl.append_column(
                            name, pa.nulls(len(tbl), type=field.type)
                        )
                tbl = tbl.select(union_schema.names)
                writer.write_table(tbl)

        try:
            _stream(main_pf)
            _stream(stage_pf)
        finally:
            writer.close()

        del main_pf, stage_pf
        gc.collect()
        os.replace(tmp, self.parquet_path)
        self.staging_path.unlink()
        return merged_rows

    @property
    def exists(self) -> bool:
        """Check if the parquet file exists."""
        return self.parquet_path.exists()

    def read(
        self,
        columns: list[str] | None = None,
        filter_expr: pl.Expr | None = None,
    ) -> pl.DataFrame:
        """Read the parquet file.

        Args:
            columns: Specific columns to read (None = all).
            filter_expr: Polars filter expression to apply.

        Returns:
            DataFrame with requested data.
        """
        if not self.exists:
            raise SpineError(f"Parquet file not found: {self.parquet_path}")

        # Eager read with mmap disabled — on Windows, mmap'd reads block
        # subsequent writes to the same file (OSError 1224).
        df = pl.read_parquet(self.parquet_path, memory_map=False)
        if columns:
            valid_cols = [c for c in columns if c in df.columns]
            if not valid_cols:
                raise SpineError(f"None of the requested columns exist: {columns}")
            df = df.select(valid_cols)
        if filter_expr is not None:
            df = df.filter(filter_expr)
        return df

    def write(self, df: pl.DataFrame) -> None:
        """Atomically write a DataFrame as the parquet file (full overwrite).

        Writes to a sibling temp file then renames, so any prior memory-map
        of the target path (Windows OSError 1224) cannot block the write.

        Args:
            df: DataFrame to write.
        """
        tmp_path = self.parquet_path.with_suffix(".parquet.tmp")
        df.write_parquet(tmp_path)
        # Force release of any lingering mmaps before replace
        gc.collect()
        os.replace(tmp_path, self.parquet_path)

    def append(self, df: pl.DataFrame) -> None:
        """Append rows to the existing parquet file.

        If the file doesn't exist, creates it. Handles schema evolution
        by filling missing columns with null.

        Args:
            df: DataFrame with rows to append.
        """
        if not self.exists:
            self.write(df)
            return

        existing = pl.read_parquet(self.parquet_path, memory_map=False)

        # Schema evolution: align columns
        all_columns = list(dict.fromkeys(existing.columns + df.columns))

        for col in all_columns:
            if col not in existing.columns:
                existing = existing.with_columns(pl.lit(None).alias(col))
            if col not in df.columns:
                # Match the dtype from existing if possible
                if col in existing.columns:
                    dtype = existing.schema[col]
                    df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))

        # Ensure same column order
        existing = existing.select(all_columns)
        df = df.select(all_columns)

        combined = pl.concat([existing, df], how="vertical_relaxed")
        self.write(combined)

    def update_columns(self, pmids: list[str], updates: dict[str, list]) -> None:
        """Update specific columns for specific PMIDs.

        Args:
            pmids: List of PMIDs to update.
            updates: Dict of column_name -> list of values (same order as pmids).
        """
        if not self.exists:
            raise SpineError("Cannot update: parquet file does not exist")

        df = pl.read_parquet(self.parquet_path, memory_map=False)

        # Build update DataFrame
        update_data: dict[str, list] = {"pmid": pmids}
        update_data.update(updates)
        update_df = pl.DataFrame(update_data)

        # Left join to bring in updates
        for col in updates:
            if col in df.columns:
                df = df.drop(col)

        df = df.join(update_df, on="pmid", how="left")
        self.write(df)

    def add_columns(self, new_df: pl.DataFrame, on: str = "pmid") -> None:
        """Add new columns to the parquet by joining on a key column.

        Args:
            new_df: DataFrame with the join key + new columns.
            on: Column to join on.
        """
        if not self.exists:
            raise SpineError("Cannot add columns: parquet file does not exist")

        existing = pl.read_parquet(self.parquet_path, memory_map=False)

        # Upsert semantics: rows in new_df update matching pmids; rows in
        # existing whose pmid is NOT in new_df are left untouched. This is
        # what makes incremental parse safe (only new rows get parsed).
        for col in new_df.columns:
            if col == on:
                continue
            if col not in existing.columns:
                dtype = new_df.schema[col]
                existing = existing.with_columns(
                    pl.lit(None).cast(dtype).alias(col)
                )

        result = existing.update(new_df, on=on, how="left")
        self.write(result)

    def remove_rows(self, pmids: Sequence[str]) -> int:
        """Remove rows by PMID.

        Args:
            pmids: PMIDs to remove.

        Returns:
            Number of rows removed.
        """
        if not self.exists:
            return 0

        df = pl.read_parquet(self.parquet_path, memory_map=False)
        before = len(df)
        df = df.filter(~pl.col("pmid").is_in(list(pmids)))
        after = len(df)
        self.write(df)
        return before - after

    def inspect(self) -> dict:
        """Return summary statistics about the parquet file."""
        if not self.exists:
            return {"exists": False}

        df = pl.read_parquet(self.parquet_path, memory_map=False)
        return {
            "exists": True,
            "rows": len(df),
            "columns": df.columns,
            "schema": {col: str(dtype) for col, dtype in df.schema.items()},
            "file_size_mb": round(self.parquet_path.stat().st_size / (1024 * 1024), 2),
        }

    def get_pmids(self) -> set[str]:
        """Get all PMIDs in the parquet file."""
        if not self.exists:
            return set()
        df = self.read(columns=["pmid"])
        return set(df["pmid"].to_list())

    def count(self) -> int:
        """Get the number of rows."""
        if not self.exists:
            return 0
        return len(pl.read_parquet(self.parquet_path, memory_map=False, columns=["pmid"]))
