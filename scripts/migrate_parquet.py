"""One-time re-chunk + re-compress migration for papers.parquet.

Why this exists
---------------
Before the Spine write-defaults landed, every ``merge_staging`` cycle wrote
through ``pq.ParquetWriter`` with no compression argument, so pyarrow used
its ``snappy`` default. The live ``main/papers.parquet`` ended up entirely
snappy-compressed with ~120K-row groups. That row-group size is ~1.2 GB
compressed / 4-6 GB decoded — far larger than any streaming batch we want to
load, which means ``pyarrow.ParquetFile.iter_batches`` effectively cannot
stream it (a single row group materialises before any slicing).

This script rewrites the file one row group at a time, emitting the new
``PARQUET_ROW_GROUP_SIZE``-sized groups with zstd-3 compression. After it
runs, every subsequent ``Spine.iter_batches`` call actually honours
``batch_size``.

Usage
-----
    uv run python scripts/migrate_parquet.py [--output-dir DIR]

Defaults to ``./main`` to match ``publiminer.yaml``'s ``output_dir: main``.
Peak memory during migration equals the largest existing row group (~5 GB
on a pre-migration 583K-row file) — no worse than current steady state.

Safety
------
- The original file is copied to ``papers.parquet.bak`` before any write.
- The new file is written to ``papers.parquet.migrated`` then atomically
  swapped in via ``os.replace``.
- If anything fails before the final ``os.replace``, the original remains
  untouched.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

# Ensure ``src`` is importable when this file is run as a standalone script.
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from publiminer.constants import PARQUET_FILENAME  # noqa: E402
from publiminer.core.spine import (  # noqa: E402
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_DATA_PAGE_SIZE,
    PARQUET_ROW_GROUP_SIZE,
)


def _fmt_mb(n_bytes: int) -> str:
    return f"{n_bytes / (1024 * 1024):.1f} MB"


def migrate(output_dir: Path) -> None:
    src_path = output_dir / PARQUET_FILENAME
    dst_path = output_dir / (PARQUET_FILENAME + ".migrated")
    bak_path = output_dir / (PARQUET_FILENAME + ".bak")

    if not src_path.exists():
        raise SystemExit(f"Source parquet not found: {src_path}")

    src_size = src_path.stat().st_size
    print(f"Source: {src_path}")
    print(f"  size: {_fmt_mb(src_size)}")

    src_pf = pq.ParquetFile(src_path)
    src_meta = src_pf.metadata
    print(f"  rows: {src_meta.num_rows:,}")
    print(f"  row groups: {src_meta.num_row_groups}")
    print(f"  columns: {src_meta.num_columns}")
    if src_meta.num_row_groups > 0:
        rg0 = src_meta.row_group(0)
        print(f"  existing compression (rg0, col0): {rg0.column(0).compression}")

    # Backup — cheap safety net before we touch anything.
    print(f"\nBackup → {bak_path}")
    if bak_path.exists():
        print("  WARNING: backup already exists, skipping (delete it to re-run)")
    else:
        shutil.copy2(src_path, bak_path)
        print(f"  done ({_fmt_mb(bak_path.stat().st_size)})")

    # Rewrite: one source row group → one writer.write_table call, which the
    # writer splits into ``PARQUET_ROW_GROUP_SIZE`` chunks internally.
    print(
        f"\nRewriting → {dst_path}\n"
        f"  compression: {PARQUET_COMPRESSION} (level {PARQUET_COMPRESSION_LEVEL})\n"
        f"  row_group_size: {PARQUET_ROW_GROUP_SIZE:,}"
    )
    writer = pq.ParquetWriter(
        dst_path,
        src_pf.schema_arrow,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        write_statistics=True,
        data_page_size=PARQUET_DATA_PAGE_SIZE,
    )
    t0 = time.time()
    rows_written = 0
    try:
        for i in range(src_meta.num_row_groups):
            tbl = src_pf.read_row_group(i)
            writer.write_table(tbl, row_group_size=PARQUET_ROW_GROUP_SIZE)
            rows_written += len(tbl)
            elapsed = time.time() - t0
            print(
                f"  rg {i + 1}/{src_meta.num_row_groups}: "
                f"wrote {len(tbl):,} rows ({rows_written:,} total, "
                f"{elapsed:.1f}s elapsed)"
            )
            del tbl
    finally:
        writer.close()
    del src_pf

    # Verify the new file before swapping.
    dst_pf = pq.ParquetFile(dst_path)
    dst_meta = dst_pf.metadata
    dst_size = dst_path.stat().st_size
    print(
        f"\nVerification of {dst_path.name}:\n"
        f"  rows: {dst_meta.num_rows:,}\n"
        f"  row groups: {dst_meta.num_row_groups}\n"
        f"  size: {_fmt_mb(dst_size)} ({100 * dst_size / src_size:.1f}% of source)\n"
        f"  compression (rg0, col0): {dst_meta.row_group(0).column(0).compression}"
    )
    if dst_meta.num_rows != src_meta.num_rows:
        raise SystemExit(
            f"Row count mismatch: src={src_meta.num_rows} dst={dst_meta.num_rows}"
        )
    if dst_meta.num_row_groups < max(2, src_meta.num_row_groups):
        raise SystemExit(
            f"Row-group count did not grow: src={src_meta.num_row_groups} "
            f"dst={dst_meta.num_row_groups} — re-chunk failed"
        )
    compression = dst_meta.row_group(0).column(0).compression
    if compression.upper() != PARQUET_COMPRESSION.upper():
        raise SystemExit(
            f"Compression mismatch: expected {PARQUET_COMPRESSION}, got {compression}"
        )
    del dst_pf

    # Atomic swap — only happens if verification passed.
    print(f"\nAtomic swap: {dst_path.name} → {src_path.name}")
    os.replace(dst_path, src_path)
    print("  done")
    print(f"\nBackup retained at: {bak_path}")
    print("If the new file works correctly, you may delete the backup.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--output-dir",
        default="main",
        help="Directory containing papers.parquet (default: main)",
    )
    args = ap.parse_args()
    migrate(Path(args.output_dir))


if __name__ == "__main__":
    main()
