"""Build a journal registry CSV for manual scope annotation.

Reads papers.parquet and outputs the top N journals by article count as a CSV.
The 'scope' column is left blank for you to fill in manually:
  Radiology, Cardiology, Mix, General, Other

Usage:
    uv run python scripts/per-project/build_journal_registry.py --config projects/cardiac_mri.yaml
    uv run python scripts/per-project/build_journal_registry.py --config projects/cardiac_ct.yaml
    uv run python scripts/per-project/build_journal_registry.py --config projects/cardiac_mri.yaml --top 50

Output:
    {output_dir}/journal_registry.csv
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import polars as pl
import yaml


def _journal_title(journal_json: str | None) -> str | None:
    if not journal_json:
        return None
    try:
        j = json.loads(journal_json)
    except Exception:
        return None
    if isinstance(j, dict):
        return j.get("title") or j.get("journal_title") or j.get("iso_abbrev")
    if isinstance(j, str):
        return j
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Build journal registry CSV for manual annotation")
    parser.add_argument("--config", required=True, help="Path to project YAML config")
    parser.add_argument("--top", type=int, default=25, help="Number of top journals to include (default: 25)")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        raise SystemExit(f"Config not found: {config_path}")

    with config_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    output_dir = Path(cfg.get("general", {}).get("output_dir", "output"))
    parquet_path = output_dir / "papers.parquet"
    if not parquet_path.exists():
        raise SystemExit(f"papers.parquet not found at {parquet_path}")

    print(f"Reading: {parquet_path}")
    df = pl.read_parquet(parquet_path, memory_map=False, columns=["journal", "is_european"] if "is_european" in
                         pl.read_parquet(parquet_path, memory_map=False, n_rows=1).columns else ["journal"])

    total = len(df)
    n_european = 0
    if "is_european" in df.columns:
        n_european = int(df["is_european"].cast(pl.Boolean).fill_null(False).sum())
    print(f"  {total:,} papers total  |  {n_european:,} European")

    # Count journals across all papers
    all_titles = [_journal_title(v) for v in df["journal"].to_list()]
    counts_all = Counter(t for t in all_titles if t)

    # Count journals for European papers only
    counts_eu: Counter = Counter()
    if "is_european" in df.columns:
        eu_titles = [
            _journal_title(v)
            for v, is_eu in zip(df["journal"].to_list(), df["is_european"].to_list())
            if is_eu
        ]
        counts_eu = Counter(t for t in eu_titles if t)

    top_journals = counts_all.most_common(args.top)

    # Write CSV
    out_path = output_dir / "journal_registry.csv"
    lines = ["journal_name,count_total,count_european,scope"]
    for name, cnt in top_journals:
        eu_cnt = counts_eu.get(name, 0)
        safe_name = name.replace('"', '""')
        lines.append(f'"{safe_name}",{cnt},{eu_cnt},')

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nTop {args.top} journals written -> {out_path}")
    print("\nFill in the 'scope' column with: Radiology / Cardiology / Mix / General / Other\n")

    # Print preview
    print(f"{'Journal':<55} {'Total':>6} {'European':>8}  Scope")
    print("-" * 80)
    for name, cnt in top_journals[:15]:
        eu = counts_eu.get(name, 0)
        short = name[:53] + ".." if len(name) > 55 else name
        print(f"{short:<55} {cnt:>6} {eu:>8}")
    if len(top_journals) > 15:
        print(f"  ... and {len(top_journals) - 15} more in {out_path.name}")


if __name__ == "__main__":
    main()
