"""Quick presentation snapshot — run while extract is still going.

Usage:
    uv run python scripts/quick_viz.py
    uv run python scripts/quick_viz.py --output output/cardiac_ct   # for CT project
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl

parser = argparse.ArgumentParser()
parser.add_argument("--output", default="output/cardiac_mri")
args = parser.parse_args()
OUT = Path(args.output)

PARQUET = OUT / "papers.parquet"
DB = OUT / "extractions.db"

df = pl.read_parquet(PARQUET, memory_map=False)
total = len(df)
n_european = int(df["is_european"].sum()) if "is_european" in df.columns else 0

print(f"Total papers (post-dedup): {total:,}")
print(f"European affiliations:     {n_european:,} ({100*n_european/total:.1f}%)")

# ── figure layout ──────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(15, 5))
fig.suptitle(
    f"Cardiac MRI — PubMed 2023–2026 pilot  |  {total:,} papers after dedup",
    fontsize=13, fontweight="bold", y=1.02,
)

# Panel 1 — Papers per year (total vs European)
ax = axes[0]
years = df.filter(pl.col("year").is_not_null()).group_by("year").agg(
    pl.len().alias("total"),
    pl.col("is_european").sum().alias("european") if "is_european" in df.columns
    else pl.lit(0).alias("european"),
).sort("year").filter(pl.col("year") >= 2023)

x = years["year"].to_list()
tot = years["total"].to_list()
eur = years["european"].to_list()
bar_w = 0.35
xs = range(len(x))
ax.bar([i - bar_w/2 for i in xs], tot, bar_w, label="All", color="#4472C4")
ax.bar([i + bar_w/2 for i in xs], eur, bar_w, label="European", color="#ED7D31")
ax.set_xticks(list(xs)); ax.set_xticklabels([str(y) for y in x])
ax.set_title("Papers per year"); ax.set_ylabel("Count")
ax.legend(); ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))

# Panel 2 — European vs non-European
ax = axes[1]
labels = ["European\naffiliated", "Non-European"]
sizes  = [n_european, total - n_european]
colors = ["#ED7D31", "#4472C4"]
wedges, texts, autotexts = ax.pie(
    sizes, labels=labels, colors=colors, autopct="%1.1f%%",
    startangle=90, textprops={"fontsize": 10},
)
ax.set_title(f"Geographic split\n(n={total:,})")

# Panel 3 — Specialty breakdown (from extractions.db, whatever is done so far)
ax = axes[2]
if DB.exists():
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT extracted_json FROM extractions WHERE error_label IS NULL AND extracted_json IS NOT NULL"
    ).fetchall()
    con.close()

    first_counts: dict[str, int] = {}
    last_counts:  dict[str, int] = {}
    for (raw,) in rows:
        try:
            data = json.loads(raw)
            f = data.get("first_author_specialty", "Unclear")
            l = data.get("last_author_specialty", "Unclear")
            first_counts[f] = first_counts.get(f, 0) + 1
            last_counts[l]  = last_counts.get(l, 0)  + 1
        except Exception:
            pass

    categories = ["Radiology", "Cardiology", "Unclear"]
    f_vals = [first_counts.get(c, 0) for c in categories]
    l_vals = [last_counts.get(c, 0) for c in categories]
    bar_w = 0.35
    xs2 = range(len(categories))
    ax.bar([i - bar_w/2 for i in xs2], f_vals, bar_w, label="First author", color="#4472C4")
    ax.bar([i + bar_w/2 for i in xs2], l_vals, bar_w, label="Last author",  color="#ED7D31")
    ax.set_xticks(list(xs2)); ax.set_xticklabels(categories)
    ax.set_title(f"Author specialty\n(preliminary, n={len(rows):,} extracted so far)")
    ax.set_ylabel("Papers"); ax.legend()
    print(f"\nExtracted so far: {len(rows):,}")
    print("First author:", first_counts)
    print("Last author: ", last_counts)
else:
    ax.text(0.5, 0.5, "extractions.db\nnot found yet", ha="center", va="center",
            transform=ax.transAxes, fontsize=12, color="grey")
    ax.set_title("Author specialty\n(extract step not run yet)")

plt.tight_layout()
out_png = OUT / "presentation_snapshot.png"
plt.savefig(out_png, dpi=150, bbox_inches="tight")
print(f"\nSaved → {out_png}")
plt.show()
