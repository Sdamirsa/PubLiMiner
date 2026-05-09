"""Generate a one-page A4 landscape pipeline overview PDF.

Reads actual numbers from the latest snapshot JSONs for both projects.

Usage:
    uv run python scripts/generate_pipeline_pdf.py
    uv run python scripts/generate_pipeline_pdf.py --out output/my_pipeline.pdf

Output:
    output/pipeline_overview.pdf
    output/pipeline_overview.png
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


# ── Load stats from snapshots ────────────────────────────────────────────────

def _load(pattern_dir: str, pattern: str) -> dict:
    snaps = sorted(Path(pattern_dir).glob(pattern))
    if not snaps:
        return {}
    d = json.loads(snaps[-1].read_text(encoding="utf-8"))
    cs = d.get("corpus_stats", {})
    rs = d.get("run_summary", {})
    return {
        "total":        cs.get("total_papers", 0),
        "european":     cs.get("european_papers", 0),
        "european_pct": cs.get("european_pct", 0),
        "classified":   rs.get("n_success", 0),
        "failed":       rs.get("n_failed", 0),
    }


# ── Drawing helpers ───────────────────────────────────────────────────────────

def _rounded_box(ax, x, y, w, h, fc, ec, lw=1.5, radius=0.12):
    box = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad={radius}",
        facecolor=fc, edgecolor=ec, linewidth=lw,
        transform=ax.transData, clip_on=False,
    )
    ax.add_patch(box)


def _step_box(ax, x, y, w, h, header_color, body_color,
              step_label, title, desc_lines, stat_lines):
    header_h = h * 0.28

    # Body
    _rounded_box(ax, x, y, w, h, body_color, header_color, lw=1.8)

    # Header overlay
    _rounded_box(ax, x, y + h - header_h, w, header_h + 0.02,
                 header_color, header_color, lw=0, radius=0.10)
    rect = mpatches.Rectangle((x, y + h - header_h), w, header_h * 0.4,
                               facecolor=header_color, edgecolor='none')
    ax.add_patch(rect)

    # Step badge circle
    cx = x + 0.28
    cy = y + h - header_h / 2
    circle = plt.Circle((cx, cy), 0.18, color='white', zorder=4)
    ax.add_patch(circle)
    ax.text(cx, cy, step_label, ha='center', va='center',
            fontsize=10, fontweight='bold', color=header_color, zorder=5)

    # Header title
    ax.text(x + w / 2 + 0.1, y + h - header_h / 2, title,
            ha='center', va='center',
            fontsize=9.5, fontweight='bold', color='white', zorder=5)

    # Description lines
    desc_top = y + h - header_h - 0.22
    for i, line in enumerate(desc_lines):
        ax.text(x + w / 2, desc_top - i * 0.32, line,
                ha='center', va='top', fontsize=8, color='#2D3748')

    # Stat lines (bottom of box)
    stat_top = y + 0.60
    for i, line in enumerate(stat_lines):
        ax.text(x + w / 2, stat_top - i * 0.28, line,
                ha='center', va='top',
                fontsize=7.5, color=header_color, fontweight='bold')


def _arrow(ax, x1, x2, yc, color='#718096'):
    ax.annotate(
        '', xy=(x2, yc), xytext=(x1, yc),
        arrowprops=dict(arrowstyle='-|>', color=color, lw=1.8,
                        mutation_scale=14),
        zorder=3,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='output/pipeline_overview.pdf')
    args = parser.parse_args()

    mri = _load('output/cardiac_mri', 'snapshot_cardiac_mri_v2_*.json')
    ct  = _load('output/cardiac_ct',  'snapshot_cardiac_ct_v2_*.json')

    mri_total      = f"{mri['total']:,}"          if mri else "N/A"
    mri_eu         = f"{mri['european']:,}"        if mri else "N/A"
    mri_eu_pct     = f"{mri['european_pct']:.0f}"  if mri else "N/A"
    mri_classified = f"{mri['classified']:,}"      if mri else "N/A"
    ct_total       = f"{ct['total']:,}"            if ct  else "N/A"
    ct_eu          = f"{ct['european']:,}"         if ct  else "N/A"
    ct_eu_pct      = f"{ct['european_pct']:.0f}"   if ct  else "N/A"
    ct_classified  = f"{ct['classified']:,}"       if ct  else "N/A"

    # ── Figure setup ─────────────────────────────────────────────────────────
    W, H = 11.69, 8.27          # A4 landscape (inches)
    fig = plt.figure(figsize=(W, H), facecolor='white')
    ax  = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis('off')

    # ── Vertical layout (bottom → top) ───────────────────────────────────────
    # footnote → output strip → [gap] → AI callout → [gap] → step boxes → [gap] → header
    footnote_y = 0.10
    out_y      = 0.28;  out_h     = 0.68   # output strip  0.28 → 0.96
    callout_y  = 1.08;  callout_h = 1.00   # AI callout    1.08 → 2.08
    box_y      = 2.22;  box_h     = 4.50   # step boxes    2.22 → 6.72
    header_y   = 6.96;  header_h  = 1.14   # header bar    6.96 → 8.10

    # ── Header bar ───────────────────────────────────────────────────────────
    _rounded_box(ax, 0.25, header_y, W - 0.5, header_h,
                 '#1F4E79', '#1F4E79', lw=0, radius=0.15)
    ax.text(W / 2, header_y + header_h * 0.64,
            'Cardiac Imaging Literature Analysis',
            ha='center', va='center',
            fontsize=18, fontweight='bold', color='white')
    ax.text(W / 2, header_y + header_h * 0.24,
            'Automated PubMed pipeline  ·  European publications 2025–2026  '
            '·  AI-assisted classification',
            ha='center', va='center',
            fontsize=9.5, color='#BEE3F8')

    # ── Pipeline step boxes ──────────────────────────────────────────────────
    STEPS = [
        dict(color='#2166AC', light='#EBF4FF', label='1', title='SEARCH',
             desc=['Search PubMed for', 'cardiac MRI & CT', 'publications'],
             stats=[f'MRI  {mri_total} papers',
                    f'CT    {ct_total} papers']),
        dict(color='#2C7A7B', light='#E6FFFA', label='2', title='PARSE',
             desc=['Extract structured', 'data from PubMed XML', 'records'],
             stats=['Title · Abstract',
                    'Authors · Affiliations',
                    'Grants · Pub. type']),
        dict(color='#6B4226', light='#FFF8F0', label='3', title='DEDUPLICATE',
             desc=['Remove identical or', 'near-identical papers', '(DOI + title match)'],
             stats=['Exact DOI match',
                    'Fuzzy title ≥90%',
                    'similarity']),
        dict(color='#276749', light='#F0FFF4', label='4', title='EU FILTER',
             desc=['Keep papers with', '≥1 European author', 'affiliation'],
             stats=[f'MRI  {mri_eu} ({mri_eu_pct}%)',
                    f'CT    {ct_eu} ({ct_eu_pct}%)']),
        dict(color='#9B2226', light='#FFF5F5', label='5', title='AI CLASSIFY',
             desc=['AI reads each paper', 'and answers', '5 structured questions'],
             stats=[f'MRI  {mri_classified} classified',
                    f'CT    {ct_classified} classified']),
    ]

    n       = len(STEPS)
    box_w   = 1.88
    arrow_w = 0.30
    total_w = n * box_w + (n - 1) * arrow_w
    start_x = (W - total_w) / 2
    arrow_y = box_y + box_h / 2

    for i, step in enumerate(STEPS):
        bx = start_x + i * (box_w + arrow_w)
        _step_box(ax, bx, box_y, box_w, box_h,
                  step['color'], step['light'],
                  step['label'], step['title'],
                  step['desc'], step['stats'])
        if i < n - 1:
            _arrow(ax, bx + box_w + 0.03, bx + box_w + arrow_w - 0.03,
                   arrow_y, color='#A0AEC0')

    # ── AI fields callout — full width, 5 equal columns ──────────────────────
    # Each column = one extracted field. Two option lines per field for readability.
    fields_5col = [
        ('Relevance',     ['Main', 'Secondary · NotRelevant']),
        ('Study type',    ['Original · Review', 'Case Rept · Systematic · Other']),
        ('First author',  ['Radiology', 'Cardiology · Unclear']),
        ('Last author',   ['Radiology', 'Cardiology · Unclear']),
        ('Corresponding', ['Radiology · Cardiology', 'Unclear · Not reported']),
    ]
    cx = start_x - 0.05
    cw = total_w + 0.10
    cy = callout_y
    ch = callout_h

    _rounded_box(ax, cx, cy, cw, ch, '#FFF5F5', '#9B2226', lw=1.2, radius=0.10)
    ax.text(cx + cw / 2, cy + ch - 0.14,
            'AI extracts 5 fields per paper:',
            ha='center', va='top', fontsize=8, fontweight='bold', color='#9B2226')

    col_w = cw / n
    for j, (fname, opt_lines) in enumerate(fields_5col):
        fcx = cx + j * col_w + col_w / 2
        ax.text(fcx, cy + ch - 0.32, fname,
                ha='center', va='top',
                fontsize=7.5, fontweight='bold', color='#2D3748')
        for k, line in enumerate(opt_lines):
            ax.text(fcx, cy + ch - 0.52 - k * 0.21, line,
                    ha='center', va='top', fontsize=6.8, color='#555555')

    # Light dividers between columns
    for j in range(1, n):
        div_x = cx + j * col_w
        ax.plot([div_x, div_x], [cy + 0.12, cy + ch - 0.10],
                color='#FFCDD2', linewidth=0.7, zorder=2)

    # ── Output strip ─────────────────────────────────────────────────────────
    _rounded_box(ax, 0.35, out_y, W - 0.70, out_h,
                 '#EBF4FF', '#2166AC', lw=1.2, radius=0.10)
    ax.text(W / 2, out_y + out_h * 0.70,
            'OUTPUT',
            ha='center', va='center',
            fontsize=8.5, fontweight='bold', color='#2166AC')
    ax.text(W / 2, out_y + out_h * 0.28,
            '12 charts per project (specialty · relevance · study type · geography · funding · journal scope)'
            '  ·  JSON snapshot  ·  Journal registry CSV',
            ha='center', va='center',
            fontsize=8, color='#2D3748')

    # ── Footnote ─────────────────────────────────────────────────────────────
    ax.text(W / 2, footnote_y,
            'PubLiMiner · open-source PubMed mining pipeline · github.com/sdamirsa/PubLiMiner',
            ha='center', va='center',
            fontsize=6.5, color='#A0AEC0')

    # ── Save ─────────────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, format='pdf', bbox_inches='tight',
                facecolor='white', dpi=200)
    print(f'Saved -> {out_path}  ({out_path.stat().st_size / 1024:.0f} KB)')

    png_path = out_path.with_suffix('.png')
    fig.savefig(png_path, format='png', bbox_inches='tight',
                facecolor='white', dpi=200)
    print(f'Saved -> {png_path}  ({png_path.stat().st_size / 1024:.0f} KB)')

    plt.close(fig)


if __name__ == '__main__':
    main()
