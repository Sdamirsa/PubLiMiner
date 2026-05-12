#!/usr/bin/env python3
"""
Bern Cardiac CT — Inselspital & University of Bern Publication Profile (2001–2026)
===================================================================================
Creates a 5-panel figure summarising Bern institutions' output in the global
cardiac CT literature (2001–2026).

Output: output/cardiac_ct/analysis/figures/bern_stat.png

Run from the repository root:
    .venv/Scripts/python output/cardiac_ct/analysis/bern_analysis.py
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
import warnings
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import polars as pl

warnings.filterwarnings("ignore")

# ── Project-dir helpers (supports --project-dir when called from scripts/) ───
def _get_base_dir() -> Path:
    """Return project output dir from --project-dir arg or legacy __file__ anchor."""
    import argparse as _ap
    p = _ap.ArgumentParser(add_help=False)
    p.add_argument("--project-dir", default=None)
    a, _ = p.parse_known_args()
    return Path(a.project_dir).resolve() if a.project_dir else Path(__file__).parent.parent


def _load_run_meta(bd: Path) -> tuple[str, str]:
    """Auto-detect schema_name / run_id from step_log/extract_meta.json."""
    meta = bd / "step_log" / "extract_meta.json"
    if meta.exists():
        try:
            import json as _j
            d = _j.loads(meta.read_text(encoding="utf-8"))
            s = d.get("extra", {}).get("schema_name", "")
            r = d.get("extra", {}).get("run_id", "")
            if s and r:
                return s, r
        except Exception:
            pass
    return "cardiac_ct_v2", "20260509T112353Z"


# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = _get_base_dir()
PARQUET        = BASE_DIR / "papers.parquet"
EXTRACTIONS_DB = BASE_DIR / "extractions.db"
FIG_DIR        = BASE_DIR / "analysis" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

_src_root = BASE_DIR.parent.parent / "src"
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))
from publiminer.utils.affiliation_parser import parse_affiliation as _parse_aff

SCHEMA, RUN_ID = _load_run_meta(BASE_DIR)
PROJECT_NAME   = " ".join(
    w.upper() if w.lower() in ("ct", "mri", "pet") else w.title()
    for w in BASE_DIR.name.replace("_", " ").split()
)

# ── Bern detection patterns ───────────────────────────────────────────────────
_INS_RE = re.compile(
    r"inselspital|"
    r"bern(?:e)?\s+university\s+hospital|"
    r"university\s+hospital\s+(?:of\s+)?bern(?:e)?|"
    r"universit\w+spital\s+bern(?:e)?",
    re.IGNORECASE,
)
_UBERN_RE = re.compile(
    r"university\s+of\s+bern(?:e)?|"
    r"universit\w+t\s+bern(?:e)?|"
    r"\bunibe\b",
    re.IGNORECASE,
)
_BERN_CH_RE = re.compile(r"\bbern(?:e)?\b", re.IGNORECASE)

# All hospital names that the affiliation parser may produce for Inselspital
BERN_HOSP_ALIASES: frozenset[str] = frozenset({
    "university hospital bern",
    "bern university hospital",
    "inselspital - university hospital bern",
    "inselspital-university hospital bern",
    "inselspital university hospital",
    "university hospital of bern",
    "inselspital, university hospital bern",
    "from bern university hospital",
    "inselspital - bern university hospital",
    "inselspital bern",
})

# ── Colour scheme ─────────────────────────────────────────────────────────────
C_INS   = "#C0392B"   # Inselspital — rich red
C_UBERN = "#2471A3"   # University of Bern — deep blue
C_BOTH  = "#8E44AD"   # Both in same paper — purple
C_BERN  = "#27AE60"   # Total Bern — green
C_OTHER = "#7F8C8D"   # Context (other EU) — grey

STUDY_C = {
    "OriginalResearch":               "#2980B9",
    "CaseReportOrSeries":             "#E67E22",
    "Review":                         "#9B59B6",
    "SystematicReviewOrMetaAnalysis": "#1ABC9C",
    "Other":                          "#95A5A6",
}
SPEC_C = {
    "Cardiology": "#E74C3C",
    "Radiology":  "#2980B9",
    "Unclear":    "#95A5A6",
}

# ── Shared tables ─────────────────────────────────────────────────────────────
COUNTRY_REGION: dict[str, str] = {
    "switzerland": "Western Europe", "germany": "Western Europe",
    "france": "Western Europe",      "netherlands": "Western Europe",
    "belgium": "Western Europe",     "austria": "Western Europe",
    "luxembourg": "Western Europe",
    "united kingdom": "Northern Europe", "england": "Northern Europe",
    "scotland": "Northern Europe",   "wales": "Northern Europe",
    "ireland": "Northern Europe",    "sweden": "Northern Europe",
    "norway": "Northern Europe",     "denmark": "Northern Europe",
    "finland": "Northern Europe",    "iceland": "Northern Europe",
    "italy": "Southern Europe",      "spain": "Southern Europe",
    "portugal": "Southern Europe",   "greece": "Southern Europe",
    "malta": "Southern Europe",      "cyprus": "Southern Europe",
    "andorra": "Southern Europe",    "san marino": "Southern Europe",
    "poland": "Eastern Europe",      "czech republic": "Eastern Europe",
    "czechia": "Eastern Europe",     "hungary": "Eastern Europe",
    "romania": "Eastern Europe",     "bulgaria": "Eastern Europe",
    "croatia": "Eastern Europe",     "serbia": "Eastern Europe",
    "slovakia": "Eastern Europe",    "slovenia": "Eastern Europe",
    "estonia": "Eastern Europe",     "latvia": "Eastern Europe",
    "lithuania": "Eastern Europe",   "ukraine": "Eastern Europe",
    "russia": "Eastern Europe",      "georgia": "Eastern Europe",
    "turkey": "Middle East",
    "united states": "North America", "canada": "North America",
    "china": "East Asia",            "japan": "East Asia",
    "south korea": "East Asia",      "taiwan": "East Asia",
    "india": "South/SE Asia",        "australia": "Oceania",
}
_EU_REGIONS = frozenset({
    "Western Europe", "Northern Europe", "Southern Europe", "Eastern Europe"
})

# ── Institution normalisation helpers ─────────────────────────────────────────
_IRCCS_RE      = re.compile(r",?\s+IRCCS\b", re.IGNORECASE)
_TRAILING_COM  = re.compile(r"\s*,\s*$")
_MED_UNIV_RE   = re.compile(r"^(.+?)\s+Medical\s+University$", re.IGNORECASE)

def _normalize_name(name: str) -> str:
    n = _IRCCS_RE.sub("", name).strip()
    n = _TRAILING_COM.sub("", n).strip()
    m = _MED_UNIV_RE.match(n)
    if m:
        n = f"Medical University of {m.group(1).strip()}"
    n = re.sub(r"\s+-\s+", "-", n)
    n = re.sub(r"\bCentre\b", "Center", n, flags=re.IGNORECASE)
    return n

_GENERIC_HOSP = frozenset({
    "university hospital", "general hospital", "medical center", "medical centre",
    "heart center", "cardiovascular center", "university heart center",
    "university heart centre", "university medical center",
})
_CORP  = frozenset({"gmbh", "ag", "inc", "llc", "ltd", "s.a.", "sa", "plc", "bv", "nv"})
_STOPW = frozenset({"the", "and", "for", "of", "in", "at", "on", "a", "an", "de", "du"})
_DEPT_START_RE = re.compile(
    r"^(?:medizinische\s+klinik|klinik\s+(?:f[üu]r|der|des)|"
    r"university\s+clinic\s+of|clinical\s+research\s+(?:centre|center)|"
    r"department\s+of|interventional\s+and)",
    re.IGNORECASE,
)

def _is_meaningful_hosp(name: str) -> bool:
    """True if name looks like a standalone hospital (not a sub-unit or generic term)."""
    nl = name.lower().strip()
    if nl in _GENERIC_HOSP:
        return False
    if _DEPT_START_RE.match(nl):
        return False
    # Skip compound names like "X Hospital and University of Y"
    if re.search(r"\bhospital\b.+\band\b.+\buniversit|\buniversit.+\band\b.+\bhospital", nl):
        return False
    words = re.split(r"[\s,.\-]+", nl)
    if any(w in _CORP for w in words):
        return False
    mw = [w for w in words if len(w) > 2 and w not in _STOPW]
    return len(mw) >= 3

def _is_meaningful_univ(name: str) -> bool:
    """True if name looks like a standalone university (threshold = 2 meaningful words)."""
    nl = name.lower().strip()
    if _DEPT_START_RE.match(nl):
        return False
    words = re.split(r"[\s,.\-]+", nl)
    if any(w in _CORP for w in words):
        return False
    mw = [w for w in words if len(w) > 2 and w not in _STOPW]
    return len(mw) >= 2  # 2-word universities like "University of Bern" are valid


# ── Utilities ─────────────────────────────────────────────────────────────────

def detect_country(aff: str) -> str:
    if not aff:
        return "unknown"
    al = aff.lower().strip().rstrip(".")
    for c in sorted(COUNTRY_REGION, key=len, reverse=True):
        if c in al:
            return c
    if al.endswith("usa") or " usa" in al:
        return "united states"
    return "unknown"


def parse_authors(s: str | None) -> list[dict]:
    if not s:
        return []
    try:
        return json.loads(s)
    except Exception:
        return []


def get_aff(author: dict) -> str:
    aff = author.get("affiliations") or author.get("affiliation") or ""
    if isinstance(aff, list):
        return aff[0] if aff else ""
    return str(aff) if aff else ""


def is_bern_hosp_name(name: str) -> bool:
    """Return True if institution name refers to Inselspital / Bern University Hospital."""
    nl = _normalize_name(name).lower()
    return nl in BERN_HOSP_ALIASES or (
        "bern" in nl and ("hospital" in nl or "klinik" in nl)
    )


def is_bern_univ_name(name: str) -> bool:
    """Return True if institution name refers to the University of Bern."""
    nl = _normalize_name(name).lower()
    return (
        "bern" in nl
        and "universit" in nl
        and "hospital" not in nl
        and "klinikum" not in nl
        and "spital" not in nl
    )


def _wrap_label(s: str, width: int = 40) -> str:
    """Word-wrap a long institution name for horizontal bar chart y-labels."""
    if len(s) <= width:
        return s
    for i in range(width, max(width - 12, 0), -1):
        if i < len(s) and s[i] == " ":
            return s[:i] + "\n" + s[i + 1:]
    return s[:width] + "…"


# ── Data loading ──────────────────────────────────────────────────────────────

def build_dataset() -> tuple[list[dict], Counter, Counter]:
    """
    Returns:
        rows   : list of dicts for EU papers with Bern-specific tags
        eu_hosps_cnt : Counter of EU hospital names across all EU papers (for rank)
        eu_univs_cnt : Counter of EU university names across all EU papers (for rank)
    """
    print("Loading EU papers from parquet…")
    papers = pl.read_parquet(
        PARQUET, columns=["pmid", "year", "authors", "is_european"],
    )
    eu_papers = papers.filter(pl.col("is_european").eq(True))
    print(f"  {len(eu_papers):,} EU papers")

    print("Loading extractions…")
    conn = sqlite3.connect(EXTRACTIONS_DB)
    ext_rows = conn.execute(
        "SELECT pmid, extracted_json FROM extractions "
        "WHERE schema_name = ? AND run_id = ? AND extracted_json IS NOT NULL",
        (SCHEMA, RUN_ID),
    ).fetchall()
    conn.close()
    ext_map: dict[str, dict] = {}
    for pmid, ejson in ext_rows:
        try:
            d = json.loads(ejson)
            if isinstance(d, dict):
                ext_map[pmid] = d
        except Exception:
            pass
    print(f"  {len(ext_map):,} extractions loaded")

    rows_out: list[dict] = []
    eu_hosps_cnt: Counter = Counter()
    eu_univs_cnt: Counter = Counter()
    n_bern = 0

    print("Processing author affiliations…")
    for row in eu_papers.to_dicts():
        pmid = row["pmid"]
        if pmid not in ext_map:
            continue
        year = row.get("year")
        if not year:
            continue
        ext = ext_map[pmid]

        authors = parse_authors(row.get("authors"))
        all_affs = [get_aff(a) for a in authors]
        first_aff = all_affs[0] if all_affs else ""
        last_aff  = all_affs[-1] if len(all_affs) > 1 else first_aff

        # Raw-regex Bern detection
        is_ins   = any(_INS_RE.search(a)    for a in all_affs if a)
        is_ubern = any(_UBERN_RE.search(a)  for a in all_affs if a)
        has_bern_ch = any(
            _BERN_CH_RE.search(a) and "switzerland" in a.lower()
            for a in all_affs if a
        )
        is_bern = is_ins or is_ubern or has_bern_ch

        # First/last author Bern affiliation
        is_ins_first  = bool(_INS_RE.search(first_aff))   if first_aff else False
        is_ubern_first = bool(_UBERN_RE.search(first_aff)) if first_aff else False
        is_ins_last   = bool(_INS_RE.search(last_aff))    if last_aff  else False
        is_ubern_last = bool(_UBERN_RE.search(last_aff))  if last_aff  else False

        if is_bern:
            n_bern += 1

        # EU institution sets for rank computation
        hosps_this_paper: set[str] = set()
        univs_this_paper: set[str] = set()
        for author in authors:
            aff = get_aff(author)
            if not aff:
                continue
            p = _parse_aff(aff)
            seg0 = aff.split(";")[0]
            ctry = detect_country(seg0)
            if COUNTRY_REGION.get(ctry, "X") in _EU_REGIONS:
                if p.hospital:
                    n = _normalize_name(p.hospital)
                    if _is_meaningful_hosp(n):
                        hosps_this_paper.add(n)
                if p.university:
                    n = _normalize_name(p.university)
                    if _is_meaningful_univ(n):
                        univs_this_paper.add(n)
        for h in hosps_this_paper:
            eu_hosps_cnt[h] += 1
        for u in univs_this_paper:
            eu_univs_cnt[u] += 1

        if not is_bern:
            continue  # skip non-Bern papers from row list

        rows_out.append({
            "pmid":          pmid,
            "year":          int(year),
            "study_type":    ext.get("study_type", ""),
            "first_spec":    ext.get("first_author_specialty", "Unclear"),
            "is_ins":        is_ins,
            "is_ubern":      is_ubern,
            "is_ins_first":  is_ins_first,
            "is_ubern_first": is_ubern_first,
            "is_ins_last":   is_ins_last,
            "is_ubern_last": is_ubern_last,
        })

    print(f"  {n_bern} Bern-affiliated papers")
    return rows_out, eu_hosps_cnt, eu_univs_cnt


# ── Figure ────────────────────────────────────────────────────────────────────

def make_figure(rows: list[dict], eu_hosps_cnt: Counter, eu_univs_cnt: Counter) -> None:
    # ── Summary counts ────────────────────────────────────────────────────────
    n_total  = len(rows)
    n_ins    = sum(1 for r in rows if r["is_ins"])
    n_ubern  = sum(1 for r in rows if r["is_ubern"])
    n_both   = sum(1 for r in rows if r["is_ins"] and r["is_ubern"])
    n_ins_first  = sum(1 for r in rows if r["is_ins_first"])
    n_ubern_first = sum(1 for r in rows if r["is_ubern_first"])
    n_ins_last   = sum(1 for r in rows if r["is_ins_last"])
    n_ubern_last = sum(1 for r in rows if r["is_ubern_last"])

    # EU rank for Inselspital (combine all Bern hospital name variants)
    bern_hosp_total = sum(
        cnt for name, cnt in eu_hosps_cnt.items() if is_bern_hosp_name(name)
    )
    bern_univ_total = sum(
        cnt for name, cnt in eu_univs_cnt.items() if is_bern_univ_name(name)
    )
    # Compute ranks
    hosp_ranked = eu_hosps_cnt.most_common()
    univ_ranked = eu_univs_cnt.most_common()

    # Merge all Bern hospital variants into one entry for ranking
    merged_hosps: Counter = Counter()
    bern_hosp_key = "Inselspital (Bern University Hospital)"
    for name, cnt in hosp_ranked:
        if is_bern_hosp_name(name):
            merged_hosps[bern_hosp_key] += cnt
        else:
            merged_hosps[name] += cnt

    merged_univs: Counter = Counter()
    bern_univ_key = "University of Bern"
    for name, cnt in univ_ranked:
        if is_bern_univ_name(name):
            merged_univs[bern_univ_key] += cnt
        else:
            merged_univs[name] += cnt

    hosp_sorted = merged_hosps.most_common()
    univ_sorted = merged_univs.most_common()

    # Ranks
    ins_rank = next(
        (i + 1 for i, (n, _) in enumerate(hosp_sorted) if n == bern_hosp_key), None
    )
    ubern_rank = next(
        (i + 1 for i, (n, _) in enumerate(univ_sorted) if n == bern_univ_key), None
    )

    print(f"\n--- Bern summary ---")
    print(f"  Total Bern papers : {n_total}")
    print(f"  Inselspital       : {n_ins} (first: {n_ins_first}, last: {n_ins_last})")
    print(f"  University of Bern: {n_ubern} (first: {n_ubern_first}, last: {n_ubern_last})")
    print(f"  Both in same paper: {n_both}")
    print(f"  EU hospital rank  : #{ins_rank}  (merged count: {bern_hosp_total})")
    print(f"  EU university rank: #{ubern_rank} (merged count: {bern_univ_total})")

    # ── Layout ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(18, 15))
    gs = gridspec.GridSpec(
        3, 3,
        figure=fig,
        height_ratios=[2.2, 4.5, 3.0],
        hspace=0.52,
        wspace=0.40,
    )
    ax_time  = fig.add_subplot(gs[0, :])     # top: full-width timeline
    ax_hosp  = fig.add_subplot(gs[1, :2])    # middle-left: EU hospital rank
    ax_ent   = fig.add_subplot(gs[1, 2])     # middle-right: entity breakdown
    ax_study = fig.add_subplot(gs[2, 0])     # bottom-left: study type donut
    ax_spec  = fig.add_subplot(gs[2, 1])     # bottom-centre: specialty donut
    ax_univ  = fig.add_subplot(gs[2, 2])     # bottom-right: EU university rank context

    # ── Panel A: Timeline ─────────────────────────────────────────────────────
    year_ins   = Counter(r["year"] for r in rows if r["is_ins"])
    year_ubern = Counter(r["year"] for r in rows if r["is_ubern"])
    year_total = Counter(r["year"] for r in rows)
    all_years  = list(range(2001, 2027))

    yt = [year_total.get(y, 0) for y in all_years]
    yi = [year_ins.get(y,   0) for y in all_years]
    yu = [year_ubern.get(y, 0) for y in all_years]

    bar_w = 0.75
    bars = ax_time.bar(all_years, yt, width=bar_w, color=_lighten(C_BERN, 0.65),
                       edgecolor="none", label=f"Total Bern (n={n_total})")
    ax_time.plot(all_years, yi, color=C_INS,   lw=2, marker="o", ms=4,
                 label=f"Inselspital (n={n_ins})")
    ax_time.plot(all_years, yu, color=C_UBERN, lw=2, marker="s", ms=4,
                 label=f"University of Bern (n={n_ubern})")

    ax_time.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax_time.set_xlim(2000.5, 2026.5)
    ax_time.set_ylabel("Papers per year")
    ax_time.set_xlabel("Year")
    ax_time.set_title(
        f"Annual {PROJECT_NAME} Publications from Bern Institutions (2001–2026)",
        fontsize=12, fontweight="bold",
    )
    ax_time.yaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=6))
    ax_time.legend(loc="upper left", fontsize=9)
    ax_time.spines["top"].set_visible(False)
    ax_time.spines["right"].set_visible(False)

    # ── Panel B: EU Hospital Rank ─────────────────────────────────────────────
    # Show top-15 EU hospitals; highlight Bern
    top_n_hosp = 15
    top_hosps = hosp_sorted[:top_n_hosp]
    top_hosps_rev = list(reversed(top_hosps))

    y_pos = list(range(len(top_hosps_rev)))
    for i, (name, cnt) in enumerate(top_hosps_rev):
        is_bern_bar = (name == bern_hosp_key)
        color = C_INS if is_bern_bar else C_OTHER
        alpha = 1.0 if is_bern_bar else 0.55
        ax_hosp.barh(i, cnt, height=0.72, color=color, alpha=alpha, edgecolor="none")
        if is_bern_bar:
            ax_hosp.text(cnt + 1, i, f" {cnt}", va="center", ha="left",
                         fontsize=8, fontweight="bold", color=C_INS)
        else:
            ax_hosp.text(cnt + 1, i, f" {cnt}", va="center", ha="left", fontsize=7.5)

    rank_labels = []
    for name, cnt in top_hosps_rev:
        rank_in_full = next(
            (i + 1 for i, (n, _) in enumerate(hosp_sorted) if n == name), "?"
        )
        short = _wrap_label(name, 38)
        if name == bern_hosp_key:
            short = f"→ {short}"
        rank_labels.append(f"#{rank_in_full}  {short}")

    ax_hosp.set_yticks(y_pos)
    ax_hosp.set_yticklabels(rank_labels, fontsize=7.8)
    ax_hosp.set_xlabel("Papers (any EU-affiliated author)")
    ax_hosp.set_title(
        f"European Hospital Rank — {PROJECT_NAME}\n"
        f"(Inselspital: #{ins_rank} of {len(hosp_sorted):,})",
        fontsize=10, fontweight="bold",
    )
    ax_hosp.spines["top"].set_visible(False)
    ax_hosp.spines["right"].set_visible(False)

    # Highlight the Inselspital bar with a subtle background stripe
    bern_y = next(
        (i for i, (n, _) in enumerate(top_hosps_rev) if n == bern_hosp_key), None
    )
    if bern_y is not None:
        ax_hosp.axhspan(bern_y - 0.45, bern_y + 0.45,
                        color=C_INS, alpha=0.07, zorder=0)

    # ── Panel C: Entity breakdown (Inselspital vs U Bern by role) ────────────
    categories = ["Any author", "First author", "Last author"]
    ins_vals   = [n_ins,   n_ins_first,   n_ins_last]
    ubern_vals = [n_ubern, n_ubern_first, n_ubern_last]

    x = np.arange(len(categories))
    w = 0.35
    ax_ent.bar(x - w / 2, ins_vals,   width=w, color=C_INS,   label="Inselspital",       edgecolor="white")
    ax_ent.bar(x + w / 2, ubern_vals, width=w, color=C_UBERN, label="University of Bern", edgecolor="white")

    for xi, v in zip(x - w / 2, ins_vals):
        ax_ent.text(xi, v + 0.3, str(v), ha="center", va="bottom", fontsize=9, fontweight="bold", color=C_INS)
    for xi, v in zip(x + w / 2, ubern_vals):
        ax_ent.text(xi, v + 0.3, str(v), ha="center", va="bottom", fontsize=9, fontweight="bold", color=C_UBERN)

    ax_ent.set_xticks(x)
    ax_ent.set_xticklabels(categories, fontsize=9)
    ax_ent.set_ylabel("Papers")
    ax_ent.set_title("Inselspital vs University of Bern\nby Author Role", fontsize=10, fontweight="bold")
    ax_ent.legend(fontsize=8)
    ax_ent.spines["top"].set_visible(False)
    ax_ent.spines["right"].set_visible(False)
    ax_ent.yaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=6))

    # Overlap annotation
    ax_ent.annotate(
        f"Both in same paper: {n_both}",
        xy=(0.5, 0.94), xycoords="axes fraction",
        ha="center", fontsize=8, color=C_BOTH,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor=C_BOTH, alpha=0.8),
    )

    # ── Panel D: Study type donut ─────────────────────────────────────────────
    st_order = [
        "OriginalResearch", "CaseReportOrSeries", "Review",
        "SystematicReviewOrMetaAnalysis", "Other",
    ]
    st_labels = {
        "OriginalResearch":               "Original\nResearch",
        "CaseReportOrSeries":             "Case Report\n/ Series",
        "Review":                         "Review",
        "SystematicReviewOrMetaAnalysis": "SR / MA",
        "Other":                          "Other",
    }
    st_cnt = Counter(r["study_type"] for r in rows)
    sizes  = [st_cnt.get(k, 0) for k in st_order]
    colors = [STUDY_C[k] for k in st_order]

    wedges, texts, auto = ax_study.pie(
        sizes, colors=colors,
        autopct=lambda p: f"{p:.0f}%" if p > 3 else "",
        startangle=90,
        wedgeprops={"width": 0.58, "edgecolor": "white", "linewidth": 1.5},
    )
    for at in auto:
        at.set_fontsize(8)
    ax_study.legend(
        handles=[mpatches.Patch(color=STUDY_C[k], label=f"{st_labels[k].replace(chr(10),' ')} ({st_cnt.get(k,0)})")
                 for k in st_order if st_cnt.get(k, 0) > 0],
        loc="lower center", bbox_to_anchor=(0.5, -0.22),
        ncol=1, fontsize=7.5, framealpha=0.7,
    )
    ax_study.set_title("Study Type — Bern Papers", fontsize=10, fontweight="bold", pad=6)

    # ── Panel E: First-author specialty donut ─────────────────────────────────
    spec_cnt = Counter(r["first_spec"] for r in rows)
    sp_order = ["Cardiology", "Radiology", "Unclear"]
    sp_sizes = [spec_cnt.get(s, 0) for s in sp_order]
    sp_colors = [SPEC_C[s] for s in sp_order]

    ax_spec.pie(
        sp_sizes, colors=sp_colors,
        autopct=lambda p: f"{p:.0f}%" if p > 3 else "",
        startangle=90,
        wedgeprops={"width": 0.58, "edgecolor": "white", "linewidth": 1.5},
    )
    ax_spec.legend(
        handles=[mpatches.Patch(color=SPEC_C[s], label=f"{s} ({spec_cnt.get(s,0)})")
                 for s in sp_order if spec_cnt.get(s, 0) > 0],
        loc="lower center", bbox_to_anchor=(0.5, -0.22),
        ncol=1, fontsize=7.5, framealpha=0.7,
    )
    ax_spec.set_title("First-Author Specialty — Bern", fontsize=10, fontweight="bold", pad=6)

    # ── Panel F: EU University rank context (focused top-15 + U Bern) ─────────
    # Show top-10 + U Bern (even if outside top-10), clearly marking U Bern
    top_n_univ = 10
    top_univs = univ_sorted[:top_n_univ]

    # Ensure U Bern is included
    ubern_in_top = any(n == bern_univ_key for n, _ in top_univs)
    if not ubern_in_top and ubern_rank is not None:
        # Add U Bern with a gap indicator
        top_univs_display = list(top_univs) + [("…", 0), (bern_univ_key, bern_univ_total)]
    else:
        top_univs_display = list(top_univs)

    top_univs_rev = list(reversed(top_univs_display))
    y_pos_u = list(range(len(top_univs_rev)))

    for i, (name, cnt) in enumerate(top_univs_rev):
        if name == "…":
            ax_univ.axhline(y=i, color="#CCCCCC", lw=0.8, ls="--")
            ax_univ.text(0, i, " … (ranks omitted) …",
                         va="center", ha="left", fontsize=7, color="#888")
            continue
        is_bern_bar = (name == bern_univ_key)
        color = C_UBERN if is_bern_bar else C_OTHER
        alpha = 1.0 if is_bern_bar else 0.55
        ax_univ.barh(i, cnt, height=0.72, color=color, alpha=alpha, edgecolor="none")
        if is_bern_bar:
            ax_univ.text(cnt + 0.5, i, f" {cnt}", va="center", ha="left",
                         fontsize=8, fontweight="bold", color=C_UBERN)
        else:
            ax_univ.text(cnt + 0.5, i, f" {cnt}", va="center", ha="left", fontsize=7.5)

    univ_rank_labels = []
    shown_ranks: dict[str, int] = {}
    for name, _ in univ_sorted:
        r = len(shown_ranks) + 1
        shown_ranks[name] = r

    for name, cnt in top_univs_rev:
        if name == "…":
            univ_rank_labels.append("")
            continue
        rank_n = shown_ranks.get(name, "?")
        short  = _wrap_label(name, 30)
        if name == bern_univ_key:
            short = f"→ {short}"
        univ_rank_labels.append(f"#{rank_n}  {short}")

    ax_univ.set_yticks(y_pos_u)
    ax_univ.set_yticklabels(univ_rank_labels, fontsize=7.5)
    ax_univ.set_xlabel("Papers (any EU-affiliated author)")
    ax_univ.set_title(
        f"European University Rank — {PROJECT_NAME}\n"
        f"(U Bern: #{ubern_rank} of {len(univ_sorted):,})",
        fontsize=10, fontweight="bold",
    )
    ax_univ.spines["top"].set_visible(False)
    ax_univ.spines["right"].set_visible(False)

    # ── Suptitle ──────────────────────────────────────────────────────────────
    fig.suptitle(
        f"Inselspital & University of Bern — {PROJECT_NAME} Publications Profile (2001–2026)\n"
        f"Inselspital: {n_ins} papers (EU rank #{ins_rank})  ·  "
        f"University of Bern: {n_ubern} papers (EU rank #{ubern_rank})  ·  "
        f"Total Bern: {n_total} papers",
        fontsize=12, fontweight="bold", y=1.01,
    )

    out = FIG_DIR / "bern_stat.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close("all")
    print(f"\n[OK] Saved -> {out}")


# ── Colour helper (local, no import from generate_analysis) ──────────────────

def _lighten(hex_color: str, factor: float = 0.5) -> str:
    import matplotlib.colors as mcolors
    rgb = mcolors.to_rgb(hex_color)
    return mcolors.to_hex(tuple(1.0 - (1.0 - c) * factor for c in rgb))


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    plt.rcParams.update({
        "figure.dpi": 150,
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.20,
        "grid.linestyle": "--",
        "legend.framealpha": 0.85,
        "legend.fontsize": 9,
    })
    rows, eu_hosps_cnt, eu_univs_cnt = build_dataset()
    make_figure(rows, eu_hosps_cnt, eu_univs_cnt)
