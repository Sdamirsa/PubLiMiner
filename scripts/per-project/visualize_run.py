"""Visualize a PubLiMiner extraction run snapshot.

Before generating charts, consults an LLM panel (3 rounds) to recommend
the top 5 most informative visualizations and two format alternatives for each.
Falls back to built-in defaults if OPENROUTER_API_KEY is not set.

Usage:
    uv run python scripts/per-project/visualize_run.py --snapshot output/cardiac_mri/snapshot_*.json
    uv run python scripts/per-project/visualize_run.py --snapshot output/cardiac_mri/snapshot_*.json --no-panel

Output:
    {snapshot_dir}/viz/  — one PNG per chart (10 charts = 5 topics x 2 formats)
"""

from __future__ import annotations

import argparse
import json
import os
import textwrap
from pathlib import Path

import httpx
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── Scientific color palette ────────────────────────────────────────────────────
# Specialty: Radiology=blue, Cardiology=red, Unclear=grey (matches clinical intuition)
SPECIALTY_COLORS = {
    "Radiology":   "#2166AC",
    "Cardiology":  "#D6604D",
    "Unclear":     "#878787",
    "NotReported": "#CCCCCC",
    "Other":       "#FDAE61",
}

# Author position colors (for multi-series specialty charts, order: last, corresponding, first)
AUTHOR_POS_COLORS = {
    "Last":          "#1F4E79",  # dark navy
    "Corresponding": "#9B2226",  # dark crimson
    "First":         "#5B8DB8",  # medium blue
}

# Geographic
GEO_COLORS = {
    "European":     "#1B7837",
    "Non-European": "#A6D96A",
}

# Relevance
RELEVANCE_COLORS = {
    "Main":        "#2D6A4F",
    "Secondary":   "#74C69D",
    "NotRelevant": "#D9D9D9",
    "Irrelevant":  "#D9D9D9",   # keep for backward-compat with old snapshots
}

# Study type
STUDY_TYPE_COLORS = {
    "OriginalResearch":               "#2166AC",
    "CaseReportOrSeries":             "#FDAE61",
    "Review":                         "#ABD9E9",
    "SystematicReviewOrMetaAnalysis": "#D7191C",
    "Other":                          "#CCCCCC",
}

# Journal scope
SCOPE_COLORS = {
    "Cardiology": "#D6604D",
    "Radiology":  "#2166AC",
    "Mix":        "#74ADD1",
    "General":    "#AAAAAA",
    "Other":      "#CCCCCC",
}

# Sequential blue for journals/agencies
SEQ_BLUES = ["#08519C", "#2171B5", "#4292C6", "#6BAED6", "#9ECAE1",
             "#C6DBEF", "#DEEBF7", "#F7FBFF", "#EFF3FF", "#BDD7E7"]

# Okabe-Ito (colorblind-safe) for year trends
YEAR_COLORS = {"total": "#0072B2", "european": "#009E73"}

PLT_PARAMS = {
    "font.family":       "sans-serif",
    "font.size":         11,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "figure.dpi":        150,
    "axes.titlesize":    12,
    "axes.titleweight":  "bold",
}
plt.rcParams.update(PLT_PARAMS)


# ── Human-readable label mapping ───────────────────────────────────────────────
PRETTY_LABELS: dict[str, str] = {
    "OriginalResearch":               "Original Research",
    "CaseReportOrSeries":             "Case Report / Series",
    "Review":                         "Review",
    "SystematicReviewOrMetaAnalysis": "Systematic Review / Meta-analysis",
    "Other":                          "Other",
    "Main":                           "Main",
    "Secondary":                      "Secondary",
    "NotRelevant":                    "Not Relevant",
    "Irrelevant":                     "Not Relevant",   # old enum name → same display
    "NotReported":                    "Not Reported",
    "Radiology":                      "Radiology",
    "Cardiology":                     "Cardiology",
    "Unclear":                        "Unclear",
}


def _pretty(key: str) -> str:
    return PRETTY_LABELS.get(key, key)


# ── Specialty normalization ─────────────────────────────────────────────────────
_RADIOLOGY_ALIASES = {
    "diagnostic and interventional radiology", "medical imaging",
    "ultrasound", "computed tomography", "radiodiagnostics",
    "diagnostic imaging", "radiological sciences",
    "radiology and radiation oncology",        # compound dept names seen in LLM output
    "radiology and nuclear medicine",
    "interventional radiology",
    "nuclear medicine and radiology",
}
_CARDIOLOGY_ALIASES = {
    "cardiovascular medicine", "cardiovascular surgery", "cardiac surgery",
    "structural interventional cardiology", "cardiovascular center",
    "heart center", "cardiac center",
}


def normalize_specialty(raw: str) -> str:
    key = raw.strip().lower()
    if key == "radiology" or key in _RADIOLOGY_ALIASES:
        return "Radiology"
    if key == "cardiology" or key in _CARDIOLOGY_ALIASES:
        return "Cardiology"
    if key in ("unclear", ""):
        return "Unclear"
    if key == "notreported":
        return "NotReported"
    return "Other"


def normalize_distribution(dist: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for raw, count in dist.items():
        norm = normalize_specialty(raw)
        out[norm] = out.get(norm, 0) + count
    return {k: v for k, v in out.items() if v > 0}


# ── Relevance normalization ─────────────────────────────────────────────────────
_CARDIAC_KEYWORDS = {
    "cardiac mri", "cardiac magnetic resonance", "cmr", "cardiovascular magnetic resonance",
    "cardiac ct", "coronary ct", "ccta", "ctca", "cardiac computed tomography",
}


def normalize_relevance(raw: str) -> str:
    """Map LLM free-text relevance responses to Main / Secondary / NotRelevant.

    Also accepts old 'Irrelevant' values from runs before the enum rename.
    NotRelevant is preferred over Irrelevant: the LLM rarely returns 'not-relevant'
    as a free-text answer, whereas 'relevant' (a false positive) was common with
    the old enum and normalised as Main.
    """
    stripped = raw.strip()
    key = stripped.lower()

    if key == "main":
        return "Main"
    if key == "secondary":
        return "Secondary"
    # Accept both old (Irrelevant) and new (NotRelevant) enum names
    if key in ("notrelevant", "not_relevant", "not relevant",
               "irrelevant", "false", "0", "no"):
        return "NotRelevant"
    # Common non-compliant but clearly positive signals.
    # "true" / "1" appear when the LLM returns a boolean instead of a string
    # (e.g. relevance: true in JSON); treat as Main (most charitable positive read).
    if key in ("relevant", "true", "yes", "1"):
        return "Main"
    if key.startswith("not relevant") or key.startswith("notrelevant") or key.startswith("irrelevant"):
        return "NotRelevant"
    if key.startswith("relevant") or key.startswith("yes"):
        return "Main"
    # Free-text containing cardiac imaging keywords → Main
    if any(kw in key for kw in _CARDIAC_KEYWORDS):
        return "Main"
    # Long free-text (model explaining relevance) → Main
    if len(stripped) > 30:
        return "Main"
    return "NotRelevant"


def normalize_relevance_dist(dist: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {"Main": 0, "Secondary": 0, "NotRelevant": 0}
    for raw, count in dist.items():
        norm = normalize_relevance(raw)
        out[norm] = out.get(norm, 0) + count
    return {k: v for k, v in out.items() if v > 0}


# ── Study type normalization ────────────────────────────────────────────────────
def normalize_study_type(raw: str) -> str:
    """Map LLM study type responses to canonical enum values."""
    key = raw.strip().lower()
    # Strip non-breaking spaces and collapse whitespace
    key = " ".join(key.split())

    if key == "originalresearch":
        return "OriginalResearch"
    if key == "casereportorseries":
        return "CaseReportOrSeries"
    if key == "review":
        return "Review"
    if key == "systematicreviewormetaanalysis":
        return "SystematicReviewOrMetaAnalysis"
    if key == "other":
        return "Other"

    # Common non-compliant variants
    if key in ("prospective", "retrospective", "cohort", "observational study",
               "cross-sectional", "rct", "registry", "clinical study",
               "original", "original research", "observational"):
        return "OriginalResearch"
    if "systematic" in key or "meta-analysis" in key or "meta analysis" in key:
        return "SystematicReviewOrMetaAnalysis"
    if "case report" in key or "case series" in key:
        return "CaseReportOrSeries"
    if "review" in key:
        return "Review"
    return "Other"


def normalize_study_type_dist(dist: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for raw, count in dist.items():
        norm = normalize_study_type(raw)
        out[norm] = out.get(norm, 0) + count
    return {k: v for k, v in out.items() if v > 0}


# ── Data prep ───────────────────────────────────────────────────────────────────

def prepare_data(snapshot: dict) -> dict:
    cs = snapshot["corpus_stats"]
    rs = snapshot["run_summary"]
    fd = snapshot["field_distributions"]

    specialty_first       = normalize_distribution(fd.get("first_author_specialty", {}))
    specialty_last        = normalize_distribution(fd.get("last_author_specialty", {}))
    specialty_corr        = normalize_distribution(fd.get("corresponding_author_specialty", {}))

    relevance  = normalize_relevance_dist({k: v for k, v in fd.get("relevance", {}).items() if v > 0})
    study_type = normalize_study_type_dist({k: v for k, v in fd.get("study_type", {}).items() if v > 0})

    years_all = {int(k): v for k, v in cs["papers_by_year"].items() if int(k) >= 2023}

    # European-by-year from extractions (partial sample)
    years_eu: dict[int, int] = {}
    for e in snapshot["extractions"]:
        y = e.get("year")
        if y and int(y) >= 2023 and e.get("is_european"):
            years_eu[int(y)] = years_eu.get(int(y), 0) + 1

    journals = [(j["name"], j["count"]) for j in cs["top_journals"][:10]]
    journals = [(n[:50] + "…" if len(n) > 50 else n, c) for n, c in journals]

    funding_agencies = [(a["name"], a["count"]) for a in cs.get("top_funding_agencies", [])[:10]]

    journal_scope = cs.get("journal_scope", {})

    # Scoped journal lists for charts 7 & 8
    def _trunc(name: str) -> str:
        return name[:50] + "…" if len(name) > 50 else name

    # All study types — scope comes from snapshot (added by export_run_json >= this version)
    journals_scoped: list[tuple[str, int, str]] = [
        (_trunc(j["name"]), j["count"], j.get("scope", "Other"))
        for j in cs["top_journals"][:10]
    ]

    # Original research only — count from extractions list
    from collections import Counter as _Counter
    orig_counts: _Counter = _Counter()
    for e in snapshot["extractions"]:
        if normalize_study_type(e.get("study_type", "") or "") == "OriginalResearch":
            jt = (e.get("journal_title") or "").strip()
            if jt:
                orig_counts[jt] += 1
    # Build scope lookup from snapshot top_journals
    scope_lookup = {j["name"].lower(): j.get("scope", "Other") for j in cs["top_journals"]}
    journals_orig: list[tuple[str, int, str]] = [
        (_trunc(name), cnt, scope_lookup.get(name.lower(), "Other"))
        for name, cnt in orig_counts.most_common(10)
    ]

    # Merged: top-10 by all-paper count, with orig research count alongside
    orig_lookup = {name: cnt for name, cnt, _scope in journals_orig}
    journals_merged: list[tuple[str, int, int, str]] = [
        (name, all_cnt, orig_lookup.get(name, 0), scope)
        for name, all_cnt, scope in journals_scoped
    ]

    n_european     = cs["european_papers"]
    n_non_european = cs["total_papers"] - n_european

    return {
        "specialty_first":  specialty_first,
        "specialty_last":   specialty_last,
        "specialty_corr":   specialty_corr,
        "relevance":        relevance,
        "study_type":       study_type,
        "years_all":        years_all,
        "years_eu":         years_eu,
        "journals":         journals,
        "journals_scoped":  journals_scoped,
        "journals_orig":    journals_orig,
        "journals_merged":  journals_merged,
        "funding_agencies": funding_agencies,
        "journal_scope":    journal_scope,
        "n_european":       n_european,
        "n_non_european":   n_non_european,
        "n_extracted":      rs["n_extracted"],
        "n_success":        rs["n_success"],
        "n_failed":         rs["n_failed"],
        "n_repaired":       rs["n_repaired"],
        "coverage_pct":     rs["coverage_of_total_pct"],
        "project_name":     snapshot["search_config"]["project_name"],
        "start_date":       snapshot["search_config"]["start_date"],
        "end_date":         snapshot["search_config"]["end_date"],
    }


def data_summary_text(data: dict, snapshot: dict) -> str:
    cs = snapshot["corpus_stats"]
    f  = data["specialty_first"]
    l  = data["specialty_last"]
    c  = data["specialty_corr"]
    total_f = max(sum(f.values()), 1)
    total_l = max(sum(l.values()), 1)
    total_c = max(sum(c.values()), 1)

    rel   = data["relevance"]
    total_rel = max(sum(rel.values()), 1)
    st    = data["study_type"]
    total_st  = max(sum(st.values()), 1)

    journals_top5 = ", ".join(f"{n} ({c})" for n, c in data["journals"][:5])
    agencies_top5 = ", ".join(f"{n} ({c})" for n, c in data["funding_agencies"][:5])
    years_str = ", ".join(f"{y}: {v:,}" for y, v in sorted(data["years_all"].items()))

    specialty_block = ""
    for label, dist, total in [("Last author", l, total_l),
                                ("Corresponding", c, total_c),
                                ("First author", f, total_f)]:
        specialty_block += f"\n  {label}:\n"
        specialty_block += "\n".join(
            f"    {k}: {v:,} ({100*v//total}%)"
            for k, v in sorted(dist.items(), key=lambda x: -x[1])
        )

    relevance_block = "\n".join(
        f"  {k}: {v:,} ({100*v//total_rel}%)"
        for k, v in sorted(rel.items(), key=lambda x: -x[1])
    )
    study_type_block = "\n".join(
        f"  {k}: {v:,} ({100*v//total_st}%)"
        for k, v in sorted(st.items(), key=lambda x: -x[1])
    )

    return textwrap.dedent(f"""
        PROJECT: {data['project_name']}
        RESEARCH QUESTION: Specialty gap (Radiology vs Cardiology) in European cardiac imaging literature.
        Key dimensions: relevance, study design, author specialty (last / corresponding / first), funding.

        CORPUS (post-dedup, {data['start_date']} to {data['end_date']}):
          Total papers      : {cs['total_papers']:,}
          European papers   : {cs['european_papers']:,} ({cs['european_pct']}%)
          Non-European      : {cs['total_papers'] - cs['european_papers']:,}
          Language (English): {cs['languages'].get('eng', 0):,}

        PAPERS BY YEAR: {years_str}

        TOP 5 JOURNALS: {journals_top5}
        TOP 5 FUNDING AGENCIES: {agencies_top5}

        EXTRACTION STATUS (PRELIMINARY — {data['coverage_pct']}% of corpus processed):
          Successfully classified : {data['n_success']:,}
          Failed / unclassifiable : {data['n_failed']:,} ({100*data['n_failed']//max(data['n_extracted'],1)}% failure rate)

        RELEVANCE (n={sum(rel.values())}):
        {relevance_block}

        STUDY TYPE (n={sum(st.values())}):
        {study_type_block}

        SPECIALTY CLASSIFICATION (normalized, n={sum(f.values())}):
        {specialty_block}

        AVAILABLE DATA SOURCES:
          specialty_last        — last_author_specialty distribution
          specialty_corr        — corresponding_author_specialty distribution
          specialty_first       — first_author_specialty distribution
          specialty_compare     — last / corresponding / first side by side
          relevance             — Main / Secondary / Irrelevant distribution
          study_type            — OriginalResearch / Review / etc. distribution
          papers_by_year        — total papers per year
          european_split        — European vs Non-European total counts
          top_journals          — top 10 journals by paper count
          funding_agencies      — top 10 funding agencies by paper count

        AVAILABLE CHART TYPES: pie, donut, bar, grouped_bar, stacked_bar,
                               horizontal_bar, lollipop, line, area
    """).strip()


# ── LLM panel consultation (3 rounds) ──────────────────────────────────────────

PANEL_SYSTEM = (
    "You are a scientific data visualization expert advising biomedical researchers. "
    "You recommend chart types that maximize clarity and scientific credibility. "
    "You always consider the audience (clinicians and radiologists at a conference), "
    "the preliminary nature of the data, and colorblind-safe design."
)

ROUND1_PROMPT = """Based on the dataset summary below, recommend the top 5 most impactful
visualizations for a scientific presentation. For each, state:
- What message it delivers
- Why this chart type is the right choice
- What data source to use

Dataset summary:
{summary}"""

ROUND2_PROMPT = """Review your recommendations critically. Consider:
1. The data is PRELIMINARY (only {coverage_pct}% of papers extracted) — charts must make this clear.
2. The core research question is the Radiology vs Cardiology specialty gap — ensure at least 2 charts
   directly address this, using LAST/CORRESPONDING/FIRST author stratification.
3. Relevance and study type are new fields that help understand the corpus quality.
4. The geographic filter (European papers) is central — at least 1 chart should show this.
5. Funding agency breakdown adds a useful policy dimension.
6. Are any of your 5 charts redundant or replaceable with something more informative?

Revise your top 5 list with these considerations."""

ROUND3_PROMPT = """Finalize your top 5 visualizations. For each, provide exactly two alternative
chart formats (e.g. donut vs bar, line vs area).

Return ONLY a valid JSON array — no prose, no markdown fences — with this exact schema:
[
  {{
    "rank": 1,
    "title": "Short chart title",
    "message": "One sentence: what this chart shows and why it matters",
    "data_source": "specialty_last|specialty_corr|specialty_first|specialty_compare|relevance|study_type|papers_by_year|european_split|top_journals|funding_agencies|journal_scope",
    "format_a": {{"type": "pie|donut|bar|grouped_bar|stacked_bar|horizontal_bar|lollipop|line|area", "label": "Short label"}},
    "format_b": {{"type": "pie|donut|bar|grouped_bar|stacked_bar|horizontal_bar|lollipop|line|area", "label": "Short label"}}
  }}
]"""

DEFAULT_PANEL_SPECS = [
    {
        "rank": 1,
        "title": "Author Specialty Comparison (Last / Corresponding / First)",
        "message": "Cardiology dominates across all author positions; Radiology is consistently underrepresented.",
        "data_source": "specialty_compare",
        "format_a": {"type": "grouped_bar", "label": "Grouped bar"},
        "format_b": {"type": "stacked_bar", "label": "Stacked bar (%)"},
        "caption": (
            "Specialty of Last, Corresponding, and First Authors in European Cardiac Imaging\n"
            "Specialty was classified by AI (GPT model) from the first listed affiliation of each "
            "author in European publications retrieved from PubMed.\n"
            "Each group compares the number of cardiologists, radiologists, and unclear-specialty "
            "authors across three author positions: last, corresponding, and first.\n"
            "Cardiology consistently dominates all three author positions, while radiologists "
            "represent a minority — quantifying the specialty gap this study aims to characterise."
        ),
    },
    {
        "rank": 2,
        "title": "Study Relevance Distribution",
        "message": "Shows what fraction of retrieved papers are truly about cardiac MRI vs tangential mentions.",
        "data_source": "relevance",
        "format_a": {"type": "donut", "label": "Donut chart"},
        "format_b": {"type": "bar",   "label": "Bar chart"},
        "caption": (
            "Relevance of Retrieved Papers to the Target Cardiac Imaging Modality\n"
            "Each paper was classified by AI as Main, Secondary, or Not Relevant based on title "
            "and abstract content extracted from PubMed XML.\n"
            "Main indicates the modality is the primary focus; Secondary indicates it is one of "
            "several methods used; Not Relevant indicates keyword overlap without substantive use.\n"
            "A substantial fraction of retrieved papers require relevance filtering before "
            "specialty analysis, validating AI-based triage as an essential pipeline step."
        ),
    },
    {
        "rank": 3,
        "title": "Study Type Breakdown",
        "message": "Original research dominates; reviews and systematic reviews provide synthesis.",
        "data_source": "study_type",
        "format_a": {"type": "donut",           "label": "Donut chart"},
        "format_b": {"type": "horizontal_bar",  "label": "Horizontal bar"},
        "caption": (
            "Study Design Distribution Among European Cardiac Imaging Publications\n"
            "Study design was classified by AI from paper titles, abstracts, and PubMed "
            "publication type metadata provided in the extraction prompt.\n"
            "Categories follow standard research taxonomy: original research includes prospective "
            "and retrospective studies; case reports cover individually documented patient cases.\n"
            "Original research dominates the corpus; the small proportion of systematic reviews "
            "reflects the still-maturing evidence base in European cardiac imaging."
        ),
    },
    {
        "rank": 4,
        "title": "European vs Non-European Papers",
        "message": "European affiliation share contextualises the geographic scope of this analysis.",
        "data_source": "european_split",
        "format_a": {"type": "donut", "label": "Donut chart"},
        "format_b": {"type": "bar",   "label": "Bar chart"},
        "caption": (
            "Geographic Scope: European vs Non-European Authorship in the Retrieved Corpus\n"
            "Papers were labelled European if at least one author's affiliation contained a "
            "European country name, capital city, or major academic centre keyword.\n"
            "The chart shows what fraction of all papers retrieved by the PubMed search query "
            "had at least one European author affiliation.\n"
            "Roughly 40–45% of retrieved papers have European authorship, establishing the "
            "denominator for the specialty-gap analysis."
        ),
    },
    {
        "rank": 5,
        "title": "Top Funding Agencies",
        "message": "Funding landscape reveals which bodies drive cardiac MRI research in Europe.",
        "data_source": "funding_agencies",
        "format_a": {"type": "horizontal_bar", "label": "Horizontal bar"},
        "format_b": {"type": "lollipop",       "label": "Lollipop chart"},
        "caption": (
            "Top 10 Funding Agencies Supporting European Cardiac Imaging Research\n"
            "Grant acknowledgement data was extracted from PubMed XML grant fields without "
            "LLM processing; each agency is counted once per paper regardless of grant count.\n"
            "Each bar shows the number of European papers that acknowledged a specific funding "
            "agency; one paper may acknowledge multiple agencies.\n"
            "NIH and major European national research councils are the leading funders, "
            "reflecting a transatlantic funding landscape for cardiac imaging research."
        ),
    },
    {
        "rank": 6,
        "title": "Journal Scope Distribution",
        "message": "Cardiology-scoped journals dominate the field; Radiology journals play a minor role.",
        "data_source": "journal_scope",
        "format_a": {"type": "donut",        "label": "Donut chart"},
        "format_b": {"type": "grouped_bar",  "label": "Total vs European bar"},
        "caption": (
            "Distribution of Cardiac Imaging Papers Across Journal Scopes\n"
            "Journal scope (Cardiology, Radiology, Mix, General) was assigned using a curated "
            "registry covering the top 20 journals by paper count in this corpus.\n"
            "Each category represents papers published in journals primarily associated with "
            "that clinical specialty scope, shown as total and European-authored counts.\n"
            "Cardiology-scoped journals account for the largest share, while radiology journals "
            "hold a minority — mirroring the authorship specialty gap."
        ),
    },
    {
        "rank": 7,
        "title": "Top 10 Journals — All Papers vs Original Research",
        "message": "Directly compares total paper count with original research count per journal.",
        "data_source": "top_journals_merged",
        "format_a": {"type": "merged_grouped_bar", "label": "Merged grouped bar"},
        "format_b": {},
        "caption": (
            "Top 10 Journals: All Papers vs Original Research (European Authors, colour = journal scope)\n"
            "Journal names extracted from PubMed XML; scope classification applied from the curated "
            "registry; Original Research subset filtered by AI study-type classification.\n"
            "Each journal has two bars: the upper (solid) bar shows all European papers; the lower "
            "(faded) bar shows only those classified as original research; colours indicate journal scope.\n"
            "The gap between the two bars reflects non-original content (reviews, case reports, "
            "editorials), and the colour pattern confirms cardiology-scoped journals dominate both views."
        ),
    },
]

JOURNAL_SCOPE_SPEC = DEFAULT_PANEL_SPECS[5]  # rank 6; always appended regardless of LLM panel


def run_panel(summary: str, coverage_pct: float, api_key: str) -> list[dict]:
    """Three-round LLM panel. Returns list of 5 viz specs."""
    base_url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization":      f"Bearer {api_key}",
        "HTTP-Referer":        "https://github.com/sdamirsa/PubLiMiner",
        "X-OpenRouter-Title":  "PubLiMiner",
        "Content-Type":        "application/json",
    }
    messages = [{"role": "system", "content": PANEL_SYSTEM}]

    def chat(user_msg: str) -> str:
        messages.append({"role": "user", "content": user_msg})
        payload = {
            "model": "anthropic/claude-haiku-4-5",
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 1200,
        }
        resp = httpx.post(base_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        messages.append({"role": "assistant", "content": content})
        return content

    print("\n[Panel] Round 1 -- initial recommendations...")
    chat(ROUND1_PROMPT.format(summary=summary))

    print("[Panel] Round 2 -- critique and refinement...")
    chat(ROUND2_PROMPT.format(coverage_pct=f"{coverage_pct:.1f}"))

    print("[Panel] Round 3 -- finalize with format alternatives...")
    raw_json = chat(ROUND3_PROMPT)

    try:
        raw_json = raw_json.strip()
        if raw_json.startswith("```"):
            raw_json = raw_json.split("```")[1]
            if raw_json.startswith("json"):
                raw_json = raw_json[4:]
        specs = json.loads(raw_json)
        if isinstance(specs, list) and len(specs) >= 1:
            print(f"[Panel] Received {len(specs)} visualization specs.")
            return specs[:5]
    except Exception as e:
        print(f"[Panel] JSON parse failed ({e}); using built-in defaults.")

    return DEFAULT_PANEL_SPECS


# ── Chart generators ─────────────────────────────────────────────────────────────

def _annotation(ax: plt.Axes, text: str, fontsize: int = 8) -> None:
    ax.text(0, -0.12, text, transform=ax.transAxes,
            fontsize=fontsize, color="#555555", ha="left")



def chart_donut(data: dict, title: str, colors_map: dict, path: Path,
                note: str = "", caption: str = "") -> None:
    labels = list(data.keys())
    values = [data[k] for k in labels]
    colors = [colors_map.get(k, "#AAAAAA") for k in labels]
    total  = sum(values)

    fig, ax = plt.subplots(figsize=(6, 5))
    wedges, texts, autotexts = ax.pie(
        values, labels=None, colors=colors,
        autopct=lambda p: f"{p:.1f}%\n({int(round(p*total/100)):,})",
        startangle=90, pctdistance=0.78,
        wedgeprops=dict(width=0.48, edgecolor="white", linewidth=1.5),
    )
    for t in autotexts:
        t.set_fontsize(8)
    ax.legend(
        wedges, [f"{_pretty(k)}  ({data[k]:,})" for k in labels],
        loc="lower center", bbox_to_anchor=(0.5, -0.15),
        ncol=min(len(labels), 3), fontsize=9, frameon=False,
    )
    ax.set_title(title, pad=14)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_pie(data: dict, title: str, colors_map: dict, path: Path,
              note: str = "", caption: str = "") -> None:
    labels = list(data.keys())
    values = [data[k] for k in labels]
    colors = [colors_map.get(k, "#AAAAAA") for k in labels]

    fig, ax = plt.subplots(figsize=(6, 5))
    wedges, texts, autotexts = ax.pie(
        values, labels=None, colors=colors, autopct="%1.1f%%",
        startangle=90,
        wedgeprops=dict(edgecolor="white", linewidth=1.5),
    )
    for t in autotexts:
        t.set_fontsize(9)
    ax.legend(
        wedges, [f"{_pretty(k)}  ({data[k]:,})" for k in labels],
        loc="lower center", bbox_to_anchor=(0.5, -0.15),
        ncol=min(len(labels), 3), fontsize=9, frameon=False,
    )
    ax.set_title(title, pad=14)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_bar(data: dict, title: str, colors_map: dict, path: Path,
              ylabel: str = "Papers", note: str = "", caption: str = "") -> None:
    labels = list(data.keys())
    display_labels = [_pretty(k) for k in labels]
    values = [data[k] for k in labels]
    colors = [colors_map.get(k, "#4292C6") for k in labels]

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(display_labels, values, color=colors, edgecolor="white", linewidth=0.8)
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                f"{v:,}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_grouped_bar(
    groups: dict[str, dict[str, int]],
    title: str, colors_map: dict, path: Path,
    ylabel: str = "Papers", note: str = "", caption: str = "",
) -> None:
    """groups: {group_label: {series_label: value}}"""
    group_labels  = list(groups.keys())
    series_labels = list(next(iter(groups.values())).keys())
    n_groups  = len(group_labels)
    n_series  = len(series_labels)
    x         = np.arange(n_groups)
    width     = 0.8 / n_series
    colors    = [colors_map.get(s, "#AAAAAA") for s in series_labels]

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, (sl, color) in enumerate(zip(series_labels, colors)):
        vals   = [groups[gl].get(sl, 0) for gl in group_labels]
        offset = (i - n_series / 2 + 0.5) * width
        bars   = ax.bar(x + offset, vals, width, label=sl, color=color,
                        edgecolor="white", linewidth=0.6)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                        f"{v:,}", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(x)
    ax.set_xticklabels(group_labels, rotation=15, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_stacked_bar_pct(
    groups: dict[str, dict[str, int]],
    title: str, colors_map: dict, path: Path,
    note: str = "", caption: str = "",
) -> None:
    """Percent-stacked bar. groups: {x_label: {category: count}}"""
    x_labels   = list(groups.keys())
    categories = list(next(iter(groups.values())).keys())
    x      = np.arange(len(x_labels))
    colors = [colors_map.get(c, "#AAAAAA") for c in categories]

    fig, ax = plt.subplots(figsize=(9, 5))
    bottoms = np.zeros(len(x_labels))
    for cat, color in zip(categories, colors):
        totals = np.array([sum(groups[g].values()) for g in x_labels], dtype=float)
        vals   = np.array([groups[g].get(cat, 0) for g in x_labels], dtype=float)
        pcts   = np.where(totals > 0, vals / totals * 100, 0)
        ax.bar(x, pcts, bottom=bottoms, label=cat, color=color,
               edgecolor="white", linewidth=0.6)
        for xi, (bot, pct) in enumerate(zip(bottoms, pcts)):
            if pct > 5:
                ax.text(xi, bot + pct/2, f"{pct:.0f}%",
                        ha="center", va="center", fontsize=8, color="white", fontweight="bold")
        bottoms += pcts

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=15, ha="right")
    ax.set_ylim(0, 100)
    ax.set_ylabel("Percentage (%)")
    ax.set_title(title)
    ax.legend(loc="upper right", frameon=False)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_horizontal_bar(
    items: list[tuple[str, int]], title: str, color: str, path: Path,
    xlabel: str = "Papers", note: str = "", pretty: bool = False,
    caption: str = "",
) -> None:
    labels = [(_pretty(i[0]) if pretty else i[0]) for i in reversed(items)]
    values = [i[1] for i in reversed(items)]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    bars = ax.barh(labels, values, color=color, edgecolor="white", linewidth=0.6)
    for bar, v in zip(bars, values):
        ax.text(bar.get_width() + max(values)*0.005, bar.get_y() + bar.get_height()/2,
                f"{v:,}", va="center", fontsize=8)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_lollipop(
    items: list[tuple[str, int]], title: str, color: str, path: Path,
    xlabel: str = "Papers", note: str = "", caption: str = "",
) -> None:
    labels = [i[0] for i in reversed(items)]
    values = [i[1] for i in reversed(items)]
    y = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.hlines(y, 0, values, colors=color, linewidth=1.5, alpha=0.6)
    ax.scatter(values, y, color=color, s=60, zorder=5)
    for yi, v in zip(y, values):
        ax.text(v + max(values)*0.005, yi, f"{v:,}", va="center", fontsize=8)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_line(data: dict, title: str, colors_map: dict, path: Path,
               ylabel: str = "Papers", note: str = "", caption: str = "") -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    for label, series in data.items():
        xs = sorted(series.keys())
        ys = [series[x] for x in xs]
        ax.plot(xs, ys, marker="o", label=label,
                color=colors_map.get(label, "#0072B2"), linewidth=2, markersize=6)
        for x, y in zip(xs, ys):
            ax.text(x, y + max(max(s.values()) for s in data.values())*0.015,
                    f"{y:,}", ha="center", fontsize=8)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_area(data: dict, title: str, colors_map: dict, path: Path,
               ylabel: str = "Papers", note: str = "", caption: str = "") -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    for label, series in data.items():
        xs = sorted(series.keys())
        ys = [series[x] for x in xs]
        ax.fill_between(xs, ys, alpha=0.3, color=colors_map.get(label, "#0072B2"))
        ax.plot(xs, ys, marker="o", label=label,
                color=colors_map.get(label, "#0072B2"), linewidth=2, markersize=6)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_horizontal_bar_scoped(
    items: list[tuple[str, int, str]], title: str, path: Path,
    xlabel: str = "Papers", note: str = "", caption: str = "",
) -> None:
    """Horizontal bar chart colored by journal scope. items: (name, count, scope_key)."""
    rev = list(reversed(items))
    labels = [i[0] for i in rev]
    values = [i[1] for i in rev]
    colors = [SCOPE_COLORS.get(i[2], "#AAAAAA") for i in rev]

    fig, ax = plt.subplots(figsize=(10, 5.5))
    bars = ax.barh(labels, values, color=colors, edgecolor="white", linewidth=0.6)
    for bar, v in zip(bars, values):
        ax.text(bar.get_width() + max(values) * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{v:,}", va="center", fontsize=8)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))

    scope_present = sorted(set(i[2] for i in items),
                           key=lambda s: -sum(x[1] for x in items if x[2] == s))
    handles = [mpatches.Patch(facecolor=SCOPE_COLORS.get(s, "#AAAAAA"), label=s)
               for s in scope_present]
    ax.legend(handles=handles, loc="lower right", frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_lollipop_scoped(
    items: list[tuple[str, int, str]], title: str, path: Path,
    xlabel: str = "Papers", note: str = "", caption: str = "",
) -> None:
    """Lollipop chart colored by journal scope. items: (name, count, scope_key)."""
    rev = list(reversed(items))
    labels = [i[0] for i in rev]
    values = [i[1] for i in rev]
    colors = [SCOPE_COLORS.get(i[2], "#AAAAAA") for i in rev]
    y = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for yi, v, c in zip(y, values, colors):
        ax.hlines(yi, 0, v, colors=c, linewidth=1.5, alpha=0.5)
        ax.scatter([v], [yi], color=c, s=70, zorder=5)
        ax.text(v + max(values) * 0.005, yi, f"{v:,}", va="center", fontsize=8)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))

    scope_present = sorted(set(i[2] for i in items),
                           key=lambda s: -sum(x[1] for x in items if x[2] == s))
    handles = [mpatches.Patch(facecolor=SCOPE_COLORS.get(s, "#AAAAAA"), label=s)
               for s in scope_present]
    ax.legend(handles=handles, loc="lower right", frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def chart_journals_merged_scoped(
    items: list[tuple[str, int, int, str]], title: str, path: Path,
    note: str = "", caption: str = "",
) -> None:
    """Grouped horizontal bar: two bars per journal (All Papers vs Original Research).

    items: [(journal_name, all_count, orig_count, scope_key), ...] highest→lowest by all_count.
    """
    rev  = list(reversed(items))
    names       = [i[0] for i in rev]
    all_counts  = [i[1] for i in rev]
    orig_counts = [i[2] for i in rev]
    scope_keys  = [i[3] for i in rev]

    n      = len(rev)
    y      = np.arange(n)
    bar_h  = 0.38
    max_v  = max(max(all_counts, default=1), 1)

    fig, ax = plt.subplots(figsize=(10.5, max(5.5, n * 0.72)))

    for i in range(n):
        color = SCOPE_COLORS.get(scope_keys[i], "#AAAAAA")
        # All Papers — full opacity, top slot
        ax.barh(y[i] + bar_h / 2, all_counts[i], bar_h,
                color=color, edgecolor="white", linewidth=0.5)
        ax.text(all_counts[i] + max_v * 0.008, y[i] + bar_h / 2,
                f"{all_counts[i]:,}", va="center", fontsize=8)
        # Original Research — 45 % opacity, bottom slot
        ax.barh(y[i] - bar_h / 2, orig_counts[i], bar_h,
                color=color, alpha=0.45, edgecolor="white", linewidth=0.5)
        ax.text(orig_counts[i] + max_v * 0.008, y[i] - bar_h / 2,
                f"{orig_counts[i]:,}", va="center", fontsize=8)

    ax.set_yticks(y)
    ax.set_yticklabels(names)
    ax.set_xlabel("Papers")
    ax.set_title(title)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))

    # Legend: series (solid vs faded)
    series_handles = [
        mpatches.Patch(facecolor="#888888", alpha=1.0,  label="All Papers"),
        mpatches.Patch(facecolor="#888888", alpha=0.45, label="Original Research"),
    ]
    # Legend: scope colours
    scope_present = sorted(set(scope_keys), key=lambda s: -sum(all_counts[i] for i, sk in enumerate(scope_keys) if sk == s))
    scope_handles = [mpatches.Patch(facecolor=SCOPE_COLORS.get(s, "#AAAAAA"), label=s)
                     for s in scope_present]

    leg1 = ax.legend(handles=series_handles, loc="lower right", frameon=False, fontsize=8,
                     title="Series", title_fontsize=8)
    ax.add_artist(leg1)
    ax.legend(handles=scope_handles, loc="center right", frameon=False, fontsize=8,
              title="Journal scope", title_fontsize=8, bbox_to_anchor=(1.0, 0.25))

    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


# ── Chart dispatch ────────────────────────────────────────────────────────────────

def _build_specialty_compare_groups(data: dict) -> tuple[dict, dict]:
    """Build groups dict for specialty_compare charts.

    Returns (groups_by_specialty, color_map) where groups are keyed by specialty
    and series are ordered: Last, Corresponding, First.
    """
    all_cats = sorted(
        set(data["specialty_last"]) | set(data["specialty_corr"]) | set(data["specialty_first"]),
        key=lambda k: -data["specialty_last"].get(k, 0),
    )
    groups = {
        cat: {
            "Last":          data["specialty_last"].get(cat, 0),
            "Corresponding": data["specialty_corr"].get(cat, 0),
            "First":         data["specialty_first"].get(cat, 0),
        }
        for cat in all_cats
    }
    return groups, AUTHOR_POS_COLORS


def render_spec(spec: dict, data: dict, out_dir: Path, snapshot: dict) -> list[Path]:
    """Generate both format_a and format_b for a single spec. Returns saved paths."""
    rank    = spec["rank"]
    title   = spec["title"]
    source  = spec["data_source"]
    caption = spec.get("caption", "")
    saved: list[Path] = []

    def _path(fmt_label: str) -> Path:
        safe = fmt_label.lower().replace(" ", "_").replace("/", "_")
        return out_dir / f"{rank:02d}_{safe}.png"

    for fmt in (spec.get("format_a", {}), spec.get("format_b", {})):
        if not fmt:
            continue
        chart_type = fmt.get("type", "bar")
        label      = fmt.get("label", chart_type)
        path       = _path(label)

        # ── single-author-position specialty ──────────────────────────────────
        if source in ("specialty_first", "specialty_last", "specialty_corr"):
            pos_map = {
                "specialty_first": ("specialty_first", "first author affiliation"),
                "specialty_last":  ("specialty_last",  "last author affiliation"),
                "specialty_corr":  ("specialty_corr",  "corresponding author affiliation"),
            }
            key, note_suffix = pos_map[source]
            d = data[key]
            if chart_type == "donut":
                chart_donut(d, title, SPECIALTY_COLORS, path,
                            note=f"Source: {note_suffix}", caption=caption)
            else:
                chart_bar(d, title, SPECIALTY_COLORS, path,
                          note=f"Source: {note_suffix}", caption=caption)

        # ── three-position specialty comparison (last / corresponding / first) ─
        elif source == "specialty_compare":
            groups, color_map = _build_specialty_compare_groups(data)
            if chart_type == "stacked_bar":
                pos_groups = {
                    "Last":          data["specialty_last"],
                    "Corresponding": data["specialty_corr"],
                    "First":         data["specialty_first"],
                }
                chart_stacked_bar_pct(pos_groups, title, SPECIALTY_COLORS, path,
                                      note="Last / Corresponding / First author",
                                      caption=caption)
            else:
                chart_grouped_bar(groups, title, color_map, path,
                                  note="Last / Corresponding / First author",
                                  caption=caption)

        # ── relevance ─────────────────────────────────────────────────────────
        elif source == "relevance":
            d = data["relevance"]
            if not d:
                print("  [skip] no relevance data available yet")
                continue
            if chart_type in ("donut", "pie"):
                (chart_donut if chart_type == "donut" else chart_pie)(
                    d, title, RELEVANCE_COLORS, path, caption=caption)
            else:
                chart_bar(d, title, RELEVANCE_COLORS, path, caption=caption)

        # ── study type ────────────────────────────────────────────────────────
        elif source == "study_type":
            d = data["study_type"]
            if not d:
                print("  [skip] no study_type data available yet")
                continue
            if chart_type in ("donut", "pie"):
                (chart_donut if chart_type == "donut" else chart_pie)(
                    d, title, STUDY_TYPE_COLORS, path, caption=caption)
            elif chart_type == "horizontal_bar":
                chart_horizontal_bar(
                    sorted(d.items(), key=lambda x: -x[1]), title, "#4292C6", path,
                    pretty=True, caption=caption)
            else:
                chart_bar(d, title, STUDY_TYPE_COLORS, path, caption=caption)

        # ── geographic split ──────────────────────────────────────────────────
        elif source == "european_split":
            d = {"European": data["n_european"], "Non-European": data["n_non_european"]}
            if chart_type in ("donut", "pie"):
                (chart_donut if chart_type == "donut" else chart_pie)(
                    d, title, GEO_COLORS, path,
                    note="Based on keyword matching in author affiliations",
                    caption=caption)
            else:
                chart_bar(d, title, GEO_COLORS, path,
                          note="Based on keyword matching in author affiliations",
                          caption=caption)

        # ── publication trend ─────────────────────────────────────────────────
        elif source == "papers_by_year":
            series = {"All papers": data["years_all"]}
            if data["years_eu"]:
                series["European (sample)"] = data["years_eu"]
            color_map = {"All papers": YEAR_COLORS["total"],
                         "European (sample)": YEAR_COLORS["european"]}
            note = "2026 is a partial year"
            if chart_type == "line":
                chart_line(series, title, color_map, path, note=note, caption=caption)
            elif chart_type == "area":
                chart_area(series, title, color_map, path, note=note, caption=caption)
            else:
                chart_bar(data["years_all"], title,
                          {str(k): YEAR_COLORS["total"] for k in data["years_all"]},
                          path, note=note, caption=caption)

        # ── top journals (legacy data_source key) ─────────────────────────────
        elif source == "top_journals":
            if chart_type == "horizontal_bar":
                chart_horizontal_bar(data["journals"], title, "#2171B5", path,
                                     note="Top 10 by paper count (all languages)",
                                     caption=caption)
            else:
                chart_lollipop(data["journals"], title, "#2171B5", path,
                               note="Top 10 by paper count (all languages)",
                               caption=caption)

        # ── top journals scoped — all study types ─────────────────────────────
        elif source == "top_journals_all":
            items = data["journals_scoped"]
            if not items:
                print("  [skip] no scoped journal data")
                continue
            if chart_type == "horizontal_bar":
                chart_horizontal_bar_scoped(items, title, path,
                                            note="Colour = journal scope; top 10 European papers",
                                            caption=caption)
            else:
                chart_lollipop_scoped(items, title, path,
                                      note="Colour = journal scope; top 10 European papers",
                                      caption=caption)

        # ── top journals scoped — original research only ──────────────────────
        elif source == "top_journals_orig":
            items = data["journals_orig"]
            if not items:
                print("  [skip] no original-research journal data")
                continue
            if chart_type == "horizontal_bar":
                chart_horizontal_bar_scoped(items, title, path,
                                            note="Colour = journal scope; OriginalResearch papers only",
                                            caption=caption)
            else:
                chart_lollipop_scoped(items, title, path,
                                      note="Colour = journal scope; OriginalResearch papers only",
                                      caption=caption)

        # ── top journals merged (all papers vs original research) ────────────
        elif source == "top_journals_merged":
            items_m = data["journals_merged"]
            if not items_m:
                print("  [skip] no merged journal data")
                continue
            chart_journals_merged_scoped(
                items_m, title, path,
                note="Upper bar = All Papers  ·  Lower bar = Original Research  ·  Colour = journal scope",
                caption=caption,
            )

        # ── funding agencies ──────────────────────────────────────────────────
        elif source == "funding_agencies":
            items = data["funding_agencies"]
            if not items:
                print("  [skip] no funding agency data in snapshot")
                continue
            if chart_type == "horizontal_bar":
                chart_horizontal_bar(items, title, "#5B8DB8", path,
                                     note="Papers with this agency in grant list",
                                     caption=caption)
            else:
                chart_lollipop(items, title, "#5B8DB8", path,
                               note="Papers with this agency in grant list",
                               caption=caption)

        # ── journal scope ─────────────────────────────────────────────────────
        elif source == "journal_scope":
            d = data["journal_scope"]
            if not d:
                print("  [skip] no journal_scope data (fill journal_registry.csv first)")
                continue
            ordered = dict(sorted(d.items(), key=lambda x: -x[1]["total"]))
            if chart_type in ("donut", "pie"):
                totals = {scope: vals["total"] for scope, vals in ordered.items()}
                (chart_donut if chart_type == "donut" else chart_pie)(
                    totals, title, SCOPE_COLORS, path,
                    note="Coverage: top-20 journals only; remaining papers not shown",
                    caption=caption)
            elif chart_type == "grouped_bar":
                groups = {
                    scope: {"Total": vals["total"], "European": vals["european"]}
                    for scope, vals in ordered.items()
                }
                chart_grouped_bar(groups, title,
                                  {"Total": "#4292C6", "European": "#1B7837"}, path,
                                  note="Coverage: top-20 journals only",
                                  caption=caption)
            else:
                totals = {scope: vals["total"] for scope, vals in ordered.items()}
                chart_bar(totals, title, SCOPE_COLORS, path,
                          note="Coverage: top-20 journals only",
                          caption=caption)

        else:
            print(f"  [skip] unknown data_source: {source!r}")
            continue

        print(f"  Saved: {path.name}")
        saved.append(path)

    return saved


# ── Main ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize a PubLiMiner extraction snapshot")
    parser.add_argument("--snapshot", required=True,
                        help="Path to snapshot JSON from export_run_json.py")
    parser.add_argument("--no-panel", action="store_true",
                        help="Skip LLM panel, use built-in default specs")
    args = parser.parse_args()

    snap_path = Path(args.snapshot)
    if not snap_path.exists():
        matches = list(snap_path.parent.glob(snap_path.name))
        if not matches:
            raise SystemExit(f"Snapshot not found: {snap_path}")
        snap_path = sorted(matches)[-1]
        print(f"Resolved snapshot: {snap_path.name}")

    snapshot = json.loads(snap_path.read_text(encoding="utf-8"))
    data = prepare_data(snapshot)

    print(f"\nProject : {data['project_name']}")
    print(f"Corpus  : {snapshot['corpus_stats']['total_papers']:,} papers  |  "
          f"{data['n_european']:,} European ({snapshot['corpus_stats']['european_pct']}%)")
    print(f"Extracted: {data['n_success']:,} success / {data['n_extracted']:,} processed "
          f"({data['coverage_pct']:.1f}% coverage)")

    out_dir = snap_path.parent / "viz"
    out_dir.mkdir(exist_ok=True)

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if args.no_panel or not api_key:
        reason = "--no-panel flag" if args.no_panel else "OPENROUTER_API_KEY not set"
        print(f"\n[Panel] Skipped ({reason}); using built-in default specs.")
        specs = DEFAULT_PANEL_SPECS
    else:
        summary = data_summary_text(data, snapshot)
        try:
            specs = run_panel(summary, data["coverage_pct"], api_key)
        except Exception as e:
            print(f"[Panel] Failed ({e}); falling back to built-in defaults.")
            specs = DEFAULT_PANEL_SPECS

    # Always append fixed specs that the LLM panel does not control
    fixed_sources = {"journal_scope", "top_journals_merged"}
    for fixed_spec in DEFAULT_PANEL_SPECS:
        if fixed_spec["data_source"] in fixed_sources:
            if not any(s.get("data_source") == fixed_spec["data_source"] for s in specs):
                specs.append(fixed_spec)

    print(f"\nGenerating up to {len(specs) * 2} charts in {out_dir}/\n")
    all_saved: list[Path] = []
    for spec in specs:
        print(f"  [{spec['rank']}] {spec['title']}")
        print(f"       {spec.get('message', '')}")
        saved = render_spec(spec, data, out_dir, snapshot)
        all_saved.extend(saved)

    print(f"\nDone -- {len(all_saved)} charts saved to {out_dir}/")
    print("\nFiles:")
    for p in sorted(all_saved):
        size_kb = p.stat().st_size / 1024
        print(f"  {p.name:<50} {size_kb:.0f} KB")


if __name__ == "__main__":
    main()
