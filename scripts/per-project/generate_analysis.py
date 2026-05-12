#!/usr/bin/env python3
"""
Cardiac CT — Comprehensive Publication Analysis (2001–2026)
============================================================
Expert panel-designed visualizations (May 2026 panel session).

Visualization groups:
  T1–T6  Temporal: volume, specialty by year (first/last/corresponding), study types
  G1–G4  Geographic: world map/top countries, regional trends, decade comparison
  I1–I4  Institutional: top-20 global/European, first/last/any breakdown
  J1–J2  Journals: top-20 all papers, European-only subset
  X1–X2  Cross-section: specialty×study-type heatmap, 6-panel dashboard

Filter applied: all extracted papers (relevance filter removed — labels deemed unreliable)
Region scheme: ESC-aligned (N.America / W.Europe / N.Europe / S.Europe /
               E.Europe / East Asia / Middle East / S-SE Asia / Oceania /
               Latin America / Africa)
"""
from __future__ import annotations

import json
import re
import sqlite3
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
JOURNAL_REG    = BASE_DIR / "journal_registry.csv"
FIG_DIR        = BASE_DIR / "analysis" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# ── Typed affiliation parser ──────────────────────────────────────────────────
import sys as _sys
_src_root = BASE_DIR.parent.parent / "src"
if str(_src_root) not in _sys.path:
    _sys.path.insert(0, str(_src_root))
from publiminer.utils.affiliation_parser import parse_affiliation as _parse_aff

SCHEMA, RUN_ID = _load_run_meta(BASE_DIR)
PROJECT_NAME   = " ".join(
    w.upper() if w.lower() in ("ct", "mri", "pet") else w.title()
    for w in BASE_DIR.name.replace("_", " ").split()
)

# ── Color palettes ────────────────────────────────────────────────────────────
SPEC_C = {
    "Cardiology":    "#E74C3C",
    "Radiology":     "#2980B9",
    "Unclear":       "#95A5A6",
    "NotReported":   "#BDC3C7",
    "Mixed":         "#8E44AD",
}

REL_C = {"Main": "#27AE60", "Secondary": "#F39C12"}

STUDY_C = {
    "OriginalResearch":               "#2980B9",
    "CaseReportOrSeries":             "#E67E22",
    "Review":                         "#9B59B6",
    "SystematicReviewOrMetaAnalysis": "#1ABC9C",
    "Other":                          "#95A5A6",
}

REGION_C = {
    "North America":   "#1F77B4",
    "Western Europe":  "#2CA02C",
    "Northern Europe": "#17BECF",
    "Southern Europe": "#98DF8A",
    "Eastern Europe":  "#FFBB78",
    "East Asia":       "#D62728",
    "Middle East":     "#FF7F0E",
    "South/SE Asia":   "#9467BD",
    "Oceania":         "#8C564B",
    "Latin America":   "#BCBD22",
    "Africa":          "#7F7F7F",
    "Unknown":         "#DDDDDD",
}

REGION_ORDER = [
    "North America", "Western Europe", "Northern Europe", "Southern Europe",
    "Eastern Europe", "East Asia", "Middle East", "South/SE Asia",
    "Oceania", "Latin America", "Africa", "Unknown",
]

# ── Country → Region ──────────────────────────────────────────────────────────
COUNTRY_REGION: dict[str, str] = {
    # North America
    "united states": "North America", "canada": "North America",
    # Western Europe
    "germany": "Western Europe",     "france": "Western Europe",
    "netherlands": "Western Europe", "belgium": "Western Europe",
    "switzerland": "Western Europe", "austria": "Western Europe",
    "luxembourg": "Western Europe",  "liechtenstein": "Western Europe",
    # Northern Europe
    "united kingdom": "Northern Europe", "england": "Northern Europe",
    "scotland": "Northern Europe",   "wales": "Northern Europe",
    "ireland": "Northern Europe",    "sweden": "Northern Europe",
    "norway": "Northern Europe",     "denmark": "Northern Europe",
    "finland": "Northern Europe",    "iceland": "Northern Europe",
    # Southern Europe
    "italy": "Southern Europe",      "spain": "Southern Europe",
    "portugal": "Southern Europe",   "greece": "Southern Europe",
    "malta": "Southern Europe",      "cyprus": "Southern Europe",
    "san marino": "Southern Europe", "monaco": "Southern Europe",
    "andorra": "Southern Europe",
    # Eastern Europe
    "poland": "Eastern Europe",      "czech republic": "Eastern Europe",
    "czechia": "Eastern Europe",     "hungary": "Eastern Europe",
    "romania": "Eastern Europe",     "bulgaria": "Eastern Europe",
    "croatia": "Eastern Europe",     "serbia": "Eastern Europe",
    "slovakia": "Eastern Europe",    "slovenia": "Eastern Europe",
    "estonia": "Eastern Europe",     "latvia": "Eastern Europe",
    "lithuania": "Eastern Europe",   "albania": "Eastern Europe",
    "belarus": "Eastern Europe",     "bosnia": "Eastern Europe",
    "kosovo": "Eastern Europe",      "moldova": "Eastern Europe",
    "montenegro": "Eastern Europe",  "north macedonia": "Eastern Europe",
    "ukraine": "Eastern Europe",     "russia": "Eastern Europe",
    "georgia": "Eastern Europe",
    # East Asia
    "china": "East Asia",            "japan": "East Asia",
    "south korea": "East Asia",      "korea": "East Asia",
    "taiwan": "East Asia",           "hong kong": "East Asia",
    "singapore": "East Asia",
    # South / SE Asia
    "india": "South/SE Asia",        "pakistan": "South/SE Asia",
    "bangladesh": "South/SE Asia",   "thailand": "South/SE Asia",
    "malaysia": "South/SE Asia",     "indonesia": "South/SE Asia",
    "vietnam": "South/SE Asia",      "philippines": "South/SE Asia",
    "sri lanka": "South/SE Asia",    "nepal": "South/SE Asia",
    # Middle East
    "turkey": "Middle East",         "iran": "Middle East",
    "israel": "Middle East",         "saudi arabia": "Middle East",
    "jordan": "Middle East",         "lebanon": "Middle East",
    "egypt": "Middle East",          "qatar": "Middle East",
    "united arab emirates": "Middle East", "uae": "Middle East",
    "kuwait": "Middle East",         "bahrain": "Middle East",
    "oman": "Middle East",           "iraq": "Middle East",
    "syria": "Middle East",
    # Oceania
    "australia": "Oceania",          "new zealand": "Oceania",
    # Latin America
    "brazil": "Latin America",       "argentina": "Latin America",
    "mexico": "Latin America",       "chile": "Latin America",
    "colombia": "Latin America",     "peru": "Latin America",
    "venezuela": "Latin America",    "cuba": "Latin America",
    # Africa
    "south africa": "Africa",        "nigeria": "Africa",
    "kenya": "Africa",               "ghana": "Africa",
    "ethiopia": "Africa",            "morocco": "Africa",
    "tunisia": "Africa",             "algeria": "Africa",
}

COUNTRY_DISPLAY = {
    "united states": "USA", "united kingdom": "UK",
    "united arab emirates": "UAE", "south korea": "S. Korea",
    "hong kong": "Hong Kong", "new zealand": "N. Zealand",
    "south africa": "S. Africa", "czech republic": "Czech Rep.",
    "saudi arabia": "Saudi Arabia", "north macedonia": "N. Macedonia",
}

US_CITY_STATE = {
    "boston", "new york", "new york city", "san francisco", "los angeles",
    "chicago", "houston", "philadelphia", "baltimore", "cleveland",
    "pittsburgh", "detroit", "minneapolis", "st. louis", "atlanta",
    "california", "new york state", "texas", "florida", "illinois",
    "massachusetts", "ohio", "michigan", "pennsylvania", "north carolina",
    "georgia", "washington state", "maryland", "minnesota", "arizona",
    " ma,", " ny,", " ca,", " tx,", " fl,", " il,", " pa,", " oh,",
    " mi,", " ga,", " nc,", " wa,", " mn,", " az,", " ct,", " mo,",
    " nj,", " co,", " tn,", " va,", " sc,", " wi,", " in,", " ky,",
}

# ── WoS-style institution parsing constants ───────────────────────────────────
# "Organisation Enhanced" heuristic: strip country → strip city → find first
# comma-token that contains an organisational entity keyword (left-to-right).
# Ref: Waltman & van Eck (2012), Rons (2018), ROR community guidelines.

INST_KWORDS = (
    "universit",   # university, universität, università, universitat, etc.
    "hospital",
    "klinik",      # German: Klinik, Klinikum, Kliniken
    "clinic",      # clinic, clinics, clinique, clinica
    "hosp.",
    "institut",    # institute, instituto, Institut, instytut
    " center",     # (leading space avoids "center for" sub-unit prefix)
    " centre",
    "zentrum",
    "college",
    "academy",     "akademie",
    "akadem",      # akademikliniken, akademiska sjukhuset
    "foundation",  "fondation", "stiftung",
    "charite",     "charité",
    "school of med", "medical school",
    "health system", "health sciences",
    "clin. ", "clin ",   # abbreviated "clinical" in inst names
    "polyclin",    # polyclinic
    "sanatorium",  "sanitarium",
    "mumc",        # Maastricht UMC
    "erasmus mc",  "radboud",
    "irccs",       # Italian research hospitals
    "aphp",        # Assistance Publique – Hôpitaux de Paris
)

SUBUNIT_STARTS = (
    "department of", "dept. of", "dept of",
    "division of",   "div. of",  "div of",
    "section of",    "unit of",
    "laboratory of", "lab of",
    "group of",      "team of",
    "school of",     "faculty of",   # without "medicine" → sub-unit
)

SUBUNIT_ENDS = (
    " department", " dept",
    " division", " unit", " section",
    " laboratory", " lab",
    " ward", " service", " group", " team",
    " imaging",        # "cardiac imaging"
    " cardiology",     " radiology",
    " medicine",       " surgery",
    " oncology",       " neurology",
    " pediatrics",     " paediatrics",
    " psychiatry",     " anesthesia",
    " anesthesiology", " physiology",
    " sciences",       # avoids "health sciences" being blocked (handled by INST_KWORDS)
)

_STREET_WORDS = (
    "cesta", "ulica", "street", " st.", " st ", "road", " rd.",
    "avenue", " ave.", "boulevard", "blvd", "lane", " ln.",
    "strasse", "straße", "gasse", "weg", "platz", "allee",
    "rue ", "via ", "piazza", "corso",
)

def _is_address(part: str) -> bool:
    """True if the part looks like a street address (digits + street words)."""
    if not any(c.isdigit() for c in part):
        return False
    pl = part.lower()
    return (any(w in pl for w in _STREET_WORDS)
            or bool(re.search(r'\b\d{4,6}\b', part)))


# ── Utility functions ─────────────────────────────────────────────────────────

def detect_country(affiliation: str) -> str:
    if not affiliation:
        return "unknown"
    al = affiliation.lower().strip().rstrip(".")
    # US city/state shorthand
    for indicator in US_CITY_STATE:
        if indicator in al:
            return "united states"
    # Country names — longest first to avoid partial matches
    for country in sorted(COUNTRY_REGION, key=len, reverse=True):
        if country in al:
            return country
    # Final US catch: ends with " usa" or ", usa"
    if al.endswith("usa") or " usa" in al or ", usa" in al:
        return "united states"
    return "unknown"


def extract_institution(affiliation: str) -> str:
    """
    WoS Organisation-Enhanced heuristic (simplified):
      1. Take only the first affiliation if ";" separates multiple.
      2. Split by comma; filter address-like tokens and country tokens.
      3. Strip trailing short geographic tokens (city, state, zip).
      4. Left-to-right: return the first token containing an org-entity keyword
         (university, hospital, clinic, institute, center …).
      5. Fallback: return the first token that doesn't look like a pure sub-unit.
    """
    if not affiliation:
        return "Unknown"

    # Multiple affiliations in one string (e.g. from semicolon-separated records)
    aff = affiliation.split(";")[0].strip()

    parts = [p.strip().rstrip(".") for p in aff.split(",")]
    parts = [p for p in parts if len(p.strip()) > 2]
    if not parts:
        return "Unknown"
    if len(parts) == 1:
        return parts[0][:80]

    # 1. Strip country tokens (appear anywhere)
    clean = [p for p in parts if detect_country(p) == "unknown"]

    # 2. Strip address-like tokens
    clean = [p for p in clean if not _is_address(p)]

    if not clean:
        return parts[0][:80]

    # 3. Strip trailing short geographic tokens (city, state/province)
    while len(clean) > 1:
        last = clean[-1]
        has_inst_kw = any(kw in last.lower() for kw in INST_KWORDS)
        # Short + no institution keyword + no digit (zip) → likely a city
        if not has_inst_kw and len(last) <= 35 and not re.search(r'\d', last):
            clean.pop()
        else:
            break

    # 4. Return first token that contains an institution keyword
    for part in clean:
        if any(kw in part.lower() for kw in INST_KWORDS):
            return part[:80]

    # 5. Fallback: skip pure sub-unit tokens
    for part in clean:
        p_lower = part.lower().strip()
        starts_as_sub = any(p_lower.startswith(sw) for sw in SUBUNIT_STARTS)
        ends_as_sub   = any(p_lower.endswith(se.strip()) for se in SUBUNIT_ENDS)
        if not starts_as_sub and not ends_as_sub:
            return part[:80]

    return clean[0][:80]


def parse_authors(authors_json: str | None) -> list[dict]:
    if not authors_json:
        return []
    try:
        return json.loads(authors_json)
    except Exception:
        return []


def get_affiliation(author: dict) -> str:
    aff = author.get("affiliations") or author.get("affiliation") or ""
    if isinstance(aff, list):
        return aff[0] if aff else ""
    return str(aff) if aff else ""


def majority_spec(first: str | None, last: str | None, corr: str | None) -> str:
    votes = [v for v in (first, last, corr) if v and v not in ("Unclear", "NotReported")]
    if not votes:
        return "Unclear"
    c = Counter(votes)
    top_val, top_cnt = c.most_common(1)[0]
    if top_cnt >= 2:
        return top_val
    return "Mixed"


def rolling_avg(values: list[float], window: int = 5) -> tuple[np.ndarray, np.ndarray]:
    """Return (indices, smoothed_values) truncated at window/2 from each end.

    Stops window/2 steps before the last data point to avoid convolution
    boundary artefacts — as per the half-window truncation rule.
    Returns index slice so callers can align against year/x arrays.
    """
    arr = np.array(values, dtype=float)
    kernel = np.ones(window) / window
    full = np.convolve(arr, kernel, mode="same")
    half = window // 2 + 1          # ceil(window / 2): e.g. 5-yr → 3
    idx = slice(0, len(arr) - half)  # drop last `half` points
    return idx, full[idx]


def save_fig(name: str) -> None:
    plt.tight_layout()
    out = FIG_DIR / name
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close("all")
    print(f"  [OK] {name}")


def _darken(hex_color: str, factor: float = 0.65) -> str:
    """Return a darkened version of a hex colour (factor < 1 = darker)."""
    import matplotlib.colors as mcolors
    rgb = mcolors.to_rgb(hex_color)
    return mcolors.to_hex(tuple(c * factor for c in rgb))


def _lighten(hex_color: str, factor: float = 0.5) -> str:
    """Blend hex_color toward white (factor=0 → white, factor=1 → original)."""
    import matplotlib.colors as mcolors
    rgb = mcolors.to_rgb(hex_color)
    return mcolors.to_hex(tuple(1.0 - (1.0 - c) * factor for c in rgb))


_GENERIC_INST_NAMES: frozenset[str] = frozenset({
    # Institutes
    "cardiovascular center", "cardiovascular centre",
    "heart center", "heart centre",
    "cardiology center", "cardiology centre",
    "institute of cardiology", "institute of cardiovascular",
    "medical center", "medical centre",
    "research center", "research centre",
    # Hospitals
    "university hospital", "general hospital",
    "city hospital", "national hospital",
    "district hospital", "regional hospital",
    # Universities / schools
    "school of medicine", "college of medicine",
    "faculty of medicine", "medical school",
    "school of medical", "department of medicine",
    # Heart centers (are hospitals — filter from university panels; they're misclas'd by parser)
    "university heart center", "university heart centre",
    "universitäres herzzentrum", "university heart and vascular center",
})

_STOPWORDS = frozenset({"the", "and", "for", "of", "in", "at", "on", "a", "an", "de", "du"})
_EU_REGIONS = frozenset({"Western Europe", "Northern Europe", "Southern Europe", "Eastern Europe"})

# Corporate entities — should never appear as academic institutions
_CORPORATE_SUFFIXES: frozenset[str] = frozenset({
    "gmbh", "ag", "inc.", "inc", "llc", "ltd.", "ltd", "s.a.", "sa", "plc", "bv", "nv",
})

# Sub-unit department prefixes that indicate a clinical ward, not a standalone institution
_DEPT_START_RE = re.compile(
    r'^(?:'
    r'medizinische\s+klinik|klinik\s+(?:f[üu]r|der|des|an\s+der|und)|'
    r'klinikum\s+(?:der|des)\s+universit[äa]|'
    r'clinique\s+de|servicio\s+de|'
    r'i\.\s+medizinische|ii\.\s+medizinische|iii\.\s+medizinische'
    r')',
    re.IGNORECASE,
)

# Suffixes that don't distinguish institutions — strip before deduplication
_IRCCS_SUFFIX_RE = re.compile(r',?\s+IRCCS\b', re.IGNORECASE)
_TRAILING_COMMA_RE = re.compile(r'\s*,\s*$')

# "Medical University of Innsbruck" ↔ "Innsbruck Medical University"
# Canonical form: "Medical University of <City>"
_MED_UNIV_SWAP_RE = re.compile(
    r'^(.+?)\s+Medical\s+University$', re.IGNORECASE
)


def _normalize_inst_name(name: str) -> str:
    """Normalize institution name for deduplication.

    1. Strip trailing IRCCS designation (same institution in/without).
    2. Canonicalize "<City> Medical University" → "Medical University of <City>".
    3. Strip trailing commas.
    4. Normalize dash spacing: " - " → "-" (Charité - Universitätsmedizin ↔ Charité-Universitätsmedizin).
    5. Normalize British Centre → American Center spelling for deduplication.
    """
    n = _IRCCS_SUFFIX_RE.sub("", name).strip()
    n = _TRAILING_COMMA_RE.sub("", n).strip()
    # Canonicalize word-order: "Innsbruck Medical University" → "Medical University of Innsbruck"
    m = _MED_UNIV_SWAP_RE.match(n)
    if m:
        city_part = m.group(1).strip()
        n = f"Medical University of {city_part}"
    # Normalize " - " → "-" so "Charité - Universitätsmedizin Berlin" and
    # "Charité-Universitätsmedizin Berlin" collapse to the same key.
    n = re.sub(r'\s+-\s+', '-', n)
    # Normalize British "Centre" → "Center" to merge e.g. "Medical Centre" / "Medical Center".
    n = re.sub(r'\bCentre\b', 'Center', n, flags=re.IGNORECASE)
    return n


def _is_meaningful_inst(name: str) -> bool:
    """Return False for overly generic, corporate, or sub-unit institution names."""
    nl = name.lower().strip()
    if nl in _GENERIC_INST_NAMES:
        return False
    if _DEPT_START_RE.match(nl):
        return False
    all_words = re.split(r"[\s,.\-]+", nl)
    if any(w in _CORPORATE_SUFFIXES for w in all_words):
        return False
    words = [w for w in all_words if len(w) > 2 and w not in _STOPWORDS]
    return len(words) >= 3


# ── Global matplotlib style ───────────────────────────────────────────────────
plt.rcParams.update({
    "figure.dpi": 150,
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linestyle": "--",
    "legend.framealpha": 0.85,
    "legend.fontsize": 9,
})


# ── Data loading ──────────────────────────────────────────────────────────────

def load_papers() -> pl.DataFrame:
    return pl.read_parquet(
        PARQUET,
        columns=["pmid", "title", "year", "authors", "journal", "is_european"],
    )


def load_extractions() -> pl.DataFrame:
    conn = sqlite3.connect(EXTRACTIONS_DB)
    rows = conn.execute(
        "SELECT pmid, extracted_json FROM extractions "
        "WHERE schema_name = ? AND run_id = ? AND extracted_json IS NOT NULL",
        (SCHEMA, RUN_ID),
    ).fetchall()
    conn.close()
    records = []
    for pmid, ejson in rows:
        try:
            d = json.loads(ejson)
            if not isinstance(d, dict):
                continue
        except Exception:
            continue
        records.append({
            "pmid": pmid,
            "relevance":   str(d.get("relevance") or ""),
            "study_type":  str(d.get("study_type") or ""),
            "first_spec":  str(d.get("first_author_specialty") or "Unclear"),
            "last_spec":   str(d.get("last_author_specialty") or "Unclear"),
            "corr_spec":   str(d.get("corresponding_author_specialty") or "NotReported"),
        })
    return pl.DataFrame(records)


def build_dataset() -> list[dict]:
    """Load, join, enrich, return list of row dicts."""
    print("Loading papers...")
    papers = load_papers()
    print("Loading extractions...")
    ext = load_extractions()

    joined = papers.join(ext, on="pmid", how="inner")
    relevant = (
        joined
        .filter(pl.col("year").is_not_null())
        .with_columns(pl.col("year").cast(pl.Int32))
    )
    print(f"  {len(relevant):,} papers (all relevance categories)")

    print("Enriching author affiliations...")
    rows_out: list[dict] = []
    for row in relevant.to_dicts():
        authors = parse_authors(row.get("authors"))

        first_aff = get_affiliation(authors[0]) if authors else ""
        last_aff  = get_affiliation(authors[-1]) if (authors and len(authors) > 1) else first_aff

        all_affs = [get_affiliation(a) for a in authors]
        all_countries = list({detect_country(a) for a in all_affs if a})
        all_countries = [c for c in all_countries if c != "unknown"]
        all_insts = list({extract_institution(a) for a in all_affs if a})

        # Typed entity extraction via affiliation parser
        parsed_first = _parse_aff(first_aff)
        parsed_last  = _parse_aff(last_aff)
        all_hosps_set: set[str] = set()
        all_univs_set: set[str] = set()
        all_centers_set: set[str] = set()
        eu_hosps_set: set[str] = set()
        eu_univs_set: set[str] = set()
        eu_centers_set: set[str] = set()
        for author in authors:
            aff = get_affiliation(author)
            if aff:
                p = _parse_aff(aff)
                if p.hospital:    all_hosps_set.add(p.hospital)
                if p.university:  all_univs_set.add(p.university)
                if p.institution: all_centers_set.add(p.institution)
                # Use only the first semicolon segment for country detection so that
                # a European keyword in segment 2 doesn't contaminate a non-EU entity
                # extracted from segment 1 by the affiliation parser.
                aff_seg0 = aff.split(";")[0]
                ctry = detect_country(aff_seg0)
                if COUNTRY_REGION.get(ctry, "Unknown") in _EU_REGIONS:
                    if p.hospital:    eu_hosps_set.add(p.hospital)
                    if p.university:  eu_univs_set.add(p.university)
                    if p.institution: eu_centers_set.add(p.institution)

        jdata = {}
        if row.get("journal"):
            try:
                jdata = json.loads(row["journal"])
            except Exception:
                pass
        journal_title = jdata.get("title") or jdata.get("title_abbreviated") or ""

        rows_out.append({
            "pmid":         row["pmid"],
            "year":         row["year"],
            "relevance":    row["relevance"],
            "study_type":   row["study_type"],
            "first_spec":   row["first_spec"],
            "last_spec":    row["last_spec"],
            "corr_spec":    row["corr_spec"],
            "majority":     majority_spec(row["first_spec"], row["last_spec"], row["corr_spec"]),
            # Use only segment-0 for country so a European keyword in a later
            # semicolon-separated segment does not contaminate the entity extracted
            # from segment 0 by the affiliation parser.
            "first_country":  detect_country(first_aff.split(";")[0] if first_aff else ""),
            "first_inst":     extract_institution(first_aff),
            "last_country":   detect_country(last_aff.split(";")[0] if last_aff else ""),
            "last_inst":      extract_institution(last_aff),
            "all_countries":  all_countries,
            "all_insts":      all_insts,
            # Typed entity fields
            "first_hosp":    parsed_first.hospital,
            "first_univ":    parsed_first.university,
            "first_center":  parsed_first.institution,
            "last_hosp":     parsed_last.hospital,
            "last_univ":     parsed_last.university,
            "last_center":   parsed_last.institution,
            "all_hosps":     list(all_hosps_set),
            "all_univs":     list(all_univs_set),
            "all_centers":   list(all_centers_set),
            "eu_hosps":      list(eu_hosps_set),
            "eu_univs":      list(eu_univs_set),
            "eu_centers":    list(eu_centers_set),
            "journal":       journal_title,
            "is_european":   bool(row.get("is_european")),
        })

    return rows_out


# ── Stacked-proportion bar helper ─────────────────────────────────────────────

def stacked_prop_bar(
    ax: plt.Axes,
    year_cat_counts: dict[int, Counter],
    years: list[int],
    order: list[str],
    colors: dict[str, str],
    min_n: int = 15,
) -> list[int]:
    """Draw 100%-stacked bars; returns list of years actually drawn."""
    valid = [y for y in years if sum(year_cat_counts.get(y, Counter()).values()) >= min_n]
    if not valid:
        return []
    bottoms = np.zeros(len(valid))
    for label in order:
        vals = np.array([year_cat_counts.get(y, Counter()).get(label, 0) for y in valid], dtype=float)
        tots = np.array([sum(year_cat_counts.get(y, Counter()).values()) for y in valid], dtype=float)
        props = np.where(tots > 0, vals / tots * 100, 0)
        ax.bar(valid, props, bottom=bottoms, color=colors.get(label, "#CCC"),
               label=label, width=0.8, edgecolor="none")
        bottoms += props
    ax.set_xlim(min(valid) - 0.6, max(valid) + 0.6)
    ax.set_ylim(0, 100)
    ax.set_ylabel("Share (%)")
    ax.set_xlabel("Year")
    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    return valid


# ── T1: Volume by year ────────────────────────────────────────────────────────

def fig_T1(rows: list[dict]) -> None:
    years = sorted({r["year"] for r in rows})
    rel_order = ["Main", "Secondary", "NotRelevant"]
    rel_c = {
        "Main":        REL_C.get("Main", "#27AE60"),
        "Secondary":   REL_C.get("Secondary", "#F39C12"),
        "NotRelevant": "#95A5A6",
    }
    year_counts: dict[int, Counter] = defaultdict(Counter)
    for r in rows:
        v = r.get("relevance") or "NotRelevant"
        year_counts[r["year"]][v] += 1

    fig, ax = plt.subplots(figsize=(14, 5))
    bottoms = np.zeros(len(years))
    for label in rel_order:
        if not any(year_counts.get(y, Counter()).get(label, 0) for y in years):
            continue
        vals = np.array([year_counts.get(y, Counter()).get(label, 0) for y in years], dtype=float)
        ax.bar(years, vals, bottom=bottoms, color=rel_c.get(label, "#CCC"),
               label=label, width=0.8, edgecolor="none")
        bottoms += vals

    totals = [sum(year_counts.get(y, Counter()).values()) for y in years]
    ma5 = np.convolve(totals, np.ones(5) / 5, mode="same")
    ax.plot(years, ma5, color="#C0392B", lw=2, ls="--", label="5-yr MA")

    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax.set_xlim(min(years) - 0.6, max(years) + 0.6)
    ax.set_ylabel("Papers per year")
    ax.set_xlabel("Year")
    ax.set_title(f"{PROJECT_NAME} Publications per Year  (all extracted papers, n={len(rows):,})")
    ax.legend(loc="upper left")
    save_fig("T1_volume_by_year.png")


# ── T2-T4: Specialty by year (first / last / corresponding) ──────────────────

def _specialty_by_year(rows: list[dict], col: str, title: str, fname: str) -> None:
    spec_order = ["Cardiology", "Radiology", "Unclear", "NotReported"]
    spec_order = [s for s in spec_order if s in SPEC_C]
    years = sorted({r["year"] for r in rows})
    year_counts: dict[int, Counter] = defaultdict(Counter)
    for r in rows:
        v = r.get(col) or "Unclear"
        year_counts[r["year"]][v] += 1

    fig, ax = plt.subplots(figsize=(14, 5))
    valid = stacked_prop_bar(ax, year_counts, years, spec_order, SPEC_C)

    # Overlay total counts as thin line
    ax2 = ax.twinx()
    totals = [sum(year_counts.get(y, Counter()).values()) for y in valid]
    ax2.plot(valid, totals, color="#555", lw=1.2, ls=":", alpha=0.6, label="n (right)")
    ax2.set_ylabel("n papers", color="#555", fontsize=9)
    ax2.tick_params(axis="y", labelcolor="#555", labelsize=8)
    ax2.spines["top"].set_visible(False)

    patches = [mpatches.Patch(color=SPEC_C[s], label=s) for s in spec_order if s in SPEC_C]
    ax.legend(handles=patches, loc="upper left")
    ax.set_title(title)
    save_fig(fname)


def fig_T2(rows: list[dict]) -> None:
    _specialty_by_year(rows, "first_spec",
                       "First Author Specialty by Year  (proportional)",
                       "T2_first_author_specialty_by_year.png")


def fig_T3(rows: list[dict]) -> None:
    _specialty_by_year(rows, "last_spec",
                       "Last Author Specialty by Year  (proportional)",
                       "T3_last_author_specialty_by_year.png")


def fig_T4(rows: list[dict]) -> None:
    _specialty_by_year(rows, "corr_spec",
                       "Corresponding Author Specialty by Year  (proportional)",
                       "T4_corresponding_author_specialty_by_year.png")


# ── T5: Study type by year ────────────────────────────────────────────────────

def fig_T5(rows: list[dict]) -> None:
    st_order = ["OriginalResearch", "CaseReportOrSeries", "Review",
                "SystematicReviewOrMetaAnalysis", "Other"]
    years = sorted({r["year"] for r in rows})
    year_counts: dict[int, Counter] = defaultdict(Counter)
    for r in rows:
        st = r.get("study_type") or "Other"
        year_counts[r["year"]][st] += 1

    fig, ax = plt.subplots(figsize=(14, 5))
    stacked_prop_bar(ax, year_counts, years, st_order, STUDY_C)

    patches = [mpatches.Patch(color=STUDY_C[s], label=s) for s in st_order if s in STUDY_C]
    ax.legend(handles=patches, loc="upper center", bbox_to_anchor=(0.5, 1.18),
              ncol=3, fontsize=8, framealpha=0.9)
    ax.set_title("Study Type Distribution by Year  (proportional)", pad=30)
    save_fig("T5_study_type_by_year.png")


# ── T6: Majority specialty (donut + over-time) ────────────────────────────────

def fig_T6(rows: list[dict]) -> None:
    order = ["Cardiology", "Radiology", "Mixed", "Unclear"]
    overall = Counter(r["majority"] for r in rows)

    fig = plt.figure(figsize=(14, 6))
    gs = gridspec.GridSpec(1, 2, width_ratios=[1, 2])
    ax_donut = fig.add_subplot(gs[0])
    ax_bar   = fig.add_subplot(gs[1])

    # Donut
    sizes = [overall.get(o, 0) for o in order]
    colors_d = [SPEC_C.get(o, "#CCC") for o in order]
    wedges, texts, autotexts = ax_donut.pie(
        sizes, labels=None, colors=colors_d,
        autopct=lambda p: f"{p:.0f}%" if p > 2 else "",
        startangle=90, wedgeprops={"width": 0.55, "edgecolor": "white"},
    )
    for at in autotexts:
        at.set_fontsize(9)
    ax_donut.legend(
        handles=[mpatches.Patch(color=SPEC_C.get(o, "#CCC"), label=f"{o} ({overall.get(o,0):,})")
                 for o in order],
        loc="lower center", bbox_to_anchor=(0.5, -0.12), ncol=2, fontsize=9,
    )
    ax_donut.set_title("Overall Majority Specialty")

    # Stacked proportion by year
    years = sorted({r["year"] for r in rows})
    year_counts: dict[int, Counter] = defaultdict(Counter)
    for r in rows:
        year_counts[r["year"]][r["majority"]] += 1
    stacked_prop_bar(ax_bar, year_counts, years, order, SPEC_C)
    patches = [mpatches.Patch(color=SPEC_C.get(o, "#CCC"), label=o) for o in order]
    ax_bar.legend(handles=patches)
    ax_bar.set_title("Paper Majority Specialty over Time\n"
                     "(≥2 of first/last/corresponding agree)")
    save_fig("T6_majority_specialty.png")


# ── G1: Country distribution (bar fallback if no geopandas) ──────────────────

def fig_G1(rows: list[dict]) -> None:
    # Count per country: total papers and original-research papers
    country_total: Counter = Counter()
    country_orig:  Counter = Counter()
    for r in rows:
        is_orig = r.get("study_type") == "OriginalResearch"
        for c in set(r["all_countries"]):
            country_total[c] += 1
            if is_orig:
                country_orig[c] += 1
    country_total.pop("unknown", None)

    top40 = country_total.most_common(40)
    # Draw bottom-to-top (reversed list)
    top40_rev = list(reversed(top40))

    fig, ax = plt.subplots(figsize=(10, 13))
    y_pos = list(range(len(top40_rev)))
    seen_regions: set[str] = set()
    region_patches: list = []

    for i, (c_raw, total) in enumerate(top40_rev):
        orig = country_orig.get(c_raw, 0)
        rest = total - orig
        region = COUNTRY_REGION.get(c_raw, "Unknown")
        base_col = REGION_C.get(region, "#CCC")
        dark_col = _darken(base_col, 0.62)
        ax.barh(i, orig, color=base_col,  height=0.7, edgecolor="none")
        ax.barh(i, rest, left=orig, color=dark_col, height=0.7, edgecolor="none")
        # Label total at end of bar
        ax.text(total + total * 0.01, i, f"{total:,}",
                va="center", ha="left", fontsize=7)
        if region not in seen_regions:
            region_patches.append(mpatches.Patch(color=base_col, label=region))
            seen_regions.add(region)

    ax.set_yticks(y_pos)
    ax.set_yticklabels([COUNTRY_DISPLAY.get(c, c.title()) for c, _ in top40_rev], fontsize=8)
    ax.set_xlabel("Number of papers (≥1 author from country)")
    ax.set_title(f"Top 40 Countries — {PROJECT_NAME} Publications")
    ax.set_xlim(right=max(v for _, v in top40) * 1.12)

    # Legend: regions + shading key
    shade_patches = [
        mpatches.Patch(color="#888888", alpha=0.6,  label="Original Research (lighter)"),
        mpatches.Patch(color="#444444", alpha=0.85, label="Other study types (darker)"),
    ]
    ax.legend(handles=region_patches + shade_patches, loc="lower right", fontsize=7, ncol=1)
    save_fig("G1_country_map.png")


# ── G2: Top 20 countries bar ──────────────────────────────────────────────────

def fig_G2(rows: list[dict]) -> None:
    country_total: Counter = Counter()
    country_orig:  Counter = Counter()
    for r in rows:
        is_orig = r.get("study_type") == "OriginalResearch"
        for c in set(r["all_countries"]):
            country_total[c] += 1
            if is_orig:
                country_orig[c] += 1
    country_total.pop("unknown", None)

    top20 = country_total.most_common(20)
    top20_rev = list(reversed(top20))

    fig, ax = plt.subplots(figsize=(10, 7))
    y_pos = list(range(len(top20_rev)))
    seen: set[str] = set()
    region_patches: list = []

    for i, (c_raw, total) in enumerate(top20_rev):
        orig = country_orig.get(c_raw, 0)
        rest = total - orig
        region = COUNTRY_REGION.get(c_raw, "Unknown")
        base_col = REGION_C.get(region, "#CCC")
        dark_col = _darken(base_col, 0.62)
        ax.barh(i, orig, color=base_col,  height=0.72, edgecolor="none")
        ax.barh(i, rest, left=orig, color=dark_col, height=0.72, edgecolor="none")
        ax.text(total + total * 0.01, i,
                f"{total:,}  (orig: {orig:,})",
                va="center", ha="left", fontsize=8)
        if region not in seen:
            region_patches.append(mpatches.Patch(color=base_col, label=region))
            seen.add(region)

    ax.set_yticks(y_pos)
    ax.set_yticklabels([COUNTRY_DISPLAY.get(c, c.title()) for c, _ in top20_rev], fontsize=9)
    ax.set_xlabel("Number of papers (≥1 author from country)")
    ax.set_title(f"Top 20 Countries — {PROJECT_NAME} Publications")
    ax.set_xlim(right=max(v for _, v in top20) * 1.30)

    shade_patches = [
        mpatches.Patch(color="#888888", alpha=0.6,  label="Original Research (lighter)"),
        mpatches.Patch(color="#444444", alpha=0.85, label="Other study types (darker)"),
    ]
    ax.legend(handles=region_patches + shade_patches, loc="lower right", fontsize=8)
    save_fig("G2_top20_countries.png")


# ── G3: Region over time (proportional stacked bar) — 2 rows ─────────────────

def fig_G3(rows: list[dict]) -> None:
    orig_rows = [r for r in rows if r.get("study_type") == "OriginalResearch"]
    all_years = sorted({r["year"] for r in rows})

    def _draw_region_bar(subset: list[dict], ax: plt.Axes, title: str, min_n: int = 5) -> None:
        year_region: dict[int, Counter] = defaultdict(Counter)
        for r in subset:
            region = COUNTRY_REGION.get(r["first_country"], "Unknown")
            year_region[r["year"]][region] += 1
        valid = [y for y in all_years if sum(year_region.get(y, Counter()).values()) >= min_n]
        totals_arr = np.array([sum(year_region.get(y, Counter()).values()) for y in valid], dtype=float)
        bottoms = np.zeros(len(valid))
        for region in REGION_ORDER:
            vals = np.array([year_region.get(y, Counter()).get(region, 0) for y in valid], dtype=float)
            props = np.where(totals_arr > 0, vals / totals_arr * 100, 0)
            ax.bar(valid, props, bottom=bottoms,
                   color=REGION_C.get(region, "#CCC"), label=region, width=0.85, edgecolor="none")
            bottoms += props
        ax.set_ylim(0, 100)
        ax.set_ylabel("Share (%)")
        ax.set_xlabel("Year")
        ax.set_title(title)
        ax.xaxis.set_major_locator(mticker.MultipleLocator(2))

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=False)
    _draw_region_bar(rows,      axes[0], f"Geographic Distribution — All Papers  (n={len(rows):,})")
    _draw_region_bar(orig_rows, axes[1], f"Geographic Distribution — Original Research Only  (n={len(orig_rows):,})")

    patches = [mpatches.Patch(color=REGION_C.get(r, "#CCC"), label=r) for r in REGION_ORDER]
    fig.legend(handles=patches, loc="upper left", ncol=2, fontsize=8,
               bbox_to_anchor=(1.01, 0.95))
    plt.tight_layout()
    save_fig("G3_region_over_time.png")


# ── G4: Decade comparison (2001–2014 vs 2015–2026) — 2 rows ──────────────────

def fig_G4(rows: list[dict]) -> None:
    orig_rows = [r for r in rows if r.get("study_type") == "OriginalResearch"]

    def region_share(subset: list[dict]) -> dict[str, float]:
        cnt: Counter = Counter()
        for r in subset:
            cnt[COUNTRY_REGION.get(r["first_country"], "Unknown")] += 1
        total = sum(cnt.values()) or 1
        return {k: v / total * 100 for k, v in cnt.items()}

    def _draw_era_row(subset: list[dict], axes_row: list[plt.Axes], row_label: str) -> None:
        era_a = [r for r in subset if 2001 <= r["year"] <= 2014]
        era_b = [r for r in subset if 2015 <= r["year"] <= 2026]
        share_a = region_share(era_a)
        share_b = region_share(era_b)
        regions = [r for r in REGION_ORDER if share_a.get(r, 0) + share_b.get(r, 0) > 0]
        val_a = [share_a.get(r, 0) for r in regions]
        val_b = [share_b.get(r, 0) for r in regions]
        diff  = [b - a for a, b in zip(val_a, val_b)]
        y = np.arange(len(regions))
        w = 0.38

        ax = axes_row[0]
        ax.barh(y + w / 2, val_a, height=w, color="#2980B9", label="2001–2014", alpha=0.85)
        ax.barh(y - w / 2, val_b, height=w, color="#E74C3C", label="2015–2026", alpha=0.85)
        ax.set_yticks(y)
        ax.set_yticklabels(regions, fontsize=9)
        ax.set_xlabel("Share (%)")
        ax.set_title(f"{row_label}\n2001–2014 (n={len(era_a):,}) vs 2015–2026 (n={len(era_b):,})")
        ax.legend(fontsize=8)

        ax2 = axes_row[1]
        colours = ["#E74C3C" if d > 0 else "#2980B9" for d in diff]
        ax2.barh(y, diff, color=colours, alpha=0.85)
        ax2.axvline(0, color="#555", lw=1)
        ax2.set_yticks(y)
        ax2.set_yticklabels(regions, fontsize=9)
        ax2.set_xlabel("Change in share (pp)")
        ax2.set_title(f"Change in Regional Share — {row_label}\n(2015–2026 minus 2001–2014)")

    fig, axes = plt.subplots(2, 2, figsize=(14, 13))
    _draw_era_row(rows,      [axes[0, 0], axes[0, 1]], "All Papers")
    _draw_era_row(orig_rows, [axes[1, 0], axes[1, 1]], "Original Research Only")

    plt.suptitle("Geographic Era Comparison", y=1.01, fontsize=13, fontweight="bold")
    plt.tight_layout()
    save_fig("G4_decade_comparison.png")


# ── I1: Top-15 institutions global — stratified by entity type ────────────────

_HOSP_BASE  = "#2980B9"   # blue
_UNIV_BASE  = "#C0392B"   # deep red
_INST_BASE  = "#16A085"   # teal

def _typed_panel(
    ax: plt.Axes,
    rows: list[dict],
    any_key: str,
    orig_key: str | None,
    top_n: int,
    base_col: str,
    title: str,
    xlabel: str = "Papers (any-author)",
    label_width: int = 42,
    filter_generic: bool = False,
) -> None:
    """Draw a single horizontal-bar panel for one entity type."""
    cnt:  Counter = Counter()
    orig: Counter = Counter()
    for r in rows:
        is_orig = r.get("study_type") == "OriginalResearch"
        raw = r[any_key]
        names = raw if isinstance(raw, list) else ([raw] if raw else [])
        for name in set(names):
            if not name:
                continue
            name = _normalize_inst_name(name)
            if filter_generic and not _is_meaningful_inst(name):
                continue
            cnt[name] += 1
            if is_orig and orig_key:
                orig[name] += 1
    if not cnt:
        ax.set_visible(False)
        return
    dark_col = _darken(base_col, 0.62)
    top = cnt.most_common(top_n)
    top_rev = list(reversed(top))
    for i, (name, total) in enumerate(top_rev):
        o = orig.get(name, 0)
        ax.barh(i, o, color=base_col, height=0.72, edgecolor="none")
        ax.barh(i, total - o, left=o, color=dark_col, height=0.72, edgecolor="none")
        ax.text(total + max(1, total * 0.015), i, f"{total:,}",
                va="center", ha="left", fontsize=7.5)
    ax.set_yticks(range(len(top_rev)))
    ax.set_yticklabels([t[:label_width] for t, _ in top_rev], fontsize=8)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_title(title, fontsize=10, fontweight="bold", color=base_col)
    ax.set_xlim(right=max(v for _, v in top) * 1.16)
    ax.legend(
        handles=[mpatches.Patch(color=base_col, label="Original Research"),
                 mpatches.Patch(color=dark_col, label="Other study types")],
        loc="lower right", fontsize=7.5, framealpha=0.7,
    )


def fig_I1(rows: list[dict]) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(20, 8))
    _typed_panel(axes[0], rows, "all_hosps",   "all_hosps",   15, _HOSP_BASE,
                 "Hospitals", label_width=40, filter_generic=True)
    _typed_panel(axes[1], rows, "all_univs",   "all_univs",   15, _UNIV_BASE,
                 "Universities", label_width=40, filter_generic=True)
    _typed_panel(axes[2], rows, "all_centers", "all_centers", 15, _INST_BASE,
                 "Research Institutes / Centers", label_width=40, filter_generic=True)
    fig.suptitle(
        f"Top 15 Institutions by Type — {PROJECT_NAME}, Global (any-author count)",
        fontsize=13, fontweight="bold", y=1.01,
    )
    save_fig("I1_top20_institutions_global.png")


# ── I2: Top-12 institutions European only — stratified by entity type ─────────

def fig_I2(rows: list[dict]) -> None:
    # Use eu_hosps/eu_univs/eu_centers — entities from European-affiliated authors only
    eu = [r for r in rows if r["is_european"]]
    fig, axes = plt.subplots(1, 3, figsize=(20, 7))
    _typed_panel(axes[0], eu, "eu_hosps",   "eu_hosps",   12, _HOSP_BASE,
                 "Hospitals", xlabel="Papers (European-affiliated authors)",
                 label_width=40, filter_generic=True)
    _typed_panel(axes[1], eu, "eu_univs",   "eu_univs",   12, _UNIV_BASE,
                 "Universities", xlabel="Papers (European-affiliated authors)",
                 label_width=40, filter_generic=True)
    _typed_panel(axes[2], eu, "eu_centers", "eu_centers", 12, _INST_BASE,
                 "Research Institutes / Centers",
                 xlabel="Papers (European-affiliated authors)", label_width=40,
                 filter_generic=True)
    fig.suptitle(
        f"Top 12 European Institutions by Type — {PROJECT_NAME} (European-author affiliations)",
        fontsize=13, fontweight="bold", y=1.01,
    )
    save_fig("I2_top20_institutions_european.png")


# ── I3: Institutions by author role — separate rows for hospitals / universities

def _role_panel(
    ax: plt.Axes,
    rows: list[dict],
    any_key: str,
    first_key: str,
    last_key: str,
    top_n: int,
    base_col: str,
    title: str,
) -> None:
    """Grouped 3-bar panel: any / first / last counts for top institutions of one type."""
    any_cnt:   Counter = Counter()
    first_cnt: Counter = Counter()
    last_cnt:  Counter = Counter()
    for r in rows:
        fi = _normalize_inst_name(r.get(first_key) or "")
        la = _normalize_inst_name(r.get(last_key) or "")
        if fi and _is_meaningful_inst(fi):
            first_cnt[fi] += 1
        if la and _is_meaningful_inst(la):
            last_cnt[la] += 1
        raw = r[any_key]
        names = raw if isinstance(raw, list) else ([raw] if raw else [])
        for name in set(names):
            if name:
                name = _normalize_inst_name(name)
                if _is_meaningful_inst(name):
                    any_cnt[name] += 1
    if not any_cnt:
        ax.set_visible(False)
        return
    top = [name for name, _ in any_cnt.most_common(top_n)]
    labels = [t[:48] for t in reversed(top)]
    any_v   = [any_cnt.get(t, 0)   for t in reversed(top)]
    first_v = [first_cnt.get(t, 0) for t in reversed(top)]
    last_v  = [last_cnt.get(t, 0)  for t in reversed(top)]
    y = np.arange(len(top))
    w = 0.26
    light_col = _lighten(base_col, 0.55)
    ax.barh(y + w, any_v,   height=w, color="#2C3E50",  label="Any author",
            alpha=0.88, edgecolor="none")
    ax.barh(y,     first_v, height=w, color=base_col,   label="First author",
            alpha=0.90, edgecolor="none")
    ax.barh(y - w, last_v,  height=w, color=light_col,  label="Last author",
            alpha=0.90, edgecolor=_darken(base_col, 0.7), linewidth=0.4)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Number of papers", fontsize=8)
    ax.set_title(title, fontsize=10, fontweight="bold", color=base_col)
    ax.legend(fontsize=8, loc="lower right")


def fig_I3(rows: list[dict]) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(13, 14))
    _role_panel(axes[0], rows,
                any_key="all_hosps", first_key="first_hosp", last_key="last_hosp",
                top_n=12, base_col=_HOSP_BASE,
                title="Hospitals — First vs Last vs Any-Author Count")
    _role_panel(axes[1], rows,
                any_key="all_univs", first_key="first_univ", last_key="last_univ",
                top_n=12, base_col=_UNIV_BASE,
                title="Universities — First vs Last vs Any-Author Count")
    fig.suptitle(
        "Top 12 Hospitals and Universities — Author-Role Breakdown",
        fontsize=13, fontweight="bold",
    )
    save_fig("I3_institutions_by_role.png")


# ── I4: Hospital/University × country (2-row × 5-column grid) ───────────────

def _country_entity_panel(
    ax: plt.Axes,
    rows: list[dict],
    country: str,
    entity_key: str,    # "first_hosp" or "first_univ"
    top_n: int,
    base_col: str,
) -> None:
    subset = [r for r in rows if r["first_country"] == country]
    cnt:  Counter = Counter()
    orig: Counter = Counter()
    for r in subset:
        name = r.get(entity_key)
        if name:
            name = _normalize_inst_name(name)
            if not _is_meaningful_inst(name):
                continue
            cnt[name] += 1
            if r.get("study_type") == "OriginalResearch":
                orig[name] += 1
    if not cnt:
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, fontsize=8, color="gray")
        ax.set_xticks([])
        ax.set_yticks([])
        return
    dark_col = _darken(base_col, 0.62)
    top = cnt.most_common(top_n)
    top_rev = list(reversed(top))
    for i, (name, total) in enumerate(top_rev):
        o = orig.get(name, 0)
        ax.barh(i, o, color=base_col, height=0.72, edgecolor="none")
        ax.barh(i, total - o, left=o, color=dark_col, height=0.72, edgecolor="none")
    ax.set_yticks(range(len(top_rev)))
    ax.set_yticklabels([t[:38] for t, _ in top_rev], fontsize=7)
    ax.set_xlim(right=max(v for _, v in top) * 1.16)


def fig_I4(rows: list[dict]) -> None:
    country_cnt: Counter = Counter(
        r["first_country"] for r in rows if r["first_country"] != "unknown"
    )
    top5 = [c for c, _ in country_cnt.most_common(5)]

    fig, axes = plt.subplots(2, 5, figsize=(20, 12))

    row_config = [
        ("first_hosp", _HOSP_BASE, "Hospitals"),
        ("first_univ", _UNIV_BASE, "Universities"),
    ]
    for row_idx, (entity_key, base_col, row_label) in enumerate(row_config):
        for col_idx, country in enumerate(top5):
            ax = axes[row_idx][col_idx]
            _country_entity_panel(ax, rows, country, entity_key, 5, base_col)
            ax.set_xlabel("Papers (first author)", fontsize=7)
            if col_idx == 0:
                ax.set_ylabel(row_label, fontsize=8, fontweight="bold", color=base_col)
            if row_idx == 0:
                ax.set_title(
                    COUNTRY_DISPLAY.get(country, country.title()),
                    fontsize=10, fontweight="bold",
                )

    # Shared legend
    legend_patches = [
        mpatches.Patch(color=_HOSP_BASE, label="Hospital — Original Research"),
        mpatches.Patch(color=_darken(_HOSP_BASE, 0.62), label="Hospital — Other"),
        mpatches.Patch(color=_UNIV_BASE, label="University — Original Research"),
        mpatches.Patch(color=_darken(_UNIV_BASE, 0.62), label="University — Other"),
    ]
    fig.legend(handles=legend_patches, loc="lower center",
               ncol=4, fontsize=8, bbox_to_anchor=(0.5, -0.01))
    fig.suptitle(
        "Top 5 Hospitals and Universities per Top-5 Country  (first-author affiliation)",
        fontsize=13, fontweight="bold", y=1.01,
    )
    save_fig("I4_institution_by_country.png")


# ── J1: Top-20 journals (all papers) ─────────────────────────────────────────

# ── Expert-curated journal scope classification ───────────────────────────────
# Categories follow the WoS Journal Citation Reports subject areas for cardiac
# imaging literature.  Lookup is case-insensitive; substring matching is used
# for abbreviated / variant titles.
#
#  Cardiovascular Imaging  — dedicated cardiac / vascular imaging journals
#  Radiology               — general diagnostic imaging / radiology journals
#  Cardiology              — general cardiology / cardiovascular medicine
#  Nuclear Medicine        — nuclear cardiology / PET / SPECT-specific
#  General/Other           — multidisciplinary, methods, case-report aggregators
#
JOURNAL_SCOPE: dict[str, str] = {
    # ── Cardiovascular Imaging ────────────────────────────────────────────────
    "journal of cardiovascular computed tomography":   "Cardiovascular Imaging",
    "international journal of cardiovascular imaging": "Cardiovascular Imaging",
    "jacc. cardiovascular imaging":                    "Cardiovascular Imaging",
    "jacc cardiovascular imaging":                     "Cardiovascular Imaging",
    "european heart journal. cardiovascular imaging":  "Cardiovascular Imaging",
    "european heart journal - cardiovascular imaging": "Cardiovascular Imaging",
    "ehj cardiovascular imaging":                      "Cardiovascular Imaging",
    "circulation. cardiovascular imaging":             "Cardiovascular Imaging",
    "circulation: cardiovascular imaging":             "Cardiovascular Imaging",
    "international journal of cardiac imaging":        "Cardiovascular Imaging",
    "echocardiography":                                "Cardiovascular Imaging",
    "journal of the american society of echocardiography": "Cardiovascular Imaging",
    "cardiovascular ultrasound":                       "Cardiovascular Imaging",
    "echo research and practice":                      "Cardiovascular Imaging",
    "journal of cardiovascular magnetic resonance":    "Cardiovascular Imaging",
    "cardiovascular imaging":                          "Cardiovascular Imaging",
    # ── Nuclear Medicine / Cardiology crossover ───────────────────────────────
    "journal of nuclear cardiology":                   "Nuclear Medicine",
    "european journal of nuclear medicine":            "Nuclear Medicine",
    "journal of nuclear medicine":                     "Nuclear Medicine",
    "nuclear medicine and molecular imaging":          "Nuclear Medicine",
    # ── Radiology ─────────────────────────────────────────────────────────────
    "european radiology":                              "Radiology",
    "european journal of radiology":                   "Radiology",
    "radiology":                                       "Radiology",
    "ajr. american journal of roentgenology":          "Radiology",
    "american journal of roentgenology":               "Radiology",
    "academic radiology":                              "Radiology",
    "investigative radiology":                         "Radiology",
    "journal of thoracic imaging":                     "Radiology",
    "journal of computer assisted tomography":         "Radiology",
    "rofo":                                            "Radiology",
    "british journal of radiology":                    "Radiology",
    "clinical radiology":                              "Radiology",
    "diagnostic and interventional radiology":         "Radiology",
    "diagnostic and interventional imaging":           "Radiology",
    "insights into imaging":                           "Radiology",
    "quantitative imaging in medicine and surgery":    "Radiology",
    "magnetic resonance imaging":                      "Radiology",
    "journal of magnetic resonance imaging":           "Radiology",
    "magnetic resonance in medicine":                  "Radiology",
    "nmr in biomedicine":                              "Radiology",
    "radiation oncology":                              "Radiology",
    "radiologie":                                      "Radiology",
    "radiologia":                                      "Radiology",
    "radiographics":                                   "Radiology",
    "the british journal of radiology":                "Radiology",
    "medical physics":                                 "Radiology",
    "physics in medicine and biology":                 "Radiology",
    "eur radiol":                                      "Radiology",
    # ── Cardiology ────────────────────────────────────────────────────────────
    "journal of the american college of cardiology":   "Cardiology",
    "the american journal of cardiology":              "Cardiology",
    "american journal of cardiology":                  "Cardiology",
    "international journal of cardiology":             "Cardiology",
    "frontiers in cardiovascular medicine":            "Cardiology",
    "european heart journal":                          "Cardiology",
    "european heart journal. case reports":            "Cardiology",
    "heart":                                           "Cardiology",
    "heart (british cardiac society)":                 "Cardiology",
    "circulation":                                     "Cardiology",
    "jacc. case reports":                              "Cardiology",
    "jacc case reports":                               "Cardiology",
    "atherosclerosis":                                 "Cardiology",
    "american heart journal":                          "Cardiology",
    "journal of the american heart association":       "Cardiology",
    "european journal of heart failure":               "Cardiology",
    "cardiovascular research":                         "Cardiology",
    "clinical cardiology":                             "Cardiology",
    "journal of cardiovascular medicine":              "Cardiology",
    "journal of interventional cardiology":            "Cardiology",
    "cardiovascular diabetology":                      "Cardiology",
    "open heart":                                      "Cardiology",
    "heart rhythm":                                    "Cardiology",
    "europace":                                        "Cardiology",
    "catheterization and cardiovascular interventions": "Cardiology",
    "eurointervention":                                "Cardiology",
    "acta cardiologica":                               "Cardiology",
    "bmc cardiovascular disorders":                    "Cardiology",
    "journal of cardiac surgery":                      "Cardiology",
    "annals of thoracic surgery":                      "Cardiology",
    "heart and vessels":                               "Cardiology",
    "cardiology":                                      "Cardiology",
    "cardiology research and practice":                "Cardiology",
    "heart, lung and circulation":                     "Cardiology",
    "the international journal of cardiology":         "Cardiology",
    # ── General / Other ───────────────────────────────────────────────────────
    "plos one":                                        "General/Other",
    "plos medicine":                                   "General/Other",
    "scientific reports":                              "General/Other",
    "journal of clinical medicine":                    "General/Other",
    "cureus":                                          "General/Other",
    "diagnostics":                                     "General/Other",
    "frontiers in medicine":                           "General/Other",
    "medicine":                                        "General/Other",
    "bmj open":                                        "General/Other",
    "bmc medicine":                                    "General/Other",
    "the lancet":                                      "General/Other",
    "new england journal of medicine":                 "General/Other",
    "jama":                                            "General/Other",
    "annals of internal medicine":                     "General/Other",
    "the journal of clinical investigation":           "General/Other",
    "journal of clinical investigation":               "General/Other",
    "internal medicine":                               "General/Other",
}

SCOPE_C = {
    "Cardiovascular Imaging": "#E74C3C",   # red
    "Nuclear Medicine":        "#D35400",   # orange-red
    "Cardiology":              "#27AE60",   # green
    "Radiology":               "#2980B9",   # blue
    "General/Other":           "#95A5A6",   # grey
}


def _classify_journal(title: str) -> str:
    """Case-insensitive scope lookup with prefix fallback."""
    t = title.lower().strip().rstrip(".")
    # Exact / substring match against curated dict
    if t in JOURNAL_SCOPE:
        return JOURNAL_SCOPE[t]
    # Try substring: a curated key appears inside the title
    for key, scope in JOURNAL_SCOPE.items():
        if key in t or t in key:
            return scope
    return "General/Other"


def _journal_bar(rows: list[dict], title: str, fname: str) -> None:
    jcnt:  Counter = Counter(r["journal"] for r in rows if r["journal"])
    jorig: Counter = Counter(r["journal"] for r in rows
                             if r["journal"] and r.get("study_type") == "OriginalResearch")
    top20 = jcnt.most_common(20)
    top20_rev = list(reversed(top20))

    fig, ax = plt.subplots(figsize=(11, 8))
    for i, (jname, total) in enumerate(top20_rev):
        orig = jorig.get(jname, 0)
        rest = total - orig
        scope = _classify_journal(jname)
        base_col = SCOPE_C.get(scope, "#95A5A6")
        dark_col = _darken(base_col, 0.62)
        ax.barh(i, orig, color=base_col,  height=0.72, edgecolor="none")
        ax.barh(i, rest, left=orig, color=dark_col, height=0.72, edgecolor="none")
        ax.text(total + 2, i, f"{total:,}", va="center", ha="left", fontsize=8)

    ax.set_yticks(range(len(top20_rev)))
    ax.set_yticklabels([t[:60] for t, _ in top20_rev], fontsize=8)
    ax.set_xlabel("Number of papers")
    ax.set_title(title)
    max_v = max(v for _, v in top20) if top20 else 1
    ax.set_xlim(right=max_v * 1.18)

    scope_patches = [mpatches.Patch(color=SCOPE_C[s], label=s) for s in SCOPE_C]
    shade_patches = [
        mpatches.Patch(color="#888888", alpha=0.6,  label="Original Research (lighter)"),
        mpatches.Patch(color="#444444", alpha=0.85, label="Other types (darker)"),
    ]
    ax.legend(handles=scope_patches + shade_patches,
              loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8, framealpha=0.9)
    save_fig(fname)


def fig_J1(rows: list[dict]) -> None:
    _journal_bar(rows, f"Top 20 Journals — {PROJECT_NAME} (all papers)", "J1_top20_journals_all.png")


def fig_J2(rows: list[dict]) -> None:
    eu = [r for r in rows if r["is_european"]]
    _journal_bar(eu, f"Top 20 Journals — {PROJECT_NAME} (European papers)", "J2_top20_journals_european.png")


# ── X1: Specialty × Study-type heatmap ───────────────────────────────────────

def fig_X1(rows: list[dict]) -> None:
    import numpy as np

    specs     = ["Cardiology", "Radiology", "Unclear", "NotReported"]
    stypes    = ["OriginalResearch", "CaseReportOrSeries", "Review",
                 "SystematicReviewOrMetaAnalysis", "Other"]
    roles     = [("first_spec",  "First Author"),
                 ("last_spec",   "Last Author"),
                 ("corr_spec",   "Corresponding Author")]

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    for ax, (col, role_label) in zip(axes, roles):
        mat = np.zeros((len(specs), len(stypes)))
        for r in rows:
            sp = r.get(col) or "Unclear"
            st = r.get("study_type") or "Other"
            if sp in specs and st in stypes:
                mat[specs.index(sp), stypes.index(st)] += 1
        # Normalise by row (specialty)
        row_tots = mat.sum(axis=1, keepdims=True)
        mat_pct = np.where(row_tots > 0, mat / row_tots * 100, 0)

        im = ax.imshow(mat_pct, cmap="Blues", vmin=0, vmax=80, aspect="auto")
        ax.set_xticks(range(len(stypes)))
        ax.set_xticklabels(["OrigRes", "CaseRep", "Review", "SysRev", "Other"],
                           rotation=30, ha="right", fontsize=8)
        ax.set_yticks(range(len(specs)))
        ax.set_yticklabels(specs, fontsize=9)
        ax.set_title(f"{role_label}", fontsize=10)
        for i in range(len(specs)):
            for j in range(len(stypes)):
                ax.text(j, i, f"{int(mat[i,j])}", ha="center", va="center",
                        fontsize=7, color="black" if mat_pct[i,j] < 50 else "white")

    plt.colorbar(im, ax=axes, shrink=0.6, label="% within specialty")
    plt.suptitle("Study Type Distribution by Author Specialty  (% within row)", y=1.02)
    save_fig("X1_specialty_studytype_heatmap.png")


# ── X2: Dashboard overview (6-panel composite) ───────────────────────────────

def fig_X2(rows: list[dict]) -> None:
    fig = plt.figure(figsize=(18, 11))
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)
    ax_vol  = fig.add_subplot(gs[0, :2])   # wide left  — volume
    ax_spec = fig.add_subplot(gs[0, 2])    # right      — first-author specialty donut
    ax_reg  = fig.add_subplot(gs[1, 0])    # bottom-left — region bar
    ax_j    = fig.add_subplot(gs[1, 1])    # bottom-mid  — journal top-10
    ax_st   = fig.add_subplot(gs[1, 2])    # bottom-right — study-type donut

    # --- Volume ---
    yc = Counter(r["year"] for r in rows)
    yrs = sorted(yc)
    vals = [yc[y] for y in yrs]
    ax_vol.bar(yrs, vals, color="#27AE60", width=0.75, alpha=0.85)
    idx, roll = rolling_avg(vals, 5)
    ax_vol.plot(np.array(yrs)[idx], roll, color="#2C3E50", lw=2, ls="--")
    ax_vol.set_title("Publications per Year")
    ax_vol.set_xlabel("Year")
    ax_vol.set_ylabel("n")
    ax_vol.xaxis.set_major_locator(mticker.MultipleLocator(3))
    ax_vol.spines["top"].set_visible(False)
    ax_vol.spines["right"].set_visible(False)

    # --- Specialty donut ---
    spec_order = ["Cardiology", "Radiology", "Unclear", "NotReported"]
    sc = Counter(r["first_spec"] for r in rows)
    sizes = [sc.get(s, 0) for s in spec_order]
    colors_d = [SPEC_C[s] for s in spec_order]
    ax_spec.pie(sizes, colors=colors_d, startangle=90,
                wedgeprops={"width": 0.5, "edgecolor": "white"},
                autopct=lambda p: f"{p:.0f}%" if p > 4 else "")
    ax_spec.set_title("First Author\nSpecialty")
    ax_spec.legend(
        handles=[mpatches.Patch(color=SPEC_C[s], label=f"{s}\n({sc.get(s,0):,})")
                 for s in spec_order],
        loc="lower center", bbox_to_anchor=(0.5, -0.22), ncol=2, fontsize=7,
    )

    # --- Region bar ---
    rc_total: Counter = Counter()
    rc_orig:  Counter = Counter()
    for r in rows:
        region = COUNTRY_REGION.get(r["first_country"], "Unknown")
        rc_total[region] += 1
        if r.get("study_type") == "OriginalResearch":
            rc_orig[region] += 1
    top_regions = rc_total.most_common(8)
    top_regions_rev = list(reversed(top_regions))
    for i, (region, total) in enumerate(top_regions_rev):
        orig = rc_orig.get(region, 0)
        rest = total - orig
        base_col = REGION_C.get(region, "#CCC")
        dark_col = _darken(base_col, 0.62)
        ax_reg.barh(i, orig, color=base_col, alpha=0.85, height=0.72, edgecolor="none")
        ax_reg.barh(i, rest, left=orig, color=dark_col, alpha=0.85, height=0.72, edgecolor="none")
    ax_reg.set_yticks(range(len(top_regions_rev)))
    ax_reg.set_yticklabels([t for t, _ in top_regions_rev], fontsize=8)
    ax_reg.set_title("Top Regions\n(first-author)")
    ax_reg.set_xlabel("n papers")
    ax_reg.spines["top"].set_visible(False)
    ax_reg.spines["right"].set_visible(False)

    # --- Top-10 journals (ISO abbreviations to avoid label collisions) ---
    _ISO_ABBR = {
        "journal of cardiovascular computed tomography":    "J Cardiovasc CT",
        "the international journal of cardiovascular imaging": "Int J Cardiovasc Imaging",
        "european radiology":                               "Eur Radiol",
        "jacc. cardiovascular imaging":                     "JACC Cardiovasc Imaging",
        "european heart journal. cardiovascular imaging":   "EHJ Cardiovasc Imaging",
        "international journal of cardiology":              "Int J Cardiol",
        "frontiers in cardiovascular medicine":             "Front Cardiovasc Med",
        "european journal of radiology":                    "Eur J Radiol",
        "journal of the american college of cardiology":    "JACC",
        "the american journal of cardiology":               "Am J Cardiol",
        "radiology":                                        "Radiology",
        "european heart journal. case reports":             "EHJ Case Reports",
        "atherosclerosis":                                  "Atherosclerosis",
        "journal of clinical medicine":                     "J Clin Med",
        "ajr. american journal of roentgenology":           "AJR",
        "jacc. case reports":                               "JACC Case Reports",
        "academic radiology":                               "Acad Radiol",
        "cureus":                                           "Cureus",
        "circulation. cardiovascular imaging":              "Circ Cardiovasc Imaging",
        "journal of nuclear cardiology : official publication of the american society of nuclear cardiology":
            "J Nucl Cardiol",
        "journal of thoracic imaging":                      "J Thorac Imaging",
        "diagnostics (basel, switzerland)":                 "Diagnostics",
        "journal of computer assisted tomography":          "J Comput Assist Tomogr",
        "plos one":                                         "PLoS ONE",
        "scientific reports":                               "Sci Rep",
        "quantitative imaging in medicine and surgery":     "Quant Imaging Med Surg",
        "echocardiography (mount kisco, n.y.)":             "Echocardiography",
        "european heart journal":                           "Eur Heart J",
        "heart (british cardiac society)":                  "Heart",
        "medical physics":                                  "Med Phys",
    }
    jc:      Counter = Counter(r["journal"] for r in rows if r["journal"])
    jc_orig: Counter = Counter(r["journal"] for r in rows
                               if r["journal"] and r.get("study_type") == "OriginalResearch")
    top_j = jc.most_common(10)
    top_j_rev = list(reversed(top_j))
    for i, (jname, total) in enumerate(top_j_rev):
        orig = jc_orig.get(jname, 0)
        rest = total - orig
        scope = _classify_journal(jname)
        base_col = SCOPE_C.get(scope, "#95A5A6")
        dark_col = _darken(base_col, 0.62)
        ax_j.barh(i, orig, color=base_col, alpha=0.85, edgecolor="none")
        ax_j.barh(i, rest, left=orig, color=dark_col, alpha=0.85, edgecolor="none")
    ax_j.set_yticks(range(len(top_j_rev)))
    ax_j.set_yticklabels([_ISO_ABBR.get(t.lower(), t[:20]) for t, _ in top_j_rev], fontsize=8)
    ax_j.set_title("Top 10 Journals")
    ax_j.set_xlabel("n papers")
    ax_j.spines["top"].set_visible(False)
    ax_j.spines["right"].set_visible(False)

    # --- Study type donut ---
    st_order = ["OriginalResearch", "CaseReportOrSeries", "Review",
                "SystematicReviewOrMetaAnalysis", "Other"]
    stc = Counter(r["study_type"] for r in rows)
    st_sizes = [stc.get(s, 0) for s in st_order]
    st_labels = ["Original\nResearch", "Case\nReport",
                 "Review", "Syst. Rev.\n/Meta", "Other"]
    ax_st.pie(st_sizes, labels=None, colors=[STUDY_C[s] for s in st_order],
              startangle=90, wedgeprops={"width": 0.5, "edgecolor": "white"},
              autopct=lambda p: f"{p:.0f}%" if p > 3 else "")
    ax_st.set_title("Study Types")
    ax_st.legend(
        handles=[mpatches.Patch(color=STUDY_C[s], label=f"{st_labels[i]}\n({stc.get(s,0):,})")
                 for i, s in enumerate(st_order)],
        loc="lower center", bbox_to_anchor=(0.5, -0.25), ncol=2, fontsize=7,
    )

    plt.suptitle(
        f"{PROJECT_NAME} Publications 2001–2026 — Summary Dashboard\n"
        f"n = {len(rows):,} papers (all extracted papers)",
        fontsize=13, fontweight="bold", y=1.01,
    )
    save_fig("X2_dashboard_overview.png")


# ── Main ──────────────────────────────────────────────────────────────────────

FIGURES = [
    ("T1 — Volume by year",                    fig_T1),
    ("T2 — First-author specialty by year",    fig_T2),
    ("T3 — Last-author specialty by year",     fig_T3),
    ("T4 — Corresponding specialty by year",   fig_T4),
    ("T5 — Study type by year",                fig_T5),
    ("T6 — Majority specialty",                fig_T6),
    ("G1 — Country map / bar",                 fig_G1),
    ("G2 — Top-20 countries",                  fig_G2),
    ("G3 — Region over time",                  fig_G3),
    ("G4 — Decade comparison",                 fig_G4),
    ("I1 — Top-20 institutions global",        fig_I1),
    ("I2 — Top-20 institutions European",      fig_I2),
    ("I3 — Institutions by author role",       fig_I3),
    ("I4 — Institutions by country",           fig_I4),
    ("J1 — Top-20 journals all",               fig_J1),
    ("J2 — Top-20 journals European",          fig_J2),
    ("X1 — Specialty × study-type heatmap",    fig_X1),
    ("X2 — Dashboard overview",                fig_X2),
]


if __name__ == "__main__":
    rows = build_dataset()
    print(f"\nGenerating {len(FIGURES)} figures...\n")
    errors = []
    for label, fn in FIGURES:
        print(f"  {label}")
        try:
            fn(rows)
        except Exception as exc:
            import traceback
            print(f"  [FAIL] {exc}")
            traceback.print_exc()
            errors.append((label, exc))

    print(f"\n{'='*50}")
    print(f"Done.  {len(FIGURES)-len(errors)}/{len(FIGURES)} figures saved to {FIG_DIR}")
    if errors:
        print("Errors:")
        for lbl, exc in errors:
            print(f"  {lbl}: {exc}")
