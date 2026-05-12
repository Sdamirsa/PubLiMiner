#!/usr/bin/env python3
"""
G1b  World choropleth — total articles (top) / original research (bottom)
G1c  World choropleth — Annual Percent Change (APC), total (top) / original (bottom)

Standalone script; mirrors path/schema constants from generate_analysis.py.
Run:  python generate_G1b_G1c_maps.py

Requirements:  geopandas  matplotlib  numpy  scipy  polars
    pip install geopandas
"""
from __future__ import annotations

import json
import sqlite3
import sys
import warnings
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from scipy import stats

warnings.filterwarnings("ignore")

try:
    import geopandas as gpd
except ImportError:
    raise SystemExit("geopandas is required:  pip install geopandas")

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

SCHEMA, RUN_ID = _load_run_meta(BASE_DIR)
PROJECT_NAME   = " ".join(
    w.upper() if w.lower() in ("ct", "mri", "pet") else w.title()
    for w in BASE_DIR.name.replace("_", " ").split()
)
YEAR_MIN = 2001
YEAR_MAX = 2026

# ── Country detection (mirrored from generate_analysis.py) ───────────────────
COUNTRY_REGION: dict[str, str] = {
    "united states": "North America",   "canada":        "North America",
    "germany":       "Western Europe",  "france":        "Western Europe",
    "netherlands":   "Western Europe",  "belgium":       "Western Europe",
    "switzerland":   "Western Europe",  "austria":       "Western Europe",
    "luxembourg":    "Western Europe",  "liechtenstein": "Western Europe",
    "united kingdom":"Northern Europe", "england":       "Northern Europe",
    "scotland":      "Northern Europe", "wales":         "Northern Europe",
    "ireland":       "Northern Europe", "sweden":        "Northern Europe",
    "norway":        "Northern Europe", "denmark":       "Northern Europe",
    "finland":       "Northern Europe", "iceland":       "Northern Europe",
    "italy":         "Southern Europe", "spain":         "Southern Europe",
    "portugal":      "Southern Europe", "greece":        "Southern Europe",
    "malta":         "Southern Europe", "cyprus":        "Southern Europe",
    "san marino":    "Southern Europe", "monaco":        "Southern Europe",
    "andorra":       "Southern Europe",
    "poland":        "Eastern Europe",  "czech republic":"Eastern Europe",
    "czechia":       "Eastern Europe",  "hungary":       "Eastern Europe",
    "romania":       "Eastern Europe",  "bulgaria":      "Eastern Europe",
    "croatia":       "Eastern Europe",  "serbia":        "Eastern Europe",
    "slovakia":      "Eastern Europe",  "slovenia":      "Eastern Europe",
    "estonia":       "Eastern Europe",  "latvia":        "Eastern Europe",
    "lithuania":     "Eastern Europe",  "albania":       "Eastern Europe",
    "belarus":       "Eastern Europe",  "bosnia":        "Eastern Europe",
    "kosovo":        "Eastern Europe",  "moldova":       "Eastern Europe",
    "montenegro":    "Eastern Europe",  "north macedonia":"Eastern Europe",
    "ukraine":       "Eastern Europe",  "russia":        "Eastern Europe",
    "georgia":       "Eastern Europe",
    "china":         "East Asia",       "japan":         "East Asia",
    "south korea":   "East Asia",       "korea":         "East Asia",
    "taiwan":        "East Asia",       "hong kong":     "East Asia",
    "singapore":     "East Asia",
    "india":         "South/SE Asia",   "pakistan":      "South/SE Asia",
    "bangladesh":    "South/SE Asia",   "thailand":      "South/SE Asia",
    "malaysia":      "South/SE Asia",   "indonesia":     "South/SE Asia",
    "vietnam":       "South/SE Asia",   "philippines":   "South/SE Asia",
    "sri lanka":     "South/SE Asia",   "nepal":         "South/SE Asia",
    "turkey":        "Middle East",     "iran":          "Middle East",
    "israel":        "Middle East",     "saudi arabia":  "Middle East",
    "jordan":        "Middle East",     "lebanon":       "Middle East",
    "egypt":         "Middle East",     "qatar":         "Middle East",
    "united arab emirates":"Middle East","uae":          "Middle East",
    "kuwait":        "Middle East",     "bahrain":       "Middle East",
    "oman":          "Middle East",     "iraq":          "Middle East",
    "syria":         "Middle East",
    "australia":     "Oceania",         "new zealand":   "Oceania",
    "brazil":        "Latin America",   "argentina":     "Latin America",
    "mexico":        "Latin America",   "chile":         "Latin America",
    "colombia":      "Latin America",   "peru":          "Latin America",
    "venezuela":     "Latin America",   "cuba":          "Latin America",
    "south africa":  "Africa",          "nigeria":       "Africa",
    "kenya":         "Africa",          "ghana":         "Africa",
    "ethiopia":      "Africa",          "morocco":       "Africa",
    "tunisia":       "Africa",          "algeria":       "Africa",
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


def detect_country(aff: str) -> str:
    if not aff:
        return "unknown"
    al = aff.lower().strip().rstrip(".")
    for ind in US_CITY_STATE:
        if ind in al:
            return "united states"
    for ctry in sorted(COUNTRY_REGION, key=len, reverse=True):
        if ctry in al:
            return ctry
    if al.endswith("usa") or " usa" in al or ", usa" in al:
        return "united states"
    return "unknown"


def parse_authors(s: str | None) -> list[dict]:
    if not s:
        return []
    try:
        return json.loads(s)
    except Exception:
        return []


def get_affiliation(author: dict) -> str:
    aff = author.get("affiliations") or author.get("affiliation") or ""
    if isinstance(aff, list):
        return aff[0] if aff else ""
    return str(aff) if aff else ""


# ── Internal name → Natural Earth 'name' column ───────────────────────────────
# Entries needed only when title-casing the internal name does NOT match NE.
_NE_BRIDGE: dict[str, str] = {
    "united states":        "United States of America",
    "united kingdom":       "United Kingdom",
    "england":              "United Kingdom",
    "scotland":             "United Kingdom",
    "wales":                "United Kingdom",
    "south korea":          "South Korea",
    "korea":                "South Korea",
    "czech republic":       "Czechia",
    "czechia":              "Czechia",
    "north macedonia":      "North Macedonia",
    "south africa":         "South Africa",
    "new zealand":          "New Zealand",
    "saudi arabia":         "Saudi Arabia",
    "united arab emirates": "United Arab Emirates",
    "uae":                  "United Arab Emirates",
    # Hong Kong has no separate polygon at 110m; attribute to China
    "hong kong":            "China",
    # Singapore is too small for 110m resolution; skip
    "singapore":            None,
    "bosnia":               "Bosnia and Herz.",
    "sri lanka":            "Sri Lanka",
}


def to_ne_name(name: str) -> str | None:
    """
    Convert internal lowercase country name to Natural Earth NAME value.
    Returns None for territories too small for 110m resolution.
    """
    if name in _NE_BRIDGE:
        return _NE_BRIDGE[name]
    return name.title()


# ── Data loading ──────────────────────────────────────────────────────────────

def build_country_year() -> tuple[
    dict[str, dict[int, int]],   # total[country][year] = n
    dict[str, dict[int, int]],   # orig[country][year]  = n
]:
    """Aggregate papers by country × year from Parquet + extractions DB."""
    print("Loading papers …")
    papers = pl.read_parquet(PARQUET, columns=["pmid", "year", "authors"])

    print("Loading extractions …")
    conn = sqlite3.connect(EXTRACTIONS_DB)
    db_rows = conn.execute(
        "SELECT pmid, extracted_json FROM extractions "
        "WHERE schema_name=? AND run_id=? AND extracted_json IS NOT NULL",
        (SCHEMA, RUN_ID),
    ).fetchall()
    conn.close()

    study_map: dict[str, str] = {}
    for pmid, ejson in db_rows:
        try:
            d = json.loads(ejson)
            if isinstance(d, dict):
                study_map[pmid] = str(d.get("study_type") or "Other")
        except Exception:
            pass

    pmid_set = set(study_map)
    df = (
        papers
        .filter(pl.col("pmid").is_in(pmid_set))
        .filter(pl.col("year").is_not_null())
        .with_columns(pl.col("year").cast(pl.Int32))
        .to_dicts()
    )
    print(f"  {len(df):,} papers after join")

    total: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    orig:  dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    for row in df:
        year    = int(row["year"])
        is_orig = study_map.get(row["pmid"], "Other") == "OriginalResearch"
        authors = parse_authors(row.get("authors"))
        seen: set[str] = set()
        for a in authors:
            aff = get_affiliation(a)
            if aff:
                c = detect_country(aff.split(";")[0])
                if c != "unknown":
                    seen.add(c)
        for c in seen:
            total[c][year] += 1
            if is_orig:
                orig[c][year] += 1

    return dict(total), dict(orig)


# ── Annual Percent Change ─────────────────────────────────────────────────────

def compute_apc(
    year_counts: dict[int, int],
    year_range: range,
    min_years:  int = 6,
    min_papers: int = 15,
) -> float | None:
    """
    APC via log-linear OLS (standard epidemiological method):
        ln(n + 0.5) = α + β·year  →  APC = (exp(β) − 1) × 100  [%]

    Returns None when data are insufficient (< min_years years with >0 papers
    or < min_papers total papers).
    """
    counts = [year_counts.get(y, 0) for y in year_range]
    if sum(1 for c in counts if c > 0) < min_years:
        return None
    if sum(counts) < min_papers:
        return None
    x = np.array(list(year_range), dtype=float)
    x = x - x.mean()                                      # centre for stability
    y = np.log(np.array(counts, dtype=float) + 0.5)
    slope, *_ = stats.linregress(x, y)
    return float((np.exp(slope) - 1) * 100)


# ── Natural Earth world loader ────────────────────────────────────────────────

_NE_110M_URL = (
    "https://naciscdn.org/naturalearth/110m/cultural/"
    "ne_110m_admin_0_countries.zip"
)


def load_world_robinson() -> gpd.GeoDataFrame:
    """
    Load Natural Earth 110m countries, Robinson projection, Antarctica removed.
    Normalises the name column to 'name' regardless of source.
    """
    world: gpd.GeoDataFrame | None = None

    # 1. Try deprecated geopandas built-in (geopandas < 1.0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
        except AttributeError:
            pass

    # 2. Download 110m countries from Natural Earth CDN (geopandas >= 1.0)
    if world is None:
        print(f"  Downloading Natural Earth 110m from {_NE_110M_URL} …")
        try:
            world = gpd.read_file(_NE_110M_URL)
            # 110m dataset uses uppercase 'NAME'; normalise to 'name'
            if "NAME" in world.columns and "name" not in world.columns:
                world = world.rename(columns={"NAME": "name"})
        except Exception as exc:
            raise SystemExit(f"Cannot load Natural Earth data: {exc}")

    world = world[world["name"] != "Antarctica"].copy()
    return world.to_crs("+proj=robin")


# ── Single-panel choropleth helper ────────────────────────────────────────────

def _choropleth_panel(
    ax: plt.Axes,
    world: gpd.GeoDataFrame,
    ne_values: dict[str, float],
    cmap: str,
    norm: mcolors.Normalize,
    title: str,
    no_data_color: str = "#CCCCCC",
) -> plt.cm.ScalarMappable:
    """
    Draw one choropleth panel onto `ax`.
    Returns a ScalarMappable suitable for plt.colorbar().
    """
    wld = world.copy()
    wld["_val"] = wld["name"].map(ne_values)

    no_data = wld[wld["_val"].isna()]
    has_data = wld[wld["_val"].notna()]

    if not no_data.empty:
        no_data.plot(ax=ax, color=no_data_color, edgecolor="#AAAAAA", linewidth=0.25)
    if not has_data.empty:
        has_data.plot(
            ax=ax,
            column="_val",
            cmap=cmap,
            norm=norm,
            edgecolor="#666666",
            linewidth=0.3,
            legend=False,
        )

    ax.set_title(title, fontsize=11, fontweight="bold", pad=8)
    ax.set_axis_off()

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    return sm


# ── Figure G1b ────────────────────────────────────────────────────────────────

def fig_G1b(
    total_cy: dict[str, dict[int, int]],
    orig_cy:  dict[str, dict[int, int]],
    world:    gpd.GeoDataFrame,
) -> None:
    """
    Two-panel world choropleth (log colour scale, shared across panels):
      Top    — total Cardiac CT publications per country
      Bottom — original research articles per country
    """
    total_cnt = {c: sum(yc.values()) for c, yc in total_cy.items()}
    orig_cnt  = {c: sum(yc.values()) for c, yc in orig_cy.items()}

    # to_ne_name may return None for territories not on 110m map (e.g. Singapore)
    total_ne: dict[str, int] = {}
    for c, v in total_cnt.items():
        ne = to_ne_name(c)
        if ne is not None:
            total_ne[ne] = total_ne.get(ne, 0) + v
    orig_ne: dict[str, int] = {}
    for c, v in orig_cnt.items():
        ne = to_ne_name(c)
        if ne is not None:
            orig_ne[ne] = orig_ne.get(ne, 0) + v

    # Shared log-norm: same scale in both panels for direct comparison
    all_vals = [v for v in list(total_ne.values()) + list(orig_ne.values()) if v > 0]
    vmin = max(1, min(all_vals))
    vmax = max(all_vals)
    norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)

    fig, axes = plt.subplots(2, 1, figsize=(16, 14))
    fig.patch.set_facecolor("white")

    sm = _choropleth_panel(
        axes[0], world, total_ne, "YlOrRd", norm,
        title=f"Total {PROJECT_NAME} Publications per Country  (2001–2026,  n = {sum(total_cnt.values()):,})",
    )
    _choropleth_panel(
        axes[1], world, orig_ne, "YlOrRd", norm,
        title=f"Original Research Articles per Country  (2001–2026,  n = {sum(orig_cnt.values()):,})",
    )

    # Shared horizontal colorbar
    cbar_ax = fig.add_axes([0.15, 0.035, 0.70, 0.016])
    cbar = fig.colorbar(sm, cax=cbar_ax, orientation="horizontal")
    cbar.set_label("Number of papers  (log scale)", fontsize=9)
    cbar.ax.tick_params(labelsize=8)

    # No-data legend on top panel
    axes[0].legend(
        handles=[mpatches.Patch(color="#CCCCCC", label="No data / not classified")],
        loc="lower left", fontsize=8, framealpha=0.85,
    )

    plt.suptitle(
        f"Geographic Distribution of {PROJECT_NAME} Literature — World Choropleth",
        fontsize=13, fontweight="bold",
    )
    plt.subplots_adjust(hspace=0.06, top=0.94, bottom=0.07, left=0.02, right=0.98)

    out = FIG_DIR / "G1b_country_map_world.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close("all")
    print(f"  [OK] G1b_country_map_world.png")


# ── Figure G1c ────────────────────────────────────────────────────────────────

def fig_G1c(
    total_cy: dict[str, dict[int, int]],
    orig_cy:  dict[str, dict[int, int]],
    world:    gpd.GeoDataFrame,
) -> None:
    """
    Two-panel world choropleth (diverging colour scale, shared across panels):
      Top    — APC for total publications
      Bottom — APC for original research articles

    APC = Annual Percent Change, estimated by log-linear OLS regression:
        ln(n + 0.5) = α + β·year  →  APC = (exp(β) − 1) × 100 [%]
    Minimum: ≥ 6 years with ≥ 1 paper, ≥ 15 papers total.
    """
    yr_range = range(YEAR_MIN, YEAR_MAX + 1)

    total_apc_ne: dict[str, float] = {}
    orig_apc_ne:  dict[str, float] = {}

    for c, yc in total_cy.items():
        ne = to_ne_name(c)
        if ne is None:
            continue
        v = compute_apc(yc, yr_range)
        if v is not None:
            total_apc_ne[ne] = v

    for c, yc in orig_cy.items():
        ne = to_ne_name(c)
        if ne is None:
            continue
        v = compute_apc(yc, yr_range)
        if v is not None:
            orig_apc_ne[ne] = v

    print(f"  APC computed for {len(total_apc_ne)} countries (total), "
          f"{len(orig_apc_ne)} (original)")

    # Colour range: data-driven p5/p95 with reasonable hard limits
    all_apcs = list(total_apc_ne.values()) + list(orig_apc_ne.values())
    p5, p95  = np.percentile(all_apcs, [5, 95])
    vmin = max(-25.0, float(p5))
    vmax = min(45.0,  float(p95))
    # Ensure 0 is strictly between vmin and vmax
    if vmin >= 0:
        vmin = -2.0
    if vmax <= 0:
        vmax = 2.0
    norm = mcolors.TwoSlopeNorm(vcenter=0.0, vmin=vmin, vmax=vmax)

    fig, axes = plt.subplots(2, 1, figsize=(16, 14))
    fig.patch.set_facecolor("white")

    n_total = len(total_apc_ne)
    n_orig  = len(orig_apc_ne)

    _choropleth_panel(
        axes[0], world, total_apc_ne, "RdBu_r", norm,
        title=(
            f"Annual Percent Change — Total Publications  "
            f"({n_total} countries, 2001–2026)"
        ),
    )
    _choropleth_panel(
        axes[1], world, orig_apc_ne, "RdBu_r", norm,
        title=(
            f"Annual Percent Change — Original Research Articles  "
            f"({n_orig} countries, 2001–2026)"
        ),
    )

    # Shared diverging colorbar
    cbar_ax = fig.add_axes([0.15, 0.035, 0.70, 0.016])
    sm_cb = plt.cm.ScalarMappable(cmap="RdBu_r", norm=norm)
    sm_cb.set_array([])
    cbar = fig.colorbar(sm_cb, cax=cbar_ax, orientation="horizontal", extend="both")
    cbar.set_label("Annual Percent Change  (%/year)", fontsize=9)
    cbar.ax.tick_params(labelsize=8)

    # No-data / insufficient-data legend on top panel
    axes[0].legend(
        handles=[
            mpatches.Patch(color="#CCCCCC",
                           label="Insufficient data  (< 6 yrs with papers  or  < 15 total)"),
        ],
        loc="lower left", fontsize=8, framealpha=0.85,
    )

    plt.suptitle(
        f"Annual Percent Change (APC) in {PROJECT_NAME} Publications — World Choropleth\n"
        r"Method: log-linear OLS  [$\ln(n{+}0.5)=\alpha+\beta\cdot year$,"
        r"  APC$=(e^\beta-1)\times100$]",
        fontsize=11, fontweight="bold",
    )
    plt.subplots_adjust(hspace=0.06, top=0.92, bottom=0.07, left=0.02, right=0.98)

    out = FIG_DIR / "G1c_country_map_APC.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close("all")
    print(f"  [OK] G1c_country_map_APC.png")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total_cy, orig_cy = build_country_year()

    print("\nLoading world geodataframe …")
    world = load_world_robinson()
    print(f"  {len(world)} countries / territories loaded")

    print("\nGenerating G1b …")
    fig_G1b(total_cy, orig_cy, world)

    print("Generating G1c …")
    fig_G1c(total_cy, orig_cy, world)

    print("\nDone.  Figures saved to:", FIG_DIR)
