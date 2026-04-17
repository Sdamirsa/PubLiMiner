"""Streamlit UI for PubLiMiner.

Single-page app to:
- Load / edit / save the unified `publiminer.yaml`
- Launch the pipeline (subprocess, streaming logs)
- Show current Parquet status (rows, columns, last step meta)

Run:
    py -3.11 -m streamlit run src/publiminer/ui/app.py
"""

from __future__ import annotations

import io
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import streamlit as st
import yaml

# ── Paths ───────────────────────────────────────────────────────────
DEFAULT_YAML = Path("publiminer.yaml")
ALL_STEPS = [
    "fetch",
    "parse",
    "deduplicate",
    "embed",
    "reduce",
    "cluster",
    "sample",
    "extract",
    "score",
    "trend",
    "rag",
    "patent",
    "export",
]
IMPLEMENTED_STEPS = {"fetch", "parse", "deduplicate"}

# Columns shown by default in the explore preview table — small, displayable.
# Excludes raw_xml (huge), abstract (long), and JSON columns (authors, mesh,
# etc.) which are noisy in a table view but available via the multiselect.
DEFAULT_PREVIEW_COLS = [
    "pmid",
    "title",
    "year",
    "doi",
    "language",
    "exclude_flag",
    "exclude_reason",
    "publication_status",
]


def default_config() -> dict:
    return {
        "general": {
            "output_dir": "output",
            "log_level": "INFO",
            "on_error": "skip",
            "max_error_rate": 0.05,
            "seed": 42,
        },
        "cache": {"ttl_days": 90},
        "steps": ["fetch", "parse", "deduplicate"],
        "fetch": {
            "query": "",
            "start_date": "",
            "end_date": "",
            "email": "",
            "api_key": "",
            "max_results": 100,
            "batch_size": 500,
            "retry_attempts": 3,
            "rate_limit_per_second": 3.0,
            "download_mode": "full",
            "ret_mode": "xml",
            "ret_type": "",
        },
        "parse": {
            "min_abstract_length": 0,
            "language_filter": "",
            "remove_html": True,
            "prepare_llm_input": True,
            "flag_exclusions": True,
        },
        "deduplicate": {
            "check_doi": True,
            "check_title_fuzzy": True,
            "fuzzy_threshold": 90,
            "remove_retracted": True,
        },
    }


def load_yaml(path: Path) -> dict:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Merge with defaults so new fields appear
        merged = default_config()
        for k, v in data.items():
            if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
                merged[k].update(v)
            else:
                merged[k] = v
        return merged
    return default_config()


def save_yaml(path: Path, cfg: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False, default_flow_style=False)


def get_parquet_summary(output_dir: str) -> dict | None:
    """Read parquet metadata only — zero row data loaded.

    Replaces the prior ``get_parquet_status`` which did
    ``pl.read_parquet(...)`` (loading all 3+ GB of the production file)
    just to count rows and list columns. With the post-migration
    ``write_statistics=True`` flag, every row group has min/max stats
    available, so this also reports compression and row-group count.
    """
    parquet = Path(output_dir) / "papers.parquet"
    if not parquet.exists():
        return None
    try:
        pf = pq.ParquetFile(parquet)
        meta = pf.metadata
        compression = "?"
        if meta.num_row_groups > 0:
            compression = meta.row_group(0).column(0).compression
        return {
            "rows": meta.num_rows,
            "columns": list(pf.schema_arrow.names),
            "schema": {f.name: str(f.type) for f in pf.schema_arrow},
            "size_mb": round(parquet.stat().st_size / 1024 / 1024, 2),
            "row_groups": meta.num_row_groups,
            "compression": compression,
        }
    except Exception as e:
        return {"error": str(e)}


def get_year_range_from_stats(parquet_path: Path) -> tuple[int, int] | None:
    """Min/max year via row-group statistics — no row data loaded.

    With ~15 row groups in the production file this is ~15 stat lookups
    (sub-millisecond). Returns ``None`` if the year column is absent or
    statistics are missing (pre-migration parquets).
    """
    try:
        pf = pq.ParquetFile(parquet_path)
        if "year" not in pf.schema_arrow.names:
            return None
        year_idx = pf.schema_arrow.get_field_index("year")
        mins: list[int] = []
        maxs: list[int] = []
        for i in range(pf.metadata.num_row_groups):
            stats = pf.metadata.row_group(i).column(year_idx).statistics
            if stats is None or not stats.has_min_max:
                continue
            if stats.min is not None:
                mins.append(int(stats.min))
            if stats.max is not None:
                maxs.append(int(stats.max))
        if mins and maxs:
            return (min(mins), max(maxs))
    except Exception:
        pass
    return None


def read_preview_streaming(
    parquet_path: Path,
    columns: list[str],
    n: int = 10,
) -> pl.DataFrame:
    """Read the first ``n`` rows with column projection — single batch.

    Uses ``pq.ParquetFile.iter_batches`` so only a tiny slice of the
    first row group is decoded (~10 rows × N columns). Replaces the
    prior pattern of ``pl.read_parquet(path).select(cols).head(n)``,
    which materialised the whole file before slicing.
    """
    if not parquet_path.exists():
        return pl.DataFrame()
    try:
        pf = pq.ParquetFile(parquet_path)
        available = set(pf.schema_arrow.names)
        valid = [c for c in columns if c in available]
        if not valid:
            return pl.DataFrame()
        for batch in pf.iter_batches(batch_size=n, columns=valid):
            return pl.from_arrow(batch).head(n)
    except Exception:
        pass
    return pl.DataFrame()


def explore_query(
    parquet_path: Path,
    *,
    year_min: int | None,
    year_max: int | None,
    has_doi: bool,
    has_abstract: bool,
    exclude_choice: str,  # "any" | "included" | "excluded"
    title_contains: str,
    title_use_regex: bool,
    language: str,
    columns: list[str],
    limit: int,
    mode: str,  # "first" | "random" | "stride"
    stride: int = 10,
    seed: int = 42,
) -> tuple[pl.DataFrame, int]:
    """Lazy-filter the spine and return ``(result, total_match_count)``.

    Why lazy: ``pl.scan_parquet`` pushes column projection and predicate
    filters down to row-group statistics. With our 50K-row groups +
    write_statistics=True, a query like "year >= 2020" reads only the
    matching row groups instead of the whole 3 GB file. ``scan_parquet``
    does not mmap (Polars 1.39: no ``memory_map`` parameter), so this is
    Windows-safe even when a pipeline write is queued behind it.

    Modes:
    - "first":   take the first ``limit`` matches in file order.
    - "random":  pick ``limit`` PMIDs uniformly at random from the match set.
    - "stride":  take every ``stride``th PMID from the match set, capped at
                 ``limit`` (deterministic spread sample).

    For random/stride: collect just the ``pmid`` column for all matches
    (~5 MB even for 583K rows of strings), pick the sample, then refetch
    full rows for those PMIDs. Avoids materialising raw_xml or any other
    fat column for non-sampled rows.
    """
    if not parquet_path.exists():
        return pl.DataFrame(), 0

    pf = pq.ParquetFile(parquet_path)
    available = set(pf.schema_arrow.names)
    valid_cols = [c for c in columns if c in available] or ["pmid"]

    def _build_filtered():
        lf = pl.scan_parquet(parquet_path)
        if year_min is not None and "year" in available:
            lf = lf.filter(pl.col("year") >= year_min)
        if year_max is not None and "year" in available:
            lf = lf.filter(pl.col("year") <= year_max)
        if has_doi and "doi" in available:
            lf = lf.filter(pl.col("doi").is_not_null() & (pl.col("doi") != ""))
        if has_abstract and "abstract" in available:
            lf = lf.filter(pl.col("abstract").is_not_null() & (pl.col("abstract") != ""))
        if exclude_choice == "included" and "exclude_flag" in available:
            lf = lf.filter(
                (pl.col("exclude_flag") == False)  # noqa: E712
                | pl.col("exclude_flag").is_null()
            )
        elif exclude_choice == "excluded" and "exclude_flag" in available:
            lf = lf.filter(pl.col("exclude_flag") == True)  # noqa: E712
        if title_contains and "title" in available:
            lf = lf.filter(
                pl.col("title").str.contains(
                    title_contains,
                    literal=not title_use_regex,
                    strict=False,
                )
            )
        if language and "language" in available:
            lf = lf.filter(pl.col("language") == language)
        return lf

    if mode == "first":
        filtered = _build_filtered()
        result = filtered.select(valid_cols).limit(limit).collect()
        # If we got fewer than the cap, that's the true total — skip the
        # second scan. Otherwise count separately (predicate-pushed, fast).
        if len(result) < limit:
            total = len(result)
        else:
            total = _build_filtered().select(pl.len()).collect().item()
        return result, total

    # random / stride: collect matching pmids, pick subset, refetch full rows.
    pmid_match = _build_filtered().select("pmid").collect()
    total = len(pmid_match)
    if total == 0:
        return pl.DataFrame(), 0
    all_pmids = pmid_match["pmid"].to_list()

    if mode == "random":
        rng = random.Random(seed)
        sample_size = min(limit, total)
        chosen = rng.sample(all_pmids, sample_size)
    elif mode == "stride":
        chosen = all_pmids[:: max(1, stride)][:limit]
    else:
        return pl.DataFrame(), total

    if not chosen:
        return pl.DataFrame(), total

    result = (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("pmid").is_in(chosen))
        .select(valid_cols)
        .collect()
    )
    return result, total


def df_to_json_bytes(df: pl.DataFrame) -> bytes:
    """Serialize a (small) DataFrame to JSON bytes for download."""
    records = df.to_pandas().to_dict(orient="records")
    return json.dumps(records, ensure_ascii=False, indent=2, default=str).encode("utf-8")


def df_to_xlsx_bytes(df: pl.DataFrame) -> bytes | None:
    """Serialize a (small) DataFrame to XLSX bytes via xlsxwriter or openpyxl.

    Returns ``None`` if neither engine is installed — caller renders a
    helpful install hint instead of crashing.
    """
    try:
        buf = io.BytesIO()
        df.write_excel(workbook=buf)
        return buf.getvalue()
    except Exception:
        try:
            buf = io.BytesIO()
            df.to_pandas().to_excel(buf, index=False, engine="openpyxl")
            return buf.getvalue()
        except Exception:
            return None


def get_step_logs(output_dir: str) -> list[dict]:
    log_dir = Path(output_dir) / "step_log"
    if not log_dir.exists():
        return []
    out = []
    for f in sorted(log_dir.glob("*.json")):
        try:
            with open(f, encoding="utf-8") as fp:
                out.append(json.load(fp))
        except Exception:
            pass
    return out


# ── Page setup ──────────────────────────────────────────────────────
st.set_page_config(page_title="PubLiMiner", layout="wide", page_icon="📚")
st.title("📚 PubLiMiner Control Panel")

# Sidebar — yaml file path & load/save
with st.sidebar:
    st.header("Config file")
    yaml_path_str = st.text_input("YAML path", value=str(DEFAULT_YAML))
    yaml_path = Path(yaml_path_str)

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📂 Load", width="stretch"):
            st.session_state["cfg"] = load_yaml(yaml_path)
            st.success(f"Loaded {yaml_path}")
    with col_b:
        if st.button("🔄 Reset", width="stretch"):
            st.session_state["cfg"] = default_config()
            st.info("Reset to defaults")

    if yaml_path.exists():
        st.caption(f"✅ File exists ({yaml_path.stat().st_size} bytes)")
    else:
        st.caption("⚠️ File does not exist yet — will be created on save")

# Initialize config in session state
if "cfg" not in st.session_state:
    st.session_state["cfg"] = load_yaml(yaml_path)

cfg = st.session_state["cfg"]

# ── Tabs ────────────────────────────────────────────────────────────
tab_config, tab_run, tab_explore, tab_status = st.tabs(
    ["⚙️ Configure", "▶️ Run", "🔍 Explore", "📊 Status"]
)

# ─────────────── CONFIGURE TAB ──────────────────────────────────────
with tab_config:
    st.subheader("General")
    c1, c2, c3 = st.columns(3)
    with c1:
        cfg["general"]["output_dir"] = st.text_input(
            "Output directory", cfg["general"]["output_dir"]
        )
    with c2:
        cfg["general"]["log_level"] = st.selectbox(
            "Log level",
            ["DEBUG", "INFO", "WARNING", "ERROR"],
            index=["DEBUG", "INFO", "WARNING", "ERROR"].index(cfg["general"]["log_level"]),
        )
    with c3:
        cfg["general"]["on_error"] = st.selectbox(
            "On error",
            ["skip", "fail"],
            index=["skip", "fail"].index(cfg["general"]["on_error"]),
        )

    st.subheader("Steps to run")
    cfg["steps"] = st.multiselect(
        "Pipeline steps (in order)",
        options=ALL_STEPS,
        default=cfg.get("steps", ["fetch", "parse", "deduplicate"]),
        help="Greyed-out items below are not yet implemented.",
    )
    not_yet = [s for s in cfg["steps"] if s not in IMPLEMENTED_STEPS]
    if not_yet:
        st.warning(f"Not yet implemented: {', '.join(not_yet)} — will fail at runtime.")

    st.divider()
    st.subheader("🔍 Fetch (PubMed)")
    f = cfg["fetch"]
    c1, c2 = st.columns(2)
    with c1:
        f["query"] = st.text_area(
            "Query",
            f["query"],
            help="PubMed query syntax, e.g. `diabetes AND machine learning`",
            height=80,
        )
        f["email"] = st.text_input("Email (required by NCBI)", f["email"])
        f["api_key"] = st.text_input(
            "NCBI API key (optional)",
            f["api_key"],
            type="password",
            help="Increases rate limit from 3 to 10 req/sec",
        )
    with c2:
        f["start_date"] = st.text_input("Start date (YYYY/MM/DD)", f["start_date"])
        f["end_date"] = st.text_input("End date (YYYY/MM/DD)", f["end_date"])
        f["max_results"] = st.number_input(
            "Max results (0 = no limit)", min_value=0, value=int(f["max_results"]), step=100
        )
        f["batch_size"] = st.number_input(
            "Batch size", min_value=1, max_value=9900, value=int(f["batch_size"])
        )
        f["rate_limit_per_second"] = st.number_input(
            "Rate limit (req/sec)",
            min_value=0.1,
            value=float(f["rate_limit_per_second"]),
            step=0.5,
        )

    st.divider()
    st.subheader("🧹 Parse")
    p = cfg["parse"]
    c1, c2, c3 = st.columns(3)
    with c1:
        p["min_abstract_length"] = st.number_input(
            "Min abstract length", min_value=0, value=int(p["min_abstract_length"])
        )
    with c2:
        p["language_filter"] = st.text_input("Language filter (e.g. eng)", p["language_filter"])
    with c3:
        p["remove_html"] = st.checkbox("Remove HTML", value=p["remove_html"])
    c1, c2 = st.columns(2)
    with c1:
        p["prepare_llm_input"] = st.checkbox(
            "Prepare LLM input column", value=p["prepare_llm_input"]
        )
    with c2:
        p["flag_exclusions"] = st.checkbox(
            "Flag exclusions (Review/Case Report/Letter)", value=p["flag_exclusions"]
        )

    st.divider()
    st.subheader("🗑️ Deduplicate")
    d = cfg["deduplicate"]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        d["check_doi"] = st.checkbox("Check DOI", value=d["check_doi"])
    with c2:
        d["check_title_fuzzy"] = st.checkbox("Fuzzy title", value=d["check_title_fuzzy"])
    with c3:
        d["fuzzy_threshold"] = st.slider(
            "Fuzzy threshold", min_value=70, max_value=100, value=int(d["fuzzy_threshold"])
        )
    with c4:
        d["remove_retracted"] = st.checkbox("Remove retracted", value=d["remove_retracted"])

    st.divider()
    col_a, col_b = st.columns([1, 3])
    with col_a:
        if st.button("💾 Save YAML", type="primary", width="stretch"):
            save_yaml(yaml_path, cfg)
            st.success(f"Saved to {yaml_path}")
    with col_b, st.expander("Preview YAML"):
        st.code(yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False), language="yaml")

# ─────────────── RUN TAB ────────────────────────────────────────────
with tab_run:
    st.subheader("Run pipeline")

    st.markdown(
        f"**Config:** `{yaml_path}` &nbsp;&nbsp; "
        f"**Output:** `{cfg['general']['output_dir']}` &nbsp;&nbsp; "
        f"**Steps:** `{', '.join(cfg['steps'])}`"
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        save_first = st.checkbox("Save YAML before running", value=True)
    with col_b:
        steps_override = st.text_input("Override steps (comma-separated, optional)", value="")

    if st.button("▶️ Run pipeline", type="primary", width="stretch"):
        if save_first:
            save_yaml(yaml_path, cfg)
            st.info(f"Saved config to {yaml_path}")

        cmd = [
            sys.executable,
            "-m",
            "publiminer.cli",
            "run",
            "--config",
            str(yaml_path),
            "--output",
            cfg["general"]["output_dir"],
        ]
        if steps_override.strip():
            cmd += ["--steps", steps_override.strip()]

        st.code(" ".join(cmd), language="bash")

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUNBUFFERED"] = "1"

        progress_label = st.empty()
        progress_bar = st.empty()
        log_box = st.empty()
        log_lines: list[str] = []

        PROGRESS_PREFIX = "__PROGRESS__ "

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                bufsize=1,
            )
            with st.spinner("Pipeline running…"):
                assert proc.stdout is not None
                for line in proc.stdout:
                    line = line.rstrip()
                    # Intercept progress events
                    if line.startswith(PROGRESS_PREFIX):
                        try:
                            evt = json.loads(line[len(PROGRESS_PREFIX) :])
                            cur = evt.get("current", 0)
                            tot = max(evt.get("total", 1), 1)
                            desc = evt.get("desc", evt.get("step", ""))
                            phase = evt.get("phase", "update")
                            pct = min(cur / tot, 1.0)
                            progress_label.markdown(
                                f"**{desc}** — {cur:,} / {tot:,} ({pct * 100:.0f}%)"
                            )
                            progress_bar.progress(pct)
                            if phase == "end":
                                progress_label.markdown(f"**{desc}** — done ✅")
                        except json.JSONDecodeError:
                            pass
                        continue
                    log_lines.append(line)
                    log_box.code("\n".join(log_lines[-200:]), language="text")
                proc.wait()
            if proc.returncode == 0:
                st.success("✅ Pipeline finished successfully")
            else:
                st.error(f"❌ Pipeline exited with code {proc.returncode}")
        except Exception as e:
            st.error(f"Failed to launch: {e}")

# ─────────────── EXPLORE TAB ────────────────────────────────────────
with tab_explore:
    st.subheader("🔍 Explore the corpus")
    st.caption(
        "Lazy parquet scan — column projection and filters are pushed down to "
        "row-group statistics, so even on a 3 GB / 580K-row file most queries "
        "return in well under a second. Loads only the columns you select."
    )

    output_dir = cfg["general"]["output_dir"]
    parquet_path = Path(output_dir) / "papers.parquet"
    summary = get_parquet_summary(output_dir)

    if summary is None:
        st.warning(f"No `papers.parquet` found in `{output_dir}/`. Run the pipeline first.")
    elif "error" in summary:
        st.error(f"Error reading parquet: {summary['error']}")
    else:
        all_columns: list[str] = summary["columns"]
        year_range = get_year_range_from_stats(parquet_path)

        # ── Filters ──────────────────────────────────────────────
        st.markdown("**Filters**")
        fc1, fc2 = st.columns(2)
        with fc1:
            if year_range and "year" in all_columns:
                ymin, ymax = year_range
                # Guard against degenerate ranges (only one year covered).
                slider_min = min(ymin, ymax - 1) if ymax > ymin else ymin - 1
                slider_max = max(ymax, ymin + 1) if ymax > ymin else ymax + 1
                year_sel = st.slider(
                    "Year range",
                    min_value=slider_min,
                    max_value=slider_max,
                    value=(ymin, ymax),
                    help="Min/max derived from parquet column statistics — no scan.",
                )
                year_min, year_max = year_sel
            else:
                year_min, year_max = None, None
                if "year" in all_columns:
                    st.caption("⚠️ Year statistics unavailable — filter disabled.")

            title_contains = st.text_input(
                "Title contains",
                value="",
                help="Case-insensitive substring (or regex if box below is checked).",
            )
            title_use_regex = st.checkbox(
                "Treat as regex",
                value=False,
                help="Off (default): literal substring. On: full regex syntax.",
            )

        with fc2:
            language = st.text_input(
                "Language code (e.g. `eng`)",
                value="",
                help="Exact match. Leave empty to include all languages.",
            )
            ec1, ec2, ec3 = st.columns(3)
            with ec1:
                has_doi = st.checkbox(
                    "Has DOI",
                    value=False,
                    disabled="doi" not in all_columns,
                )
            with ec2:
                has_abstract = st.checkbox(
                    "Has abstract",
                    value=False,
                    disabled="abstract" not in all_columns,
                )
            with ec3:
                exclude_choice = st.selectbox(
                    "Exclude flag",
                    ["any", "included", "excluded"],
                    index=0,
                    help=(
                        "any: ignore exclude_flag. "
                        "included: keep papers NOT flagged for exclusion. "
                        "excluded: keep ONLY flagged papers."
                    ),
                    disabled="exclude_flag" not in all_columns,
                )

        st.divider()

        # ── Sample mode + columns ────────────────────────────────
        st.markdown("**Sample**")
        mc1, mc2, mc3 = st.columns([2, 1, 1])
        with mc1:
            mode = st.radio(
                "Mode",
                ["first", "random", "stride"],
                horizontal=True,
                format_func=lambda x: {
                    "first": "First N",
                    "random": "Random N",
                    "stride": "Every Xth",
                }[x],
                help=(
                    "First N: take the first N matches in file order. "
                    "Random N: uniform random sample (deterministic via seed). "
                    "Every Xth: take every Xth match (deterministic spread)."
                ),
            )
        with mc2:
            limit = st.number_input(
                "N",
                min_value=1,
                max_value=10_000,
                value=100,
                step=50,
            )
        with mc3:
            if mode == "random":
                seed = st.number_input(
                    "Seed",
                    min_value=0,
                    value=42,
                    step=1,
                    help="Same seed → same sample (reproducible).",
                )
                stride = 10
            elif mode == "stride":
                stride = st.number_input(
                    "Stride X",
                    min_value=1,
                    value=10,
                    step=1,
                    help="Take every Xth row from the match set.",
                )
                seed = 42
            else:
                seed = 42
                stride = 10

        # Default selection: small displayable set; user can opt into others
        # (raw_xml stays opt-in by extra warning since it's huge).
        default_cols = [c for c in DEFAULT_PREVIEW_COLS if c in all_columns]
        col_options = [c for c in all_columns if c != "raw_xml"]
        columns_sel = st.multiselect(
            "Columns to include",
            options=col_options,
            default=default_cols,
            help="raw_xml is hidden by default — toggle it on below if needed.",
        )
        include_raw_xml = False
        if "raw_xml" in all_columns:
            include_raw_xml = st.checkbox(
                "Also include `raw_xml` (large — slows download significantly)",
                value=False,
            )
        if include_raw_xml:
            columns_sel = list(columns_sel) + ["raw_xml"]
        if not columns_sel:
            columns_sel = ["pmid"]

        # ── Run query ────────────────────────────────────────────
        run_query = st.button("🔍 Run query", type="primary", width="stretch")
        if run_query:
            try:
                with st.spinner("Scanning parquet..."):
                    t0 = time.time()
                    result_df, total = explore_query(
                        parquet_path,
                        year_min=year_min,
                        year_max=year_max,
                        has_doi=has_doi,
                        has_abstract=has_abstract,
                        exclude_choice=exclude_choice,
                        title_contains=title_contains.strip(),
                        title_use_regex=title_use_regex,
                        language=language.strip(),
                        columns=list(columns_sel),
                        limit=int(limit),
                        mode=mode,
                        stride=int(stride),
                        seed=int(seed),
                    )
                    elapsed = time.time() - t0
                st.session_state["explore_result"] = result_df
                st.session_state["explore_total"] = total
                st.session_state["explore_elapsed"] = elapsed
                st.session_state["explore_mode"] = mode
            except Exception as e:
                st.error(f"Query failed: {e}")
                st.session_state.pop("explore_result", None)

        # ── Result display + downloads ───────────────────────────
        if "explore_result" in st.session_state:
            result_df = st.session_state["explore_result"]
            total = st.session_state["explore_total"]
            elapsed = st.session_state.get("explore_elapsed", 0.0)
            saved_mode = st.session_state.get("explore_mode", "first")

            if total == 0:
                st.warning("No rows matched the filters.")
            else:
                pct = 100.0 * len(result_df) / total if total else 0.0
                st.success(
                    f"**{len(result_df):,}** rows shown "
                    f"(mode: `{saved_mode}`, {pct:.1f}% of "
                    f"**{total:,}** matches) — query took {elapsed:.2f}s"
                )
                st.dataframe(result_df.to_pandas(), width="stretch")

                # Downloads — only build when requested
                ts = time.strftime("%Y%m%d_%H%M%S")
                d1, d2 = st.columns(2)
                with d1:
                    try:
                        st.download_button(
                            "⬇️ Download JSON",
                            data=df_to_json_bytes(result_df),
                            file_name=f"publiminer_explore_{ts}.json",
                            mime="application/json",
                            width="stretch",
                        )
                    except Exception as e:
                        st.error(f"JSON build failed: {e}")
                with d2:
                    xlsx_bytes = df_to_xlsx_bytes(result_df)
                    if xlsx_bytes is not None:
                        st.download_button(
                            "⬇️ Download XLSX",
                            data=xlsx_bytes,
                            file_name=f"publiminer_explore_{ts}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            width="stretch",
                        )
                    else:
                        st.warning(
                            "XLSX export needs `xlsxwriter` or `openpyxl`. "
                            "Install with: `uv pip install xlsxwriter`."
                        )

# ─────────────── STATUS TAB ─────────────────────────────────────────
with tab_status:
    st.subheader("Database status")
    output_dir = cfg["general"]["output_dir"]
    summary = get_parquet_summary(output_dir)

    if summary is None:
        st.warning(f"No `papers.parquet` found in `{output_dir}/`. Run the pipeline first.")
    elif "error" in summary:
        st.error(f"Error reading parquet: {summary['error']}")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total papers", f"{summary['rows']:,}")
        c2.metric("Columns", len(summary["columns"]))
        c3.metric("File size", f"{summary['size_mb']} MB")
        c4.metric("Row groups", summary["row_groups"])
        st.caption(
            f"Compression: **{summary['compression']}** — "
            f"{summary['rows'] // max(summary['row_groups'], 1):,} rows per group avg. "
            "(Run `scripts/migrate_parquet.py` once if compression is SNAPPY or "
            "row groups are < 10 — required for streaming.)"
        )

        with st.expander("Schema", expanded=False):
            st.json(summary["schema"])

        # Streamed preview — reads only ~10 rows × preview_cols of the
        # first row group, not the whole 3 GB file.
        with st.expander("Sample rows (first 10)", expanded=False):
            preview_cols = [c for c in DEFAULT_PREVIEW_COLS if c in summary["columns"]]
            preview_df = read_preview_streaming(
                Path(output_dir) / "papers.parquet",
                preview_cols,
                n=10,
            )
            if len(preview_df) == 0:
                st.info("No rows to preview.")
            else:
                st.dataframe(preview_df.to_pandas(), width="stretch")
                st.caption("Use the **🔍 Explore** tab for filtered samples and exports.")

    st.divider()
    st.subheader("Step run history")
    logs = get_step_logs(output_dir)
    if not logs:
        st.info("No step logs yet.")
    else:
        for meta in logs:
            name = meta.get("step_name", "?")
            status_str = meta.get("status", "?")
            duration = meta.get("duration_seconds", "?")
            rows_b = meta.get("rows_before", "?")
            rows_a = meta.get("rows_after", "?")
            icon = "✅" if status_str == "completed" else "❌"
            with st.expander(f"{icon} {name} — {status_str} ({duration}s, {rows_b} → {rows_a})"):
                st.json(meta)

    if st.button("🔄 Refresh status"):
        st.rerun()
