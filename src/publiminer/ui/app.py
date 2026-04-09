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
import subprocess
import sys
import time
from pathlib import Path

import streamlit as st
import yaml

# ── Paths ───────────────────────────────────────────────────────────
DEFAULT_YAML = Path("publiminer.yaml")
ALL_STEPS = [
    "fetch", "parse", "deduplicate", "embed", "reduce",
    "cluster", "sample", "extract", "score", "trend",
    "rag", "patent", "export",
]
IMPLEMENTED_STEPS = {"fetch", "parse", "deduplicate"}


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


def get_parquet_status(output_dir: str) -> dict | None:
    parquet = Path(output_dir) / "papers.parquet"
    if not parquet.exists():
        return None
    try:
        import polars as pl
        df = pl.read_parquet(parquet)
        return {
            "rows": len(df),
            "columns": df.columns,
            "size_mb": round(parquet.stat().st_size / 1024 / 1024, 2),
            "schema": {c: str(df.schema[c]) for c in df.columns},
        }
    except Exception as e:
        return {"error": str(e)}


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
        if st.button("📂 Load", use_container_width=True):
            st.session_state["cfg"] = load_yaml(yaml_path)
            st.success(f"Loaded {yaml_path}")
    with col_b:
        if st.button("🔄 Reset", use_container_width=True):
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
tab_config, tab_run, tab_status = st.tabs(["⚙️ Configure", "▶️ Run", "📊 Status"])

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
            "Log level", ["DEBUG", "INFO", "WARNING", "ERROR"],
            index=["DEBUG", "INFO", "WARNING", "ERROR"].index(cfg["general"]["log_level"]),
        )
    with c3:
        cfg["general"]["on_error"] = st.selectbox(
            "On error", ["skip", "fail"],
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
            "Query", f["query"],
            help="PubMed query syntax, e.g. `diabetes AND machine learning`",
            height=80,
        )
        f["email"] = st.text_input("Email (required by NCBI)", f["email"])
        f["api_key"] = st.text_input(
            "NCBI API key (optional)", f["api_key"], type="password",
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
            "Rate limit (req/sec)", min_value=0.1, value=float(f["rate_limit_per_second"]),
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
        p["language_filter"] = st.text_input(
            "Language filter (e.g. eng)", p["language_filter"]
        )
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
        d["check_title_fuzzy"] = st.checkbox(
            "Fuzzy title", value=d["check_title_fuzzy"]
        )
    with c3:
        d["fuzzy_threshold"] = st.slider(
            "Fuzzy threshold", min_value=70, max_value=100, value=int(d["fuzzy_threshold"])
        )
    with c4:
        d["remove_retracted"] = st.checkbox(
            "Remove retracted", value=d["remove_retracted"]
        )

    st.divider()
    col_a, col_b = st.columns([1, 3])
    with col_a:
        if st.button("💾 Save YAML", type="primary", use_container_width=True):
            save_yaml(yaml_path, cfg)
            st.success(f"Saved to {yaml_path}")
    with col_b:
        with st.expander("Preview YAML"):
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
        steps_override = st.text_input(
            "Override steps (comma-separated, optional)", value=""
        )

    if st.button("▶️ Run pipeline", type="primary", use_container_width=True):
        if save_first:
            save_yaml(yaml_path, cfg)
            st.info(f"Saved config to {yaml_path}")

        cmd = [
            sys.executable, "-m", "publiminer.cli", "run",
            "--config", str(yaml_path),
            "--output", cfg["general"]["output_dir"],
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
                            evt = json.loads(line[len(PROGRESS_PREFIX):])
                            cur = evt.get("current", 0)
                            tot = max(evt.get("total", 1), 1)
                            desc = evt.get("desc", evt.get("step", ""))
                            phase = evt.get("phase", "update")
                            pct = min(cur / tot, 1.0)
                            progress_label.markdown(
                                f"**{desc}** — {cur:,} / {tot:,} ({pct*100:.0f}%)"
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

# ─────────────── STATUS TAB ─────────────────────────────────────────
with tab_status:
    st.subheader("Database status")
    output_dir = cfg["general"]["output_dir"]
    status = get_parquet_status(output_dir)

    if status is None:
        st.warning(f"No `papers.parquet` found in `{output_dir}/`. Run the pipeline first.")
    elif "error" in status:
        st.error(f"Error reading parquet: {status['error']}")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total papers", f"{status['rows']:,}")
        c2.metric("Columns", len(status["columns"]))
        c3.metric("File size", f"{status['size_mb']} MB")

        with st.expander("Schema", expanded=False):
            st.json(status["schema"])

        # Show sample rows
        with st.expander("Sample rows (first 10)", expanded=False):
            try:
                import polars as pl
                df = pl.read_parquet(Path(output_dir) / "papers.parquet")
                preview_cols = [c for c in ["pmid", "title", "year", "doi",
                                             "language", "exclude_flag", "exclude_reason"]
                                if c in df.columns]
                st.dataframe(df.select(preview_cols).head(10).to_pandas())
            except Exception as e:
                st.error(str(e))

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

    st.divider()
    st.subheader("📤 Export sample")
    st.caption(
        "Take every Xth row (sampling factor) up to a maximum of N rows. "
        "E.g. factor=10 keeps rows 1, 11, 21, ... — useful for previewing or LLM-grade sampling."
    )

    parquet_path = Path(output_dir) / "papers.parquet"
    if not parquet_path.exists():
        st.info("No parquet yet — run the pipeline first.")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            sampling_factor = st.number_input(
                "Sampling factor (X)", min_value=1, value=10, step=1,
                help="Take 1 every X rows (1 = every row)",
            )
        with c2:
            max_rows = st.number_input(
                "Max rows (N)", min_value=1, value=500, step=50,
                help="Stop after N sampled rows are collected",
            )
        with c3:
            include_xml = st.checkbox(
                "Include raw_xml", value=False,
                help="Excludes raw_xml by default — it's huge",
            )

        if st.button("🎲 Build sample", type="primary"):
            try:
                import polars as pl
                df = pl.read_parquet(parquet_path)
                total = len(df)

                # Drop raw_xml unless requested
                if not include_xml and "raw_xml" in df.columns:
                    df = df.drop("raw_xml")

                # Take every Xth row starting from index 0 (row 1 in 1-indexed terms)
                sampled = df.gather_every(int(sampling_factor))

                # Cap at N
                if len(sampled) > max_rows:
                    sampled = sampled.head(int(max_rows))

                st.success(
                    f"Sampled {len(sampled):,} rows "
                    f"(from {total:,} total, factor={sampling_factor}, cap={max_rows})"
                )
                st.dataframe(sampled.head(20).to_pandas(), use_container_width=True)

                st.session_state["sample_df"] = sampled
                st.session_state["sample_total"] = total
            except Exception as e:
                st.error(f"Sampling failed: {e}")

        # Persist downloads across reruns
        if "sample_df" in st.session_state:
            sampled = st.session_state["sample_df"]
            ts = time.strftime("%Y%m%d_%H%M%S")

            # JSON download (records orientation, UTF-8)
            try:
                records = sampled.to_pandas().to_dict(orient="records")
                json_bytes = json.dumps(
                    records, ensure_ascii=False, indent=2, default=str
                ).encode("utf-8")
            except Exception as e:
                json_bytes = None
                st.error(f"JSON build failed: {e}")

            # XLSX download via polars.write_excel (xlsxwriter) with pandas fallback
            xlsx_bytes: bytes | None = None
            try:
                buf = io.BytesIO()
                sampled.write_excel(workbook=buf)
                xlsx_bytes = buf.getvalue()
            except Exception:
                try:
                    buf = io.BytesIO()
                    sampled.to_pandas().to_excel(buf, index=False, engine="openpyxl")
                    xlsx_bytes = buf.getvalue()
                except Exception as e2:
                    st.warning(
                        f"XLSX export needs `xlsxwriter` or `openpyxl`. "
                        f"Install with: `py -3.11 -m pip install xlsxwriter`. ({e2})"
                    )

            d1, d2 = st.columns(2)
            with d1:
                if json_bytes is not None:
                    st.download_button(
                        "⬇️ Download JSON",
                        data=json_bytes,
                        file_name=f"publiminer_sample_{ts}.json",
                        mime="application/json",
                        use_container_width=True,
                    )
            with d2:
                if xlsx_bytes is not None:
                    st.download_button(
                        "⬇️ Download XLSX",
                        data=xlsx_bytes,
                        file_name=f"publiminer_sample_{ts}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
