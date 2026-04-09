# PubLiMiner

> Publication Literature Miner — fetch, parse, deduplicate, embed, cluster, and extract structured data from PubMed at scale.

[![PyPI](https://img.shields.io/pypi/v/publiminer.svg)](https://pypi.org/project/publiminer/)
[![Python](https://img.shields.io/pypi/pyversions/publiminer.svg)](https://pypi.org/project/publiminer/)
[![CI](https://github.com/sdamirsa/PubLiMiner/actions/workflows/ci.yml/badge.svg)](https://github.com/sdamirsa/PubLiMiner/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.XXXXXXX.svg)](https://doi.org/10.5281/zenodo.XXXXXXX)

PubLiMiner is a modular Python pipeline for mining biomedical literature from PubMed. It is designed for **200K+ paper corpora** with monthly incremental updates, a single Parquet file as the source of truth, and pluggable steps for embedding, clustering, and LLM-based structured extraction.

## Features

- **PubMed retrieval** — date-batched fetcher with rate limiting, retry, and SQLite response caching
- **XML parsing** — extracts title, abstract, authors, journal, year, DOI, MeSH, keywords, grants, publication type, and more
- **Deduplication** — 4-layer: PMID exact → DOI exact → fuzzy title (year-grouped) → retracted-paper removal
- **Single source of truth** — every step reads/writes columns to one `papers.parquet` file
- **Streamlit UI** — visual config editor, live progress, status panel, sample export (JSON/XLSX)
- **Resumable** — atomic writes, idempotent imports, crash-safe
- **Legacy import** — bulk-import existing JSON batches without re-downloading
- **CLI + library** — use as a Typer CLI or as a Python package

## Pipeline overview

```
fetch → parse → deduplicate → embed → reduce (optional)
                                ├→ cluster → sample → extract → score → trend
                                ├→ rag
                                └→ export ← trend, patent
```

Currently implemented: **fetch**, **parse**, **deduplicate**. The remaining steps are scaffolded in the architecture and being added incrementally.

## Installation

Requires **Python 3.11+**.

```bash
pip install publiminer              # core CLI + Python API
pip install "publiminer[ui]"        # + Streamlit UI
pip install "publiminer[all]"       # everything (UI + viz + rag + dev)
```

After install, launch the UI with a single command:

```bash
publiminer ui
```

Or use the Python API directly in a notebook/script:

```python
from publiminer import FetchStep, ParseStep, DeduplicateStep, Spine, GlobalConfig

cfg = GlobalConfig()  # loads defaults; override via publiminer.yaml or kwargs
spine = Spine("output")
print(spine.count(), "papers currently in spine")
```

### Installing from source (for contributors)

```bash
git clone https://github.com/sdamirsa/PubLiMiner.git
cd PubLiMiner
uv sync --all-extras    # or: pip install -e ".[all]"
```

## Quick start

### 1. Configure

Copy the example env and edit:

```bash
cp .env.example .env
# Edit .env and set NCBI_API_KEY (optional, raises rate limit to 10 req/sec)
```

Then either edit `publiminer.yaml` directly, or use the UI (recommended for first-time users):

```bash
# Windows
run_ui.bat
# Or any platform
python -m streamlit run src/publiminer/ui/app.py
```

The UI lets you set query, dates, output dir, and step parameters, then save the YAML and run the pipeline with a live progress bar.

### 2. Run via CLI

```bash
# Fetch + parse + deduplicate using publiminer.yaml
publiminer run --config publiminer.yaml

# Run a subset of steps
publiminer run --steps parse,deduplicate --output output

# Inspect current state
publiminer status --output output
publiminer inspect parse --output output
```

### 3. Import legacy data

If you already have PubMed data in the format produced by the [AI-in-Med-Trend](https://github.com/sdamirsa/AI-in-Med-Trend) pipeline:

```bash
publiminer import-legacy /path/to/pubmed_batch_files --output output
```

The import is **idempotent** — running it twice on the same files adds zero new rows.

## Project layout

```
src/publiminer/
├── core/         # Spine (Parquet), cache (SQLite), config, models, base step
├── steps/        # Pipeline steps — each self-contained (step.py, schema.py, default.yaml)
│   ├── fetch/
│   ├── parse/
│   └── deduplicate/
├── utils/        # Logger, rate limiter, env loader, batching, progress, legacy import
├── ui/           # Streamlit UI
├── cli.py        # Typer CLI
└── pipeline.py   # Full run orchestrator
```

## Configuration

PubLiMiner uses a single `publiminer.yaml` file. Each step has its own section with sane defaults — you only need to override what you want to change.

```yaml
general:
  output_dir: output
  log_level: INFO
fetch:
  query: "diabetes AND machine learning"
  start_date: "2024/01/01"
  end_date: "2024/12/31"
  email: ""           # or set PUBMED_EMAIL env var
  api_key: ""         # or set NCBI_API_KEY env var
  max_results: 0      # 0 = no cap (use date partitioning for large queries)
parse:
  prepare_llm_input: true
  flag_exclusions: true
deduplicate:
  fuzzy_threshold: 90
  remove_retracted: true
```

**Secrets** (`NCBI_API_KEY`, `OPENROUTER_API_KEY`, `PATENT_API_KEY`) should always come from environment variables, never the YAML.

## Resumable nightly runs

PubLiMiner is designed to be re-run nightly without redoing any work:

- **Streaming fetch**: every batch is flushed to a `papers.parquet.staging` checkpoint, so a crash mid-run loses nothing. The next run merges the staging file before continuing. Memory stays bounded (~50 MB) regardless of corpus size.
- **Incremental parse**: only rows without a `title` column are parsed. After the first sweep, subsequent runs only touch newly-fetched papers.
- **Auto date resume**: set `fetch.start_date: "auto"` to resume from (latest `fetch_date` − 7 days). The 7-day overlap covers PubMed back-dating and is harmless because PMID-level dedup skips anything already on disk.
- **`run_nightly.bat`**: Windows wrapper that logs to `nightly.log`. Schedule via Task Scheduler:
  ```
  schtasks /create /tn "PubLiMiner Nightly" /tr "C:\path\to\PubLiMiner\run_nightly.bat" /sc daily /st 02:00
  ```

## Performance notes

- **400K papers**: ~1.0–1.2 GB parquet, ~30–60 min total runtime with an NCBI API key
- **PubMed WebEnv limit**: a single esearch+efetch session caps at 9,999 records — use date partitioning (`start_date` / `end_date`) for larger queries
- **Memory**: parse loads all rows in RAM at once; for 400K with raw XML, allow ~4 GB peak
- **Disk**: keep at least 2× your final parquet size free for atomic writes

## Development

See [CLAUDE.md](CLAUDE.md) for the developer guide (architecture, conventions, how to add a new step).

```bash
pip install -e ".[dev]"
pytest                          # tests (coming soon)
ruff check src/                 # lint
ruff format src/                # format
mypy src/publiminer/            # type-check
```

## License

MIT — see [LICENSE](LICENSE).

## Citation

If you use PubLiMiner in academic work, please cite the repository:

```bibtex
@software{publiminer,
  author = {Safavi-Naini, Seyed Amir Ahmad},
  title = {PubLiMiner: Publication Literature Miner},
  url = {https://github.com/sdamirsa/PubLiMiner},
  year = {2026}
}
```
