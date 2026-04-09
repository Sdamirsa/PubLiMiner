# PubLiMiner — Developer Guide

## What is PubLiMiner?

A Python library for mining PubMed literature: fetch papers via PubMed API, parse XML, deduplicate, embed, cluster, extract structured data with LLMs, score, detect trends, and export results. Designed for 200K+ papers with monthly incremental updates.

Full architecture: `.claude/PubLiMiner_Architecture.md`

## Tech Stack

- **Python 3.11+**
- **Polars** — Parquet read/write (single source of truth)
- **Pydantic v2** — Config validation, data models
- **httpx** — HTTP client (sync for PubMed, async for LLM APIs)
- **lxml** / **xml.etree.ElementTree** — XML parsing
- **Typer + Rich** — CLI
- **thefuzz** — Fuzzy title matching for deduplication
- **scikit-learn / hdbscan** — Clustering
- **Jinja2** — Prompt templates for LLM extraction

## Project Layout

```
src/publiminer/
├── core/           # Backbone: spine.py (Parquet), cache.py (SQLite), config.py, models.py, base_step.py
├── steps/          # Pipeline steps: fetch/, parse/, deduplicate/, embed/, cluster/, etc.
│   └── <step>/     # Each step: __init__.py, step.py, schema.py, default.yaml
├── utils/          # Logger, rate limiter, env loader, batching
├── viz/            # Optional visualization
├── cli.py          # Typer CLI
├── pipeline.py     # Full run orchestrator
└── constants.py, exceptions.py
```

## Key Architectural Rules

1. **Single Parquet file** (`papers.parquet`) is the source of truth. Every step reads columns, adds columns, writes back.
2. **ChromaDB** is derived from Parquet by the RAG step. It is disposable and rebuilt on demand.
3. **SQLite cache** (`cache.db`) stores only raw external API responses (PubMed XML, OpenRouter JSON). Never duplicate processed data.
4. **Config merge order**: step `default.yaml` → user `publiminer.yaml` → runtime overrides.
5. **Environment variables** for secrets only: `NCBI_API_KEY`, `OPENROUTER_API_KEY`, `PATENT_API_KEY`.
6. **Each step is self-contained**: `step.py` (logic), `schema.py` (Pydantic config), `default.yaml` (defaults).
7. **Steps extend `StepBase`** ABC from `core/base_step.py`.

## How to Develop

```bash
# Install in editable mode
pip install -e ".[dev]"

# Run tests
pytest

# Lint + format
ruff check src/ tests/
ruff format src/ tests/

# Type check
mypy src/publiminer/

# Run CLI
publiminer run --config publiminer.yaml
publiminer inspect fetch
publiminer status
```

## Adding a New Step

1. Create `src/publiminer/steps/<name>/` with `__init__.py`, `step.py`, `schema.py`, `default.yaml`
2. In `schema.py`: define a Pydantic config model for step-specific settings
3. In `default.yaml`: set default values
4. In `step.py`: subclass `StepBase`, implement `run()`, `validate_input()`, `validate_output()`
5. Register in `steps/__init__.py`

## Code Style

- Type hints on all public functions
- Docstrings on classes and public methods (Google style)
- Use `from __future__ import annotations` in all modules
- Ruff for linting/formatting (configured in pyproject.toml)
- No IPython/Jupyter imports — this is a library
- Use `rich` for progress bars, not `tqdm`
- Use structured logging via `utils/logger.py`

## Environment Variables

```
NCBI_API_KEY=...          # PubMed API key (optional, increases rate limit)
OPENROUTER_API_KEY=...    # For embeddings and LLM extraction
PATENT_API_KEY=...        # Patent database API key (optional)
```

## Pipeline Steps (dependency order)

```
fetch → parse → deduplicate → embed → reduce (optional)
                                 ├→ cluster → sample → extract → score → trend
                                 ├→ rag
                                 └→ export ← trend, patent
```
