# Changelog

All notable changes to PubLiMiner will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Extract step** (`steps/extract/`) — general-purpose async LLM extraction with user-defined YAML schemas; writes results to a per-project `extractions.db` SQLite (not `papers.parquet`)
- **`core/openrouter.py`** — async httpx OpenRouter client with exponential-backoff retry, `X-Generation-Id` capture, cost tracking via `/api/v1/generation`, and `InsufficientCreditsError` / `NoProviderError` exceptions
- **`core/extraction_db.py`** — SQLite handler for extraction results with full audit trail (raw_response, fix_applied, fix_history, cost, tokens, latency per paper)
- **Schema builder** (`steps/extract/schema_builder.py`) — converts a flat YAML field list (with `parent` references) to a nested JSON Schema envelope and a dynamic Pydantic v2 model; supports string/integer/float/boolean/enum/list[string]/list[integer]/object/list[object] types
- **Repair pipeline** (`steps/extract/repair.py`) — two-stage PatternFixer (7 regex operations) → LLMFixer; all repair history saved regardless of outcome
- **Author block builder** (`steps/extract/author_mapper.py`) — formats first/last/corresponding author lines from parsed `authors` JSON column for injection into extraction prompts
- **Kahneman-style system prompt** (`steps/extract/prompt.py`) — 5 explicit epistemic rules for field-independent extraction
- **`general.project_name`** field in `GlobalConfig` (backward-compatible default `""`)
- **Extractions tab** in UI — schema/run selector, success/failed/repaired/cost metrics, results preview table, JSONL export download
- `pytest-asyncio` added to dev dependencies; `asyncio_mode = "auto"` in pytest config

### Changed
- `IMPLEMENTED_STEPS` in UI now includes `"extract"`
- CLI `_create_step()` wired to `ExtractStep`
- **`Irrelevant` → `NotRelevant`** in relevance enum for all project YAML schemas; visualizer normalizer is backward-compatible (old snapshots with "Irrelevant" still render correctly)
- Hardened all extract field descriptions in `projects/cardiac_mri.yaml` and `projects/cardiac_ct.yaml` with "Return EXACTLY one of: …" language and a "CRITICAL" prefix in `user_instruction` to reduce LLM enum non-compliance
- `parse/step.py` docstring updated to document `is_corresponding` and `equal_contribution` flags in the `authors` JSON column

### Added (visualize_run.py)
- **Normalization pipeline**: `normalize_relevance_dist()` and `normalize_study_type_dist()` collapse LLM free-text responses to canonical enum values before charting; eliminates bloated multi-slice donuts from non-compliant model output
- **`normalize_specialty()`** extended with compound department aliases: "radiology and radiation oncology", "radiology and nuclear medicine", "interventional radiology", "nuclear medicine and radiology"
- **Chart 6 — Journal Scope Distribution**: total vs European bar chart + donut for cardiology- vs radiology-scoped journals; funding landscape lollipop chart (chart 5)
- `generate_pipeline_pdf.py` now saves a PNG alongside the PDF for easy sharing; fixed callout box layout (was overflowing the right edge and overlapping the OUTPUT strip)

---

## [0.2.0] - 2026-04-17

### Added
- **`publiminer setup`** — interactive first-run wizard that captures the
  PubMed email + NCBI API key, scaffolds a starter `publiminer.yaml`, and
  appends `.env` to `.gitignore`. Works identically on macOS, Linux, and
  Windows via `typer.prompt(hide_input=True)`.
- **Auto-trigger setup** — `publiminer run` / `publiminer ui` detect a
  missing or incomplete `.env` and launch the wizard automatically. Opt
  out with `--no-setup` or `PUBLIMINER_NO_WIZARD=1` for CI.
- **UI first-run walkthrough** — 5-step guided wizard (Welcome → Email →
  NCBI key → Scaffold → Done) inside Streamlit for users who launch the
  UI without a configured environment. Password-masked key input, link
  buttons to the NCBI registration page, progress dots across the top.
- **Bundled starter YAML template** — shipped inside the wheel via
  `importlib.resources`, loaded by `scaffold_yaml()` with an inline
  fallback for source checkouts.

### Changed
- **README "Getting started"** rewritten around a single
  `uv tool install "publiminer[ui]" && publiminer ui` one-liner that
  works identically on macOS, Linux, and Windows. Added collapsible
  sections for uv install, NCBI key registration, UI walkthrough,
  developer commands, and alternate install paths (pip / pipx / uvx).

### Fixed
- Absolute GitHub URLs for all cross-file links in the README so PyPI's
  markdown renderer resolves them correctly (affects LICENSE badge,
  `docs/architecture.md`, and `CLAUDE.md`).

## [0.1.0] - 2026-04-09

### Added
- First public release on PyPI.
- Streaming PubMed fetch with crash-safe staging checkpoint, automatic resume,
  and bounded memory footprint (handles 300k+ papers on a laptop).
- Incremental XML parse (only unparsed rows are re-parsed; safe upserts).
- Fuzzy title + DOI deduplication (`thefuzz`), with retracted-paper removal.
- Typer CLI with commands: `run`, `status`, `inspect`, `import-legacy`, `ui`.
- Bundled Streamlit UI, launched via `publiminer ui`.
- Public Python API: `FetchStep`, `ParseStep`, `DeduplicateStep`, `Spine`, `GlobalConfig`.
- Resumable nightly runs via `start_date: "auto"` and `run_nightly.bat`.
- GitHub Actions CI (`ruff`, `mypy`, `pytest` on py3.11/3.12).
- Tag-triggered PyPI publishing via Trusted Publishing (OIDC).
- Zenodo DOI integration via `CITATION.cff` + `.zenodo.json`.
