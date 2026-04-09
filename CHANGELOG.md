# Changelog

All notable changes to PubLiMiner will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
