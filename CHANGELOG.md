# Changelog

All notable changes to PubLiMiner will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
