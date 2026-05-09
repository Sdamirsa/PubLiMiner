# PubLiMiner — Experiment Runbook

Quick reference for running searches and extractions. Full docs: `CLAUDE.md`, `docs/architecture.md`.

---

## 1 — One-time setup

```bash
# Install (editable, from repo root)
uv sync --all-extras

# Configure credentials
cp .env.example .env      # or create .env manually
```

**.env** (required fields):
```
PUBMED_EMAIL=you@example.com
NCBI_API_KEY=abc123          # optional — raises rate limit 3→10 req/s
OPENROUTER_API_KEY=sk-or-... # required for extract step
```

---

## 2 — Run a project

```bash
# Full pipeline
uv run publiminer run --config projects/cardiac_mri.yaml
uv run publiminer run --config projects/cardiac_ct.yaml

# Specific steps only
uv run publiminer run --config projects/cardiac_mri.yaml --steps fetch,parse
uv run publiminer run --config projects/cardiac_mri.yaml --steps deduplicate,filter
uv run publiminer run --config projects/cardiac_mri.yaml --steps extract

# Override output dir (useful for test runs)
uv run publiminer run --config projects/cardiac_mri.yaml --output output/test_run
```

---

## 3 — Inspect results

```bash
# Corpus summary (rows, size, columns)
uv run publiminer status --output output/cardiac_mri

# Step metadata (rows before/after, duration, errors)
uv run publiminer inspect fetch   --output output/cardiac_mri
uv run publiminer inspect parse   --output output/cardiac_mri
uv run publiminer inspect deduplicate --output output/cardiac_mri
uv run publiminer inspect filter  --output output/cardiac_mri
uv run publiminer inspect extract --output output/cardiac_mri
```

**Read parquet directly (Python):**
```python
import polars as pl
df = pl.read_parquet("output/cardiac_mri/papers.parquet")
print(df.shape, df.columns)
df.filter(pl.col("is_european") == True).select(["pmid","title","authors"]).head(5)
```

**Read extractions (Python):**
```python
import sqlite3, json, pandas as pd
con = sqlite3.connect("output/cardiac_mri/extractions.db")
df = pd.read_sql("SELECT pmid, extracted_json, model_used, cost_usd FROM extractions", con)
df["data"] = df["extracted_json"].apply(json.loads)
```

---

## 4 — Config quick reference

### Pilot vs full run
```yaml
fetch:
  start_date: "2023/01/01"   # PILOT (3 years)
  start_date: "2000/01/01"   # FULL  (change this for production)
  end_date: "2026/04/30"
  max_results: 0              # 0 = no cap
```

### Filter step (geographic tagging)
```yaml
filter:
  output_column: "is_european"
  min_author_matches: 1       # ≥1 author affiliation must match
  drop_non_matching: false    # false = tag only; true = drop rows
  case_sensitive: false
  keywords:
    - "germany"
    - " berlin"               # leading space avoids author-name false positives
    - "europe"
```

### Extract step (LLM)
```yaml
extract:
  schema_name: "my_schema"        # used as SQLite table key — must be unique per project
  model: "openai/gpt-oss-120b"
  fallback_models:
    - "anthropic/claude-haiku-4-5"
    - "openai/gpt-4o-mini"
  filter_column: "is_european"    # only process rows where this column is True/non-null
  max_cost_usd: 50.0              # hard cap — step halts cleanly if exceeded
  concurrency: 20                  # parallel requests; lower if hitting rate limits
  temperature: 0.0
  include_title: false
  include_abstract: false
  include_author_block: true      # send author+affiliation text to LLM
  user_instruction: >
    Your domain-specific instruction here.
  fields:
    - name: first_author_specialty
      type: enum
      values: ["Radiology", "Cardiology", "Unclear"]
      description: >
        Classify based on affiliation text only. ...
      required: true
```

**Enum types:** `string`, `integer`, `float`, `boolean`, `enum`, `list[string]`, `list[integer]`, `object`, `list[object]`

**Nested fields** — use `parent`:
```yaml
fields:
  - name: funding
    type: object
    description: "Funding info"
    required: false
  - name: agency
    type: string
    parent: funding
    description: "Funding agency name"
```

---

## 5 — Current projects

| Config | Topic | Date range | Output dir |
|--------|-------|------------|------------|
| `projects/cardiac_mri.yaml` | Cardiac MRI / CMR — European author specialty | 2023–2026 pilot | `output/cardiac_mri` |
| `projects/cardiac_ct.yaml`  | Cardiac CT / CCTA — European author specialty | 2023–2026 pilot | `output/cardiac_ct`  |

**To run full 2000–2026 corpus:** change `start_date: "2023/01/01"` → `"2000/01/01"` in both configs.

**LLM schema names:** `cardiac_mri_author_specialty`, `cardiac_ct_author_specialty`

**Extracted fields:** `first_author_specialty`, `last_author_specialty` — each one of `Radiology | Cardiology | Unclear`

---

## 6 — Resume & idempotency

- **Fetch** resumes from staging file if crashed mid-run
- **Parse** skips already-parsed rows (incremental)
- **Extract** skips PMIDs already in `extractions.db` for the same `(schema_name, run_id)` pair
- Re-running any step is always safe — no double-processing

To force a clean extraction re-run:
```bash
# Option A: change run_id in YAML
extract:
  run_id: "2026-05-06-v2"

# Option B: delete the DB and re-run
del output\cardiac_mri\extractions.db
```

---

## 7 — Troubleshooting

| Symptom | Fix |
|---------|-----|
| `OPENROUTER_API_KEY not set` | Add key to `.env` |
| `publiminer: command not found` | Use `uv run publiminer ...` or activate `.venv` |
| Rate limit errors from PubMed | Set `NCBI_API_KEY` in `.env` |
| Extract cost cap hit | Raise `max_cost_usd` or reduce dataset via tighter `filter` |
| `asyncio.run()` error in tests | Call `await step._async_run()` directly in async tests |
| Low filter hit rate | Add more city/country keywords or check `also_check_columns` |
| `filter_column` not found | Run `filter` step before `extract` |

---

## 8 — Useful one-liners

```bash
# How many European papers after filter?
python -c "import polars as pl; df=pl.read_parquet('output/cardiac_mri/papers.parquet'); print(df.filter(pl.col('is_european')==True).shape)"

# Extraction cost so far
python -c "import sqlite3; c=sqlite3.connect('output/cardiac_mri/extractions.db'); print(c.execute('SELECT SUM(cost_usd), COUNT(*) FROM extractions WHERE error_label IS NULL').fetchone())"

# Specialty breakdown
python -c "
import sqlite3, json, collections
con = sqlite3.connect('output/cardiac_mri/extractions.db')
rows = con.execute(\"SELECT extracted_json FROM extractions WHERE error_label IS NULL\").fetchall()
counter = collections.Counter(json.loads(r[0])['first_author_specialty'] for r in rows)
print(counter)
"

# Export extractions to JSONL
python -c "
from publiminer.core.extraction_db import ExtractionDB
from pathlib import Path
db = ExtractionDB(Path('output/cardiac_mri/extractions.db'))
n = db.export_jsonl('cardiac_mri_author_specialty', 'YOUR_RUN_ID', Path('cardiac_mri_results.jsonl'))
print(n, 'rows exported')
"
```
