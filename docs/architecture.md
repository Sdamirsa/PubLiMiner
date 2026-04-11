# PubLiMiner Architecture

## System Overview

```mermaid
graph TB
    subgraph User Interface
        CLI["CLI<br/><code>publiminer run/status/inspect/ui</code>"]
        UI["Streamlit UI<br/><code>publiminer ui</code>"]
    end

    subgraph Configuration
        YAML["publiminer.yaml<br/>(user config)"]
        DEFAULTS["default.yaml<br/>(per-step defaults)"]
        ENV[".env<br/>(NCBI_API_KEY, etc.)"]
        CONFIG["Config Loader<br/><code>core/config.py</code><br/>merge: defaults → yaml → runtime"]
    end

    subgraph Pipeline Engine
        BASE["StepBase ABC<br/><code>core/base_step.py</code><br/>execute → validate → run → save meta"]
        FETCH["FetchStep"]
        PARSE["ParseStep"]
        DEDUP["DeduplicateStep"]
    end

    subgraph Data Layer
        SPINE["Spine<br/><code>core/spine.py</code>"]
        PARQUET["papers.parquet<br/>(source of truth)"]
        STAGING["papers.parquet.staging<br/>(crash-safe checkpoint)"]
        CACHE["SQLite Cache<br/><code>cache.db</code><br/>(raw API responses)"]
        META["step_log/<br/>(JSON run metadata)"]
    end

    subgraph External
        PUBMED["PubMed E-utilities API<br/>esearch / efetch"]
    end

    CLI --> CONFIG
    UI --> CLI
    YAML --> CONFIG
    DEFAULTS --> CONFIG
    ENV --> CONFIG
    CONFIG --> BASE
    BASE --> FETCH
    BASE --> PARSE
    BASE --> DEDUP
    FETCH --> SPINE
    PARSE --> SPINE
    DEDUP --> SPINE
    SPINE --> PARQUET
    SPINE --> STAGING
    FETCH --> CACHE
    FETCH --> PUBMED
    BASE --> META
```

## Pipeline Flow

```mermaid
graph LR
    F["fetch<br/>PubMed → raw XML"] --> P["parse<br/>XML → structured fields"]
    P --> D["deduplicate<br/>4-layer dedup"]
    D --> E["embed<br/>(future)"]
    E --> C["cluster<br/>(future)"]
    C --> X["extract<br/>(future)"]

    style F fill:#2d6a4f,color:#fff
    style P fill:#2d6a4f,color:#fff
    style D fill:#2d6a4f,color:#fff
    style E fill:#6c757d,color:#fff
    style C fill:#6c757d,color:#fff
    style X fill:#6c757d,color:#fff
```

## Fetch Step — Detailed Logic

```mermaid
flowchart TD
    START([Start Fetch]) --> RESUME{Staging file<br/>exists?}
    RESUME -->|Yes| MERGE_PRIOR["Merge prior staging<br/>into main parquet<br/>(resume from crash)"]
    RESUME -->|No| LOAD_PMIDS
    MERGE_PRIOR --> LOAD_PMIDS

    LOAD_PMIDS["Load existing PMIDs<br/>from main + staging<br/>(in-memory set)"]

    LOAD_PMIDS --> RESOLVE["Resolve start_date"]
    RESOLVE --> AUTO{start_date<br/>= 'auto'?}
    AUTO -->|Yes| CALC["max(fetch_date) − 7 days"]
    AUTO -->|No| USE_DATE["Use provided date"]
    CALC --> PLAN
    USE_DATE --> PLAN

    PLAN["Plan date-batched sweep<br/>1. Generate monthly ranges<br/>2. Count each month (cached)<br/>3. Combine months ≤ 9,900"]

    PLAN --> ITER["Iterate optimized queries"]

    ITER --> SEARCH["esearch(query)<br/>→ count"]
    SEARCH --> CHECK{count > 9,999?}

    CHECK -->|No| PAGINATE["Paginate via WebEnv<br/>efetch batches of 500"]

    CHECK -->|Yes| SPLIT{Can split<br/>date range?}
    SPLIT -->|Yes| BISECT["Binary bisect<br/>date range in half<br/>↩ recurse each half"]
    SPLIT -->|No, single day| PMID_FALLBACK["PMID-list fallback:<br/>1. esearch → collect ALL PMIDs<br/>   (10k per page, no ceiling)<br/>2. efetch by id=pmid1,pmid2,...<br/>   (200 per call, no WebEnv)"]

    BISECT --> SEARCH

    PAGINATE --> EXTRACT
    PMID_FALLBACK --> EXTRACT

    EXTRACT["_extract_articles()<br/>Regex split XML → per-article<br/>Skip if PMID in existing set"]

    EXTRACT --> BUFFER{Buffer ≥ 5,000<br/>articles?}
    BUFFER -->|Yes| FLUSH["Flush to staging parquet<br/>(crash-safe checkpoint)"]
    BUFFER -->|No| MORE{More batches?}
    FLUSH --> MORE
    MORE -->|Yes| ITER
    MORE -->|No| FINAL_FLUSH["Flush remaining buffer"]

    FINAL_FLUSH --> FINAL_MERGE["Stream-merge staging<br/>into main parquet<br/>(pyarrow row groups,<br/>~50 MB memory cap)"]

    FINAL_MERGE --> DONE([Done])

    style PMID_FALLBACK fill:#d4a843,color:#000
    style BISECT fill:#d4a843,color:#000
    style FLUSH fill:#2d6a4f,color:#fff
    style FINAL_MERGE fill:#2d6a4f,color:#fff
```

## Parse Step — Detailed Logic

```mermaid
flowchart TD
    START([Start Parse]) --> CHECK{title column<br/>exists in parquet?}
    CHECK -->|Yes| INCR["Read rows where<br/>title IS NULL<br/>(incremental)"]
    CHECK -->|No| ALL["Read all rows<br/>(first parse)"]

    INCR --> LOOP
    ALL --> LOOP

    LOOP["For each unparsed row"]
    LOOP --> PARSE_XML["parse_article_xml(raw_xml)"]

    PARSE_XML --> FIELDS["Extract structured fields:<br/>• title, abstract, authors<br/>• journal, year, DOI, language<br/>• MeSH terms, keywords, grants<br/>• publication types, status<br/>• article IDs, history"]

    FIELDS --> FLAT["Flatten to row:<br/>• lists/dicts → JSON strings<br/>• optionally: llm_input, exclusion flags"]

    FLAT --> MORE{More rows?}
    MORE -->|Yes| LOOP
    MORE -->|No| UPSERT["spine.add_columns(new_df)<br/>Upsert: update matching PMIDs,<br/>leave existing data untouched"]

    UPSERT --> DONE([Done])

    style UPSERT fill:#2d6a4f,color:#fff
```

## Deduplicate Step — Four Layers

```mermaid
flowchart TD
    START([Start Deduplicate]) --> L1

    L1["Layer 1: PMID Exact<br/>df.unique(subset='pmid', keep='first')<br/>▸ Vectorized Polars operation"]
    L1 --> L2

    L2["Layer 2: DOI Exact<br/>Group by DOI → keep first PMID per DOI<br/>▸ Remove all but one per duplicate DOI"]
    L2 --> L3A

    L3A["Layer 3a: Exact Title<br/>Normalize (lowercase, strip whitespace)<br/>df.unique(subset='_title_norm')"]
    L3A --> L3B

    L3B["Layer 3b: Fuzzy Title<br/>▸ Block by (year, first 3 words)<br/>▸ Length pre-filter: skip if len ratio < threshold<br/>▸ Pairwise fuzz.ratio within blocks<br/>▸ Mark ≥ 90% similar for removal<br/>▸ O(k²) within blocks vs O(n²) naive"]
    L3B --> L4

    L4["Layer 4: Retracted Papers<br/>Filter publication_status contains 'retract'"]
    L4 --> WRITE

    WRITE["spine.write(df)<br/>Atomic overwrite"]
    WRITE --> DONE([Done])

    style L3B fill:#d4a843,color:#000
```

## Data Model — Parquet Schema

```mermaid
erDiagram
    PAPERS {
        string pmid PK "Unique PubMed ID"
        string raw_xml "Full article XML"
        string fetch_date "ISO timestamp"
        string fetch_query "Query used"
        string fetch_batch "Batch identifier"
        string title "Article title"
        string abstract "Full abstract text"
        string authors "JSON: list of author objects"
        string journal "JSON: journal metadata"
        int year "Publication year"
        string doi "Digital Object Identifier"
        string language "Article language"
        string pub_type "JSON: publication types"
        string mesh_terms "JSON: MeSH headings"
        string keywords "JSON: keyword lists"
        string grants "JSON: funding grants"
        string article_ids "JSON: all article IDs"
        string publication_status "e.g. epublish, ppublish"
        string llm_input "Formatted text for LLM"
        bool exclude_flag "Exclusion marker"
        string exclude_reason "Why excluded"
    }
```

## Memory & Crash Safety Model

```mermaid
flowchart LR
    subgraph "Memory-Bounded Fetch"
        API["PubMed API<br/>~5 MB per batch"] --> BUF["In-memory buffer<br/>≤ 5,000 articles<br/>(~50 MB)"]
        BUF -->|flush| STG["staging.parquet<br/>(crash checkpoint)"]
    end

    subgraph "Stream Merge"
        STG --> RG["Read row groups<br/>one at a time<br/>(~50 MB each)"]
        MAIN_IN["main parquet<br/>(existing)"] --> RG
        RG --> MAIN_OUT["main parquet<br/>(merged, atomic write)"]
    end

    style STG fill:#d4a843,color:#000
    style MAIN_OUT fill:#2d6a4f,color:#fff
```

## Config Merge Chain

```mermaid
flowchart LR
    A["steps/fetch/default.yaml<br/>(library defaults)"] --> M["deep_merge()"]
    B["publiminer.yaml<br/>(user config)"] --> M
    C["CLI args / runtime<br/>(--output, --steps)"] --> M
    D[".env<br/>(NCBI_API_KEY)"] --> M
    M --> CFG["GlobalConfig +<br/>FetchConfig +<br/>ParseConfig +<br/>DeduplicateConfig"]

    style CFG fill:#2d6a4f,color:#fff
```

## Retry & Rate Limiting

```mermaid
flowchart TD
    REQ["HTTP Request"] --> RATE["Token bucket<br/>rate limiter<br/>(3 or 10 req/sec)"]
    RATE --> TRY["Send request"]
    TRY --> STATUS{Response?}

    STATUS -->|200 OK| DONE["Return data"]
    STATUS -->|429 / 5xx| RETRY["Exponential backoff<br/>2^attempt + jitter<br/>(max 60s)"]
    STATUS -->|Network error<br/>peer closed /<br/>timeout / DNS| RETRY
    STATUS -->|4xx client error<br/>(not 429)| FAIL["Raise APIError<br/>(no retry)"]

    RETRY --> ATTEMPT{Attempt<br/>< max_retries?}
    ATTEMPT -->|Yes| TRY
    ATTEMPT -->|No| FAIL

    style FAIL fill:#c0392b,color:#fff
    style DONE fill:#2d6a4f,color:#fff
```
