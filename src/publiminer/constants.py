"""Global constants for PubLiMiner."""

from __future__ import annotations

# PubMed E-utilities API
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
PUBMED_ESEARCH = f"{PUBMED_BASE_URL}esearch.fcgi"
PUBMED_EFETCH = f"{PUBMED_BASE_URL}efetch.fcgi"
PUBMED_ESUMMARY = f"{PUBMED_BASE_URL}esummary.fcgi"

# PubMed API limits
PUBMED_MAX_RESULTS_PER_REQUEST = 9900  # Slightly below 10,000 to be safe
PUBMED_DEFAULT_BATCH_SIZE = 500
PUBMED_DEFAULT_RATE_LIMIT = 3  # requests per second (with API key; 1 without)

# Parquet file name
PARQUET_FILENAME = "papers.parquet"

# Cache
CACHE_FILENAME = "cache.db"
CACHE_DEFAULT_TTL_DAYS = 90

# Step log directory
STEP_LOG_DIR = "step_log"

# Export directory
EXPORT_DIR = "export"

# Default output directory
DEFAULT_OUTPUT_DIR = "output"

# Logging
LOG_DIR = "logs"

# Update history file
UPDATE_HISTORY_FILENAME = "_update_history.json"

# Pipeline metadata file
PIPELINE_META_FILENAME = "_meta.json"
