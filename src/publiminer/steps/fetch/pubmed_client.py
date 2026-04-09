"""PubMed E-utilities API client.

Migrated from AI-in-Med-Trend/Code/S1_Pubmed_Retriever.py.
Core retrieval logic preserved; replaced requests with httpx,
file caching with SQLite cache, tqdm with rich progress.
"""

from __future__ import annotations

import hashlib
import logging
import random
import re
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Iterator

import httpx

from publiminer.constants import (
    PUBMED_BASE_URL,
    PUBMED_DEFAULT_BATCH_SIZE,
    PUBMED_MAX_RESULTS_PER_REQUEST,
)
from publiminer.core.cache import ResponseCache
from publiminer.exceptions import APIError
from publiminer.utils.rate_limiter import RateLimiter

logger = logging.getLogger("publiminer.fetch")

# Valid return formats for PubMed database
_VALID_FORMATS: dict[tuple[str, str], bool] = {
    ("", "xml"): True,
    ("medline", "text"): True,
    ("uilist", "text"): True,
    ("abstract", "text"): True,
    ("gb", "text"): True,
    ("gb", "xml"): True,
    ("gbc", "xml"): True,
    ("ft", "text"): True,
}


class PubMedClient:
    """Client for the NCBI PubMed E-utilities API.

    Args:
        email: Email for NCBI identification (required).
        api_key: NCBI API key for higher rate limits.
        rate_limit: Requests per second.
        cache: Optional ResponseCache instance.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        email: str,
        api_key: str = "",
        rate_limit: float = 3.0,
        cache: ResponseCache | None = None,
        timeout: float = 120.0,
        max_retries: int = 5,
    ) -> None:
        if not email:
            raise ValueError("Email is required for PubMed API access")

        self.email = email
        self.api_key = api_key
        self.limiter = RateLimiter(rate_limit)
        self.cache = cache
        # Long read timeout: PubMed sometimes stalls mid-chunked-response.
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=30.0, read=timeout),
        )
        self.db = "pubmed"
        self.max_retries = max_retries

    def _get(self, url: str) -> str:
        """Rate-limited GET with exponential-backoff retry on transient errors.

        Retries on:
        - Network errors (peer closed connection, read timeout, ConnectError)
        - HTTP 429 (rate limit) and 5xx server errors
        Does NOT retry on 4xx client errors (bad query, missing params, etc).
        """
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            self.limiter.acquire()
            try:
                response = self.client.get(url)
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.text
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                # Non-retryable 4xx (except 429)
                if status < 500 and status != 429:
                    raise APIError("pubmed", str(e), status) from e
                last_exc = e
            except (
                httpx.RemoteProtocolError,   # peer closed connection
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpx.ConnectTimeout,
                httpx.ReadError,
                httpx.WriteError,
            ) as e:
                last_exc = e
            except httpx.RequestError as e:
                last_exc = e

            # Exponential backoff with jitter
            if attempt < self.max_retries - 1:
                sleep_s = min(60.0, (2 ** attempt) + random.uniform(0, 1))
                logger.warning(
                    f"PubMed request failed ({type(last_exc).__name__}: "
                    f"{str(last_exc)[:80]}); retry {attempt + 1}/"
                    f"{self.max_retries - 1} in {sleep_s:.1f}s"
                )
                time.sleep(sleep_s)

        raise APIError("pubmed", f"max retries exceeded: {last_exc}") from last_exc

    def _build_base_params(self) -> str:
        """Build common URL parameters."""
        params = f"db={self.db}&email={self.email}"
        if self.api_key:
            params += f"&api_key={self.api_key}"
        return params

    def validate_return_format(self, ret_mode: str, ret_type: str) -> tuple[str, str]:
        """Validate and normalize return format.

        Args:
            ret_mode: Return mode (xml, text).
            ret_type: Return type.

        Returns:
            Tuple of (ret_mode, ret_type).
        """
        ret_mode = ret_mode.lower()
        if (ret_type, ret_mode) in _VALID_FORMATS:
            return ret_mode, ret_type
        logger.warning(f"Invalid format ({ret_type}, {ret_mode}). Using default (xml, '')")
        return "xml", ""

    def search(self, query: str) -> tuple[str, str, int]:
        """Search PubMed with esearch.

        Args:
            query: Search query string.

        Returns:
            Tuple of (web_env, query_key, result_count).
        """
        encoded_query = urllib.parse.quote_plus(query)
        url = f"{PUBMED_BASE_URL}esearch.fcgi?{self._build_base_params()}&term={encoded_query}&usehistory=y"

        logger.debug(f"Searching PubMed: {query[:100]}...")

        content = self._get(url)

        # Parse XML response
        web_env_match = re.search(r"<WebEnv>(\S+)</WebEnv>", content)
        query_key_match = re.search(r"<QueryKey>(\d+)</QueryKey>", content)
        count_match = re.search(r"<Count>(\d+)</Count>", content)

        if not all([web_env_match, query_key_match, count_match]):
            raise APIError("pubmed", "Failed to parse esearch response")

        web_env = web_env_match.group(1)
        query_key = query_key_match.group(1)
        count = int(count_match.group(1))

        logger.debug(f"Search returned {count} results")
        return web_env, query_key, count

    def fetch_batch(
        self,
        web_env: str,
        query_key: str,
        retstart: int,
        retmax: int,
        download_mode: str = "full",
        ret_mode: str = "xml",
        ret_type: str = "",
    ) -> str:
        """Fetch a batch of results using efetch or esummary.

        Args:
            web_env: WebEnv from search.
            query_key: QueryKey from search.
            retstart: Starting index.
            retmax: Maximum records.
            download_mode: 'full' (efetch) or 'summary' (esummary).
            ret_mode: Return mode.
            ret_type: Return type.

        Returns:
            Response text (XML or other format).
        """
        ret_mode, ret_type = self.validate_return_format(ret_mode, ret_type)

        if download_mode == "summary":
            endpoint = "esummary.fcgi"
        else:
            endpoint = "efetch.fcgi"

        url = (
            f"{PUBMED_BASE_URL}{endpoint}?"
            f"{self._build_base_params()}"
            f"&query_key={query_key}&WebEnv={web_env}"
            f"&retstart={retstart}&retmax={retmax}"
        )
        if ret_mode:
            url += f"&retmode={ret_mode}"
        if ret_type:
            url += f"&rettype={ret_type}"

        logger.debug(f"Fetching batch: {retstart} to {retstart + retmax - 1}")

        # Check cache
        cache_key = f"efetch:{web_env}:{query_key}:{retstart}:{retmax}"
        if self.cache:
            cached = self.cache.get("pubmed", cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for batch {retstart}-{retstart + retmax - 1}")
                return cached

        result = self._get(url)

        # Store in cache
        if self.cache:
            self.cache.put("pubmed", cache_key, result)

        return result

    def generate_monthly_date_ranges(
        self, start_date: str, end_date: str
    ) -> list[dict[str, Any]]:
        """Generate monthly date ranges between start and end dates.

        Args:
            start_date: Start date as YYYY/MM/DD.
            end_date: End date as YYYY/MM/DD.

        Returns:
            List of dicts with start_date, end_date, query_fragment, month_year.
        """
        start = datetime.strptime(start_date, "%Y/%m/%d")
        end = datetime.strptime(end_date, "%Y/%m/%d")
        ranges: list[dict[str, Any]] = []
        current = start

        while current <= end:
            year, month = current.year, current.month
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)

            last_day = next_month - timedelta(days=1)
            first_str = f"{year}/{month:02d}/01"
            last_str = f"{year}/{month:02d}/{last_day.day:02d}"

            ranges.append({
                "start_date": first_str,
                "end_date": last_str,
                "query_fragment": f'"{first_str}"[Date - Publication] : "{last_str}"[Date - Publication]',
                "month_year": f"{year}-{month:02d}",
            })

            current = next_month

        return ranges

    def get_monthly_counts(
        self, query: str, date_ranges: list[dict[str, Any]]
    ) -> dict[str, int]:
        """Get result counts for each monthly date range.

        Args:
            query: Base query string.
            date_ranges: List of date range dicts.

        Returns:
            Dict mapping query_fragment -> count.
        """
        counts: dict[str, int] = {}

        for date_range in date_ranges:
            fragment = date_range["query_fragment"]

            # Check cache
            cache_key = f"count:{query}:{fragment}"
            if self.cache:
                cached = self.cache.get("pubmed_counts", cache_key)
                if cached is not None:
                    counts[fragment] = int(cached)
                    logger.debug(f"Cached count for {date_range['month_year']}: {counts[fragment]}")
                    continue

            try:
                full_query = f"({query}) AND ({fragment})"
                _, _, count = self.search(full_query)
                counts[fragment] = count
                logger.info(f"Month {date_range['month_year']}: {count} results")

                if self.cache:
                    self.cache.put("pubmed_counts", cache_key, str(count))

            except Exception as e:
                logger.error(f"Failed to get count for {date_range['month_year']}: {e}")
                counts[fragment] = 0

        return counts

    def create_optimized_queries(
        self,
        base_query: str,
        date_ranges: list[dict[str, Any]],
        monthly_counts: dict[str, int],
    ) -> list[dict[str, Any]]:
        """Combine date ranges into optimized queries under the API limit.

        Args:
            base_query: Base query without date filters.
            date_ranges: Monthly date ranges.
            monthly_counts: Count per date range.

        Returns:
            List of optimized query dicts.
        """
        queries: list[dict[str, Any]] = []
        current_count = 0
        batch_start_idx = 0

        for i, date_range in enumerate(date_ranges):
            count = monthly_counts.get(date_range["query_fragment"], 0)

            if current_count + count > PUBMED_MAX_RESULTS_PER_REQUEST and i > batch_start_idx:
                # Finalize current batch
                start = date_ranges[batch_start_idx]["start_date"]
                end = date_ranges[i - 1]["end_date"]
                date_query = f'"{start}"[Date - Publication] : "{end}"[Date - Publication]'
                queries.append({
                    "query": f"({base_query}) AND ({date_query})",
                    "count": current_count,
                    "start_date": start,
                    "end_date": end,
                    "date_range": date_query,
                    "batch_id": len(queries),
                })
                current_count = 0
                batch_start_idx = i

            current_count += count

        # Final batch
        if batch_start_idx < len(date_ranges):
            start = date_ranges[batch_start_idx]["start_date"]
            end = date_ranges[-1]["end_date"]
            date_query = f'"{start}"[Date - Publication] : "{end}"[Date - Publication]'
            queries.append({
                "query": f"({base_query}) AND ({date_query})",
                "count": current_count,
                "start_date": start,
                "end_date": end,
                "date_range": date_query,
                "batch_id": len(queries),
            })

        return queries

    def plan_date_batched(
        self, base_query: str, start_date: str, end_date: str
    ) -> tuple[list[dict[str, Any]], int]:
        """Plan a date-batched sweep: count every month, build optimized queries.

        Returns:
            (optimized_queries, expected_total_articles)
        """
        date_ranges = self.generate_monthly_date_ranges(start_date, end_date)
        logger.info(
            f"Planning date-batched sweep: counting {len(date_ranges)} months "
            f"({start_date} → {end_date})"
        )
        monthly_counts = self.get_monthly_counts(base_query, date_ranges)
        optimized = self.create_optimized_queries(base_query, date_ranges, monthly_counts)
        total = sum(q["count"] for q in optimized)
        logger.info(
            f"Plan ready: {len(optimized)} optimized queries, "
            f"~{total:,} articles to fetch"
        )
        return optimized, total

    def iter_planned(
        self,
        optimized: list[dict[str, Any]],
        batch_size: int = PUBMED_DEFAULT_BATCH_SIZE,
        download_mode: str = "full",
        ret_mode: str = "xml",
        ret_type: str = "",
    ) -> Iterator[dict[str, Any]]:
        """Stream batches from a pre-computed plan (from plan_date_batched)."""
        for query_info in optimized:
            query = query_info["query"]
            batch_id = query_info["batch_id"]
            web_env, query_key, actual_count = self.search(query)
            if actual_count == 0:
                continue
            num_sub_batches = (actual_count + batch_size - 1) // batch_size
            for i in range(num_sub_batches):
                retstart = i * batch_size
                retmax = min(batch_size, actual_count - retstart)
                data = self.fetch_batch(
                    web_env=web_env, query_key=query_key,
                    retstart=retstart, retmax=retmax,
                    download_mode=download_mode, ret_mode=ret_mode, ret_type=ret_type,
                )
                yield {
                    "query": query,
                    "batch_id": f"{batch_id}_{i}",
                    "retstart": retstart,
                    "retmax": retmax,
                    "total_count": actual_count,
                    "timestamp": datetime.now().isoformat(),
                    "data": data,
                }

    def iter_date_batched(
        self,
        base_query: str,
        start_date: str,
        end_date: str,
        batch_size: int = PUBMED_DEFAULT_BATCH_SIZE,
        download_mode: str = "full",
        ret_mode: str = "xml",
        ret_type: str = "",
    ) -> Iterator[dict[str, Any]]:
        """Streaming version of retrieve_date_batched: yields one batch dict at a time.

        Memory: only one batch (~5 MB raw XML) lives in RAM at any moment, vs.
        the legacy retrieve_date_batched which buffered every batch in a list
        (tens of GB for full sweeps).
        """
        date_ranges = self.generate_monthly_date_ranges(start_date, end_date)
        monthly_counts = self.get_monthly_counts(base_query, date_ranges)
        optimized = self.create_optimized_queries(base_query, date_ranges, monthly_counts)
        total_articles = sum(q["count"] for q in optimized)
        logger.info(
            f"Streaming fetch: {len(optimized)} optimized queries, ~{total_articles:,} articles total"
        )

        for query_info in optimized:
            query = query_info["query"]
            batch_id = query_info["batch_id"]

            web_env, query_key, actual_count = self.search(query)
            if actual_count == 0:
                continue
            num_sub_batches = (actual_count + batch_size - 1) // batch_size

            for i in range(num_sub_batches):
                retstart = i * batch_size
                retmax = min(batch_size, actual_count - retstart)
                data = self.fetch_batch(
                    web_env=web_env,
                    query_key=query_key,
                    retstart=retstart,
                    retmax=retmax,
                    download_mode=download_mode,
                    ret_mode=ret_mode,
                    ret_type=ret_type,
                )
                yield {
                    "query": query,
                    "batch_id": f"{batch_id}_{i}",
                    "retstart": retstart,
                    "retmax": retmax,
                    "total_count": actual_count,
                    "expected_total": total_articles,
                    "timestamp": datetime.now().isoformat(),
                    "data": data,
                }

    def retrieve_date_batched(
        self,
        base_query: str,
        start_date: str,
        end_date: str,
        batch_size: int = PUBMED_DEFAULT_BATCH_SIZE,
        download_mode: str = "full",
        ret_mode: str = "xml",
        ret_type: str = "",
    ) -> list[dict[str, Any]]:
        """Retrieve all results using date-based batching.

        Handles large result sets by splitting into monthly date ranges,
        then combining months to stay under the 9,900 result limit.

        Args:
            base_query: Query without date filters.
            start_date: Start date (YYYY/MM/DD).
            end_date: End date (YYYY/MM/DD).
            batch_size: Records per fetch call.
            download_mode: 'full' or 'summary'.
            ret_mode: Return mode.
            ret_type: Return type.

        Returns:
            List of dicts with query info and fetched XML data.
        """
        date_ranges = self.generate_monthly_date_ranges(start_date, end_date)
        monthly_counts = self.get_monthly_counts(base_query, date_ranges)
        optimized = self.create_optimized_queries(base_query, date_ranges, monthly_counts)

        all_results: list[dict[str, Any]] = []

        for query_info in optimized:
            query = query_info["query"]
            count = query_info["count"]
            batch_id = query_info["batch_id"]

            logger.info(f"Processing optimized batch {batch_id}: ~{count} records")

            # Search to get WebEnv
            web_env, query_key, actual_count = self.search(query)
            num_sub_batches = (actual_count + batch_size - 1) // batch_size

            for i in range(num_sub_batches):
                retstart = i * batch_size
                retmax = min(batch_size, actual_count - retstart)

                data = self.fetch_batch(
                    web_env=web_env,
                    query_key=query_key,
                    retstart=retstart,
                    retmax=retmax,
                    download_mode=download_mode,
                    ret_mode=ret_mode,
                    ret_type=ret_type,
                )

                all_results.append({
                    "query": query,
                    "batch_id": f"{batch_id}_{i}",
                    "retstart": retstart,
                    "retmax": retmax,
                    "total_count": actual_count,
                    "download_mode": download_mode,
                    "ret_mode": ret_mode,
                    "ret_type": ret_type,
                    "timestamp": datetime.now().isoformat(),
                    "data": data,
                })

        return all_results

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()
