"""Token bucket rate limiter for external API calls."""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """Thread-safe token bucket rate limiter.

    Args:
        rate: Maximum requests per second.
        burst: Maximum burst size (defaults to rate).
    """

    def __init__(self, rate: float, burst: int | None = None) -> None:
        self.rate = rate
        self.burst = burst or max(1, int(rate))
        self._tokens = float(self.burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            # Sleep for one token's worth of time
            time.sleep(1.0 / self.rate)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time. Must be called under lock."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(float(self.burst), self._tokens + elapsed * self.rate)
        self._last_refill = now
