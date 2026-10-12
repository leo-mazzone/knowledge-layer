"""Pacing and bounded retries shared by all requests in a GOV.UK ingestion."""

import logging
import math
import random
import time
from contextlib import contextmanager
from datetime import UTC
from email.utils import parsedate_to_datetime

from knowledge.errors import SourceRequestsStopped

logger = logging.getLogger(__name__)

DEFAULT_HTTP = {
    "min_interval": 1.0,
    "max_retries": 4,
    "initial_backoff": 2.0,
    "max_backoff": 60.0,
    "max_retry_after": 300.0,
}


class GovUkRequests:
    def __init__(self, client, *, clock=None, sleep=None, wallclock=None, jitter=None):
        self.client = client
        self.clock = clock or time.monotonic
        self.sleep = sleep or time.sleep
        self.wallclock = wallclock or time.time
        self.jitter = jitter or random.uniform
        self.next_request_at = 0.0
        self.configure({})

    def configure(self, options):
        if not isinstance(options, dict) or set(options) - set(DEFAULT_HTTP):
            raise ValueError("http must contain only supported request policy options")
        policy = {**DEFAULT_HTTP, **options}
        for key in ("min_interval", "initial_backoff", "max_backoff", "max_retry_after"):
            value = policy[key]
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"http.{key} must be positive and finite")
        retries = policy["max_retries"]
        if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= 10:
            raise ValueError("http.max_retries must be an integer from 0 to 10")
        if policy["max_backoff"] < policy["initial_backoff"]:
            raise ValueError("http.max_backoff must be at least initial_backoff")
        self.policy = policy
        self.report = {
            "policy": dict(policy),
            "requests": 0,
            "retries": 0,
            "rate_limited": 0,
            "service_unavailable": 0,
            "wait_seconds": 0.0,
            "stopped": False,
            "stop_reason": None,
        }

    def _wait(self, seconds, *, retry=False):
        self.report["wait_seconds"] += seconds
        long_wait = retry and seconds > 60
        while seconds > 0:
            if long_wait:
                logger.info("GOV.UK retry cooldown remaining=%.1fs", seconds)
            pause = min(seconds, 60.0)
            self.sleep(pause)
            seconds -= pause

    def _pace(self):
        if self.report["stopped"]:
            raise SourceRequestsStopped(self.report["stop_reason"])
        self._wait(max(0.0, self.next_request_at - self.clock()))
        self.next_request_at = self.clock() + self.policy["min_interval"]
        self.report["requests"] += 1

    def _retry_after(self, value):
        if not value:
            return None
        try:
            if value.strip().isdigit():
                return float(value.strip())
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            return max(0.0, date.timestamp() - self.wallclock())
        except (ValueError, TypeError, OverflowError):
            return None

    def _stop(self, reason):
        self.report.update(stopped=True, stop_reason=reason)
        logger.error("%s", reason)
        raise SourceRequestsStopped(reason)

    def _retry_delay(self, response, attempt):
        status = response.status_code
        if status not in (429, 503):
            return None
        self.report["rate_limited" if status == 429 else "service_unavailable"] += 1
        delay = self._retry_after(response.headers.get("Retry-After"))
        reason = "Retry-After" if delay is not None else "exponential backoff with jitter"
        if delay is not None:
            self.next_request_at = max(self.next_request_at, self.clock() + delay)
        if attempt >= self.policy["max_retries"]:
            self._stop(
                f"GOV.UK HTTP {status} persisted after {attempt + 1} attempts; "
                "all source requests stopped"
                + (f"; Retry-After requires waiting {delay:g} seconds" if delay is not None else "")
            )
        if delay is None:
            base = min(self.policy["max_backoff"], self.policy["initial_backoff"] * 2**attempt)
            delay = min(self.policy["max_backoff"], base + self.jitter(0.0, base * 0.25))
        if delay > self.policy["max_retry_after"]:
            self._stop(
                f"GOV.UK HTTP {status} requires a wait of {delay:g} seconds, exceeding "
                f"the configured {self.policy['max_retry_after']:g}-second wait limit; "
                "all source requests stopped; wait before restarting"
            )
        self.report["retries"] += 1
        logger.warning(
            "GOV.UK HTTP %d retry=%d/%d wait=%.1fs reason=%s",
            status,
            attempt + 1,
            self.policy["max_retries"],
            delay,
            reason,
        )
        return delay

    def get(self, url, **kwargs):
        for attempt in range(self.policy["max_retries"] + 1):
            self._pace()
            response = self.client.get(url, follow_redirects=False, **kwargs)
            try:
                delay = self._retry_delay(response, attempt)
            except SourceRequestsStopped:
                response.close()
                raise
            if delay is None:
                return response
            response.close()
            self._wait(delay, retry=True)
        raise AssertionError("Retry loop exhausted without stopping")

    @contextmanager
    def stream(self, url):
        for attempt in range(self.policy["max_retries"] + 1):
            self._pace()
            with self.client.stream("GET", url, follow_redirects=False) as response:
                delay = self._retry_delay(response, attempt)
                if delay is None:
                    yield response
                    return
            # Close a rejected stream before waiting or opening another connection.
            self._wait(delay, retry=True)
        raise AssertionError("Retry loop exhausted without stopping")
