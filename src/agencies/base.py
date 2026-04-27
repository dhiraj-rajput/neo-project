"""
Base agency HTTP client with rate-limiting, retries, and structured status tracking.
All agency modules inherit from this.

Supports two usage patterns:
  1. Standalone: client = SBDBClient()  — creates its own httpx session
  2. Shared:     client = SBDBClient(http_client=shared_client) — reuses a session
"""

import asyncio
import httpx
from datetime import datetime, timedelta, timezone
from src.logger import logger

NOT_FOUND_HOLD_DAYS = 30


class FetchStatus:
    """Structured result returned alongside every agency request."""

    __slots__ = ("source", "status_code", "failure_type", "not_found_until", "lookup_value")

    def __init__(self, source: str, status_code: int | None, failure_type: str | None = None,
                 not_found_until: datetime | None = None, lookup_value: str | None = None):
        self.source = source
        self.status_code = status_code
        self.failure_type = failure_type
        self.not_found_until = not_found_until
        self.lookup_value = lookup_value


class BaseClient:
    """
    Async HTTP client with rate-limiting, retries, and 404-caching.

    Constructor args:
        name:        Agency name for logging
        base_url:    API host (e.g. "https://ssd-api.jpl.nasa.gov")
        rate_limit:  Min seconds between requests
        http_client: Optional shared httpx.AsyncClient
        semaphore:   Optional asyncio.Semaphore for concurrency control
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        rate_limit: float = 1.0,
        http_client: httpx.AsyncClient | None = None,
        semaphore: asyncio.Semaphore | None = None,
    ):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.rate_limit = rate_limit
        self._client = http_client
        self._owns_client = http_client is None
        self._semaphore = semaphore

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
                headers={"User-Agent": "NEO-Orbital-Tracker/3.0 (Research)"},
            )
        return self._client

    async def _get(self, path: str, params: dict | None = None,
                   max_retries: int = 3) -> dict | None:
        """GET → JSON with retries. Returns parsed JSON or None."""
        data, _ = await self._fetch_json("GET", path, params=params, max_retries=max_retries)
        return data

    async def _get_with_status(self, path: str, params: dict | None = None,
                               max_retries: int = 3) -> tuple[dict | None, FetchStatus]:
        """GET → JSON with retries + structured status for 404 tracking."""
        return await self._fetch_json("GET", path, params=params, max_retries=max_retries)

    async def _post_with_status(self, path: str, json_body: dict | None = None,
                                max_retries: int = 3) -> tuple[dict | None, FetchStatus]:
        """POST → JSON with retries + structured status."""
        return await self._fetch_json("POST", path, json_body=json_body, max_retries=max_retries)

    async def _get_text(self, path: str, params: dict | None = None) -> str | None:
        """GET → raw text response."""
        client = await self._ensure_client()
        url = f"{self.base_url}{path}" if path.startswith("/") else path
        try:
            if self._semaphore:
                async with self._semaphore:
                    resp = await client.get(url, params=params, timeout=15.0)
                    await asyncio.sleep(self.rate_limit)
            else:
                resp = await client.get(url, params=params, timeout=15.0)
                await asyncio.sleep(self.rate_limit)

            if resp.status_code == 200:
                return resp.text
            return None
        except Exception as e:
            logger.warning(f"[{self.name}] Text fetch error: {e}")
            return None

    async def _fetch_json(self, method: str, path: str, *,
                          params: dict | None = None,
                          json_body: dict | None = None,
                          max_retries: int = 3) -> tuple[dict | None, FetchStatus]:
        """Core request loop with structured status tracking."""
        client = await self._ensure_client()
        url = f"{self.base_url}{path}" if path.startswith("/") else path

        for attempt in range(max_retries):
            try:
                kwargs: dict = {"timeout": 30.0}
                if params:
                    kwargs["params"] = params
                if json_body:
                    kwargs["json"] = json_body
                    kwargs["headers"] = {"Content-Type": "application/json"}

                if self._semaphore:
                    async with self._semaphore:
                        resp = await client.request(method, url, **kwargs)
                        await asyncio.sleep(self.rate_limit)
                else:
                    resp = await client.request(method, url, **kwargs)
                    await asyncio.sleep(self.rate_limit)

                status = resp.status_code

                if status == 200:
                    try:
                        data = resp.json()
                    except ValueError:
                        return None, FetchStatus(self.name, 200, "invalid_json")
                    # Check for Sentry-style "error" in 200 response
                    if isinstance(data, dict) and "error" in data:
                        err_msg = data["error"].lower()
                        if "removed" in err_msg:
                            return data, FetchStatus(self.name, 200, "removed")
                        if "not found" in err_msg:
                            hold = datetime.now(timezone.utc) + timedelta(days=NOT_FOUND_HOLD_DAYS)
                            return None, FetchStatus(self.name, 404, "not_found", hold)
                    return data, FetchStatus(self.name, 200)

                if status == 300:
                    try:
                        data = resp.json()
                    except ValueError:
                        data = None
                    return data, FetchStatus(self.name, 300, "multiple_choices")

                if status == 404:
                    hold = datetime.now(timezone.utc) + timedelta(days=NOT_FOUND_HOLD_DAYS)
                    return None, FetchStatus(self.name, 404, "not_found", hold)

                if status == 400:
                    try:
                        err_data = resp.json()
                    except ValueError:
                        err_data = {}
                    msg = str(err_data.get("message") or err_data.get("error") or "").lower()
                    if "not found" in msg or "no matching" in msg:
                        hold = datetime.now(timezone.utc) + timedelta(days=NOT_FOUND_HOLD_DAYS)
                        return None, FetchStatus(self.name, 404, "not_found", hold)
                    return None, FetchStatus(self.name, 400, "client_error")

                if 400 < status < 500:
                    return None, FetchStatus(self.name, status, "client_error")

                # 5xx — retry
                if attempt == max_retries - 1:
                    return None, FetchStatus(self.name, status, "server_error")

            except (httpx.ConnectError, httpx.ConnectTimeout,
                    httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
                if attempt == max_retries - 1:
                    logger.warning(f"[{self.name}] Network error after {max_retries} retries: {exc}")
                    return None, FetchStatus(self.name, None, type(exc).__name__)

            except Exception as exc:
                logger.error(f"[{self.name}] Unexpected error: {exc}")
                return None, FetchStatus(self.name, None, type(exc).__name__)

            wait = (attempt + 1) * 2
            logger.debug(f"[{self.name}] Retry {attempt + 1}/{max_retries} in {wait}s")
            await asyncio.sleep(wait)

        return None, FetchStatus(self.name, None, "exhausted_retries")

    async def close(self):
        if self._owns_client and self._client:
            await self._client.aclose()
            self._client = None


# Keep backward compatibility
AgencyClient = BaseClient
