"""
Minimal client for Wildberries API.

Docs: https://dev.wildberries.ru/
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests

from src.core.config import settings


class WildberriesAPIError(Exception):
    """Raised when Wildberries API request fails."""


class WildberriesClient:
    BASE_URL = settings.WILDBERRIES_BASE_URL

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        self.api_key = api_key or settings.WILDBERRIES_API_KEY
        self.timeout = timeout
        self._auth_raw = self.api_key or ""
        if self._auth_raw.lower().startswith("bearer "):
            self._auth_bearer = self._auth_raw
        else:
            self._auth_bearer = f"Bearer {self._auth_raw}"
        self.headers = {
            "Authorization": self._auth_bearer,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        retries: int = 4,
    ) -> Dict[str, Any]:
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        else:
            url = f"{self.BASE_URL}{path}"
        last_error: Exception | None = None

        for attempt in range(retries):
            try:
                response = requests.request(
                    method=method.upper(),
                    url=url,
                    headers=self.headers,
                    params=params or {},
                    json=payload,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 < retries:
                    time.sleep(2**attempt)
                    continue
                raise WildberriesAPIError(f"Network error for {url}: {exc}") from exc

            if response.status_code == 401 and self._auth_raw and self.headers.get("Authorization") != self._auth_raw:
                # Для части методов WB требуется токен без префикса Bearer.
                self.headers["Authorization"] = self._auth_raw
                continue

            if response.status_code == 429 or 500 <= response.status_code <= 504:
                if attempt + 1 < retries:
                    retry_after = response.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else 2**attempt
                    time.sleep(sleep_s)
                    continue

            if not response.ok:
                raise WildberriesAPIError(
                    f"HTTP {response.status_code} for {url}: {response.text}"
                )

            try:
                return response.json()
            except ValueError as exc:
                raise WildberriesAPIError(
                    f"Invalid JSON from Wildberries API: {response.text}"
                ) from exc

        raise WildberriesAPIError(f"Request failed for {url}: {last_error}")


def get_default_client() -> WildberriesClient:
    return WildberriesClient()
