"""
Minimal client for Yandex Market Partner API.

Docs: https://yandex.ru/dev/market/partner-api/doc/ru/
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests

from src.core.config import settings


class YandexMarketAPIError(Exception):
    """Raised when Yandex Market API request fails."""


class YandexMarketClient:
    BASE_URL = "https://api.partner.market.yandex.ru"

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        self.api_key = api_key or settings.YANDEXMARKET_API_KEY
        self.timeout = timeout
        self.headers = {
            "Api-Key": self.api_key,
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
                raise YandexMarketAPIError(f"Network error for {url}: {exc}") from exc

            if response.status_code == 429 or 500 <= response.status_code <= 504:
                if attempt + 1 < retries:
                    retry_after = response.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else 2**attempt
                    time.sleep(sleep_s)
                    continue

            if not response.ok:
                raise YandexMarketAPIError(
                    f"HTTP {response.status_code} for {url}: {response.text}"
                )

            try:
                return response.json()
            except ValueError as exc:
                raise YandexMarketAPIError(
                    f"Invalid JSON from Yandex Market API: {response.text}"
                ) from exc

        raise YandexMarketAPIError(f"Request failed for {url}: {last_error}")


def get_default_client() -> YandexMarketClient:
    return YandexMarketClient()
