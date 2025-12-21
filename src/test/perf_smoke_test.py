# src/perf_smoke_test.py
from __future__ import annotations

import sys
import requests

from ..config import settings

BASE = "https://api-performance.ozon.ru"


def get_token() -> str:
    url = f"{BASE}/api/client/token"
    payload = {
        "client_id": settings.OZON_PERF_CLIENT_ID,
        "client_secret": settings.OZON_PERF_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"token error {r.status_code}: {r.text}")
    return r.json()["access_token"]


def get_campaigns(token: str) -> dict:
    url = f"{BASE}/api/client/campaign"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=60)
    if not r.ok:
        raise RuntimeError(f"campaign error {r.status_code}: {r.text}")
    return r.json()


def main():
    token = get_token()
    data = get_campaigns(token)

    lst = data.get("list") or []
    print("TOTAL:", data.get("total"))
    print("FIRST 2:")
    for c in lst[:2]:
        print(
            {
                "id": c.get("id"),
                "title": c.get("title"),
                "state": c.get("state"),
                "advObjectType": c.get("advObjectType"),
                "paymentType": c.get("PaymentType") or c.get("paymentType"),
            }
        )


if __name__ == "__main__":
    main()
