from __future__ import annotations

import requests
from datetime import datetime

from .config import settings
from .db import execute_query

from psycopg2.extras import Json

BASE = "https://api-performance.ozon.ru"


def get_token() -> str:
    r = requests.post(
        f"{BASE}/api/client/token",
        json={
            "client_id": settings.OZON_PERF_CLIENT_ID,
            "client_secret": settings.OZON_PERF_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def load_campaigns():
    token = get_token()
    r = requests.get(
        f"{BASE}/api/client/campaign",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    r.raise_for_status()

    data = r.json()
    campaigns = data.get("list") or []

    q = """
    INSERT INTO perf_campaigns (
        campaign_id, title, state, adv_object_type,
        payment_type, created_at, updated_at, raw
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (campaign_id) DO UPDATE
    SET
        title = EXCLUDED.title,
        state = EXCLUDED.state,
        adv_object_type = EXCLUDED.adv_object_type,
        payment_type = EXCLUDED.payment_type,
        updated_at = EXCLUDED.updated_at,
        raw = EXCLUDED.raw;
    """

    for c in campaigns:
        execute_query(
            q,
            (
                c["id"],
                c.get("title"),
                c.get("state"),
                c.get("advObjectType"),
                c.get("PaymentType"),
                c.get("createdAt"),
                c.get("updatedAt"),
                Json(c),
            ),
        )

    print(f"[perf] campaigns loaded: {len(campaigns)}")


if __name__ == "__main__":
    load_campaigns()
