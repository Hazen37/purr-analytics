from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import requests
import csv
import io

from .config import settings
from .db import execute_query

BASE = "https://api-performance.ozon.ru"

def _dec_ru(x: str | None) -> Decimal:
    if not x:
        return Decimal("0")
    return Decimal(str(x).replace(" ", "").replace(",", "."))

def get_token() -> str:
    r = requests.post(
        f"{BASE}/api/client/token",
        json={
            "client_id": settings.OZON_PERF_CLIENT_ID,
            "client_secret": settings.OZON_PERF_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_daily_json(date_from: str, date_to: str) -> dict:
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from, "dateTo": date_to}
    r = requests.get(
        f"{BASE}/api/client/statistics/daily/json",
        headers=headers,
        params=params,
        timeout=90,
    )
    r.raise_for_status()
    return r.json()

def load_daily(date_from: str, date_to: str) -> int:
    data = fetch_daily_json(date_from, date_to)
    rows = data.get("rows") or []

    upsert_q = """
    INSERT INTO performance_campaign_daily (
      campaign_id, campaign_title, stat_date,
      impressions, clicks, spend, avg_bid, orders_cnt, orders_amount
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (campaign_id, stat_date) DO UPDATE SET
      campaign_title = EXCLUDED.campaign_title,
      impressions    = EXCLUDED.impressions,
      clicks         = EXCLUDED.clicks,
      spend          = EXCLUDED.spend,
      avg_bid        = EXCLUDED.avg_bid,
      orders_cnt     = EXCLUDED.orders_cnt,
      orders_amount  = EXCLUDED.orders_amount;
    """

    n = 0
    for r in rows:
        campaign_id    = str(r.get("id") or "").strip()
        if not campaign_id:
            continue
        campaign_title = (r.get("title") or "").strip()
        stat_date      = datetime.strptime(r["date"], "%Y-%m-%d").date()

        impressions    = int(r.get("views") or 0)
        clicks         = int(r.get("clicks") or 0)
        spend          = _dec_ru(r.get("moneySpent"))
        avg_bid        = _dec_ru(r.get("avgBid"))
        orders_cnt     = int(r.get("orders") or 0)
        orders_amount  = _dec_ru(r.get("ordersMoney"))

        execute_query(upsert_q, (
            campaign_id, campaign_title, stat_date,
            impressions, clicks, spend, avg_bid, orders_cnt, orders_amount
        ))
        n += 1

    return n

from datetime import datetime, timedelta

MAX_WINDOW_DAYS = 30  # безопасно (если лимит 62)

def run(date_from_str: str, date_to_str: str):
    date_from = datetime.strptime(date_from_str, "%Y-%m-%d").date()
    date_to   = datetime.strptime(date_to_str, "%Y-%m-%d").date()

    cur_from = date_from
    total = 0

    while cur_from <= date_to:
        cur_to = min(cur_from + timedelta(days=MAX_WINDOW_DAYS - 1), date_to)
        print(f"[performance_daily] window: {cur_from} — {cur_to}")

        total += load_daily(str(cur_from), str(cur_to))

        cur_from = cur_to + timedelta(days=1)

    print(f"[performance_daily] rows upserted: {total}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m src.performance_daily_etl YYYY-MM-DD YYYY-MM-DD")
    run(sys.argv[1], sys.argv[2])