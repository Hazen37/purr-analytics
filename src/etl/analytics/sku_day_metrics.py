from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

from src.core.db import execute_query
from src.ozon.seller_api import get_default_seller_client, OzonSellerAPIError


UPSERT_SQL = """
INSERT INTO public.ozon_sku_day_metrics
  (date, sku, impressions, views, cart_adds, ordered_units, revenue, loaded_at)
VALUES
  (%(date)s::date, %(sku)s::bigint, %(impressions)s, %(views)s, %(cart_adds)s, %(ordered_units)s, %(revenue)s, now())
ON CONFLICT (date, sku) DO UPDATE SET
  impressions   = EXCLUDED.impressions,
  views         = EXCLUDED.views,
  cart_adds     = EXCLUDED.cart_adds,
  ordered_units = EXCLUDED.ordered_units,
  revenue       = EXCLUDED.revenue,
  loaded_at     = now();
"""


def _safe_int(x) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0


def _safe_num(x) -> float:
    try:
        return float(x or 0)
    except Exception:
        return 0.0


def _extract_rows(resp: dict) -> list[dict]:
    if isinstance(resp.get("result"), dict) and isinstance(resp["result"].get("data"), list):
        return resp["result"]["data"] or []
    if isinstance(resp.get("data"), list):
        return resp["data"] or []
    return []


def _guess_min_from_error(msg: str) -> Optional[date]:
    # лёгкая эвристика: если Ozon скажет "90 дней" — срежем
    import re
    from datetime import date, timedelta

    t = msg.lower()
    m = re.search(r"(\d+)\s*(day|days|дн|дней)", t)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d{4}-\d{2}-\d{2})", t)
    if m:
        try:
            y, mo, d = map(int, m.group(1).split("-"))
            return date(y, mo, d)
        except Exception:
            return None
    return None


def fetch_sku_day_funnel(client: OzonSellerAPIClient, date_from: date, date_to: date) -> List[Dict]:
    metrics = ["impressions", "views", "cart_adds", "ordered_units", "revenue"]
    dimensions = ["day", "sku"]

    payload_base = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "metrics": metrics,
        "dimensions": dimensions,
        "limit": 1000,
        "offset": 0,
    }

    try:
        first = client._post("/v1/analytics/data", payload_base)  # новый метод
    except OzonSellerAPIError as e:
        # non-premium ограничение по периоду — срезаем
        min_from = _guess_min_from_error(str(e))
        if min_from and min_from > date_from:
            payload_base["date_from"] = min_from.isoformat()
            first = client.get_analytics_data(payload_base)
        else:
            raise

    raw = _extract_rows(first)
    
    print("DEBUG METRICS SAMPLE:", raw[0])
    
    offset = 1000
    while True:
        payload = dict(payload_base)
        payload["offset"] = offset
        resp = client._post("/v1/analytics/data", payload)
        batch = _extract_rows(resp)
        if not batch:
            break
        raw.extend(batch)
        offset += 1000

    out: List[Dict] = []
    for r in raw:
        dims = r.get("dimensions") or []
        mets = r.get("metrics") or []

        dim_ids = [d.get("id") for d in dims if isinstance(d, dict)]
        day_val = None
        sku_val = None

        for v in dim_ids:
            if isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-":
                day_val = v
            else:
                try:
                    sku_val = int(v)
                except Exception:
                    pass

        if not day_val or sku_val is None:
            continue

        out.append(
            {
                "date": day_val,
                "sku": sku_val,
                "impressions": _safe_int(mets[0] if len(mets) > 0 else 0),
                "views": _safe_int(mets[1] if len(mets) > 1 else 0),
                "cart_adds": _safe_int(mets[2] if len(mets) > 2 else 0),
                "ordered_units": _safe_int(mets[3] if len(mets) > 3 else 0),
                "revenue": _safe_num(mets[4] if len(mets) > 4 else 0),
            }
        )

    return out


def load_sku_day_metrics(date_from_s: str, date_to_s: str, recalc_days: int = 14) -> None:
    """
    Грузим диапазон, и дополнительно хвост recalc_days.
    """
    date_from = datetime.strptime(date_from_s, "%Y-%m-%d").date()
    date_to = datetime.strptime(date_to_s, "%Y-%m-%d").date()

    client = get_default_seller_client()

    # основной диапазон
    rows = fetch_sku_day_funnel(client, date_from, date_to)

    # хвост
    tail_from = max(date_from, date.today() - timedelta(days=recalc_days))
    if tail_from != date_from:
        tail_rows = fetch_sku_day_funnel(client, tail_from, date_to)
        seen = {(r["date"], r["sku"]) for r in rows}
        for r in tail_rows:
            k = (r["date"], r["sku"])
            if k not in seen:
                rows.append(r)
                seen.add(k)

    for r in rows:
        # execute_query у тебя точно есть и умеет бегать запросы
        execute_query(UPSERT_SQL, r)