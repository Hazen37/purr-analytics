from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests


API_SELLER = "https://api-seller.ozon.ru"
DEFAULT_LIMIT = 1000
FALLBACK_NON_PREMIUM_DAYS = 90  # если не смогли понять лимит из текста ошибки


class OzonAnalyticsError(RuntimeError):
    pass


@dataclass(frozen=True)
class AnalyticsResult:
    effective_date_from: date
    rows: List[Dict[str, Any]]  # normalized flat rows for DB


def _headers() -> Dict[str, str]:
    # используй те же env, что у тебя уже для seller_api
    client_id = os.getenv("OZON_CLIENT_ID", "")
    api_key = os.getenv("OZON_API_KEY", "")
    if not client_id or not api_key:
        raise OzonAnalyticsError("Missing OZON_CLIENT_ID / OZON_API_KEY env vars")
    return {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }


def _post(path: str, payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    url = f"{API_SELLER}{path}"
    r = requests.post(url, headers=_headers(), json=payload, timeout=timeout)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            raise OzonAnalyticsError(f"HTTP {r.status_code}: {r.text}")
        raise OzonAnalyticsError(f"HTTP {r.status_code}: {err}")
    return r.json()


def _extract_rows(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(resp.get("result"), dict) and isinstance(resp["result"].get("data"), list):
        return resp["result"]["data"] or []
    if isinstance(resp.get("data"), list):
        return resp["data"] or []
    return []


def _guess_min_date_from_error(err_text: str) -> Optional[date]:
    """
    Эвристика: Ozon может писать "не более N дней" или вставлять дату.
    Если не нашли — вернём None.
    """
    t = err_text.lower()

    m = re.search(r"(\d+)\s*(day|days|дн|дней)", t)
    if m:
        days = int(m.group(1))
        return date.today() - timedelta(days=days)

    m = re.search(r"(\d{4}-\d{2}-\d{2})", t)
    if m:
        try:
            y, mo, d = map(int, m.group(1).split("-"))
            return date(y, mo, d)
        except Exception:
            return None

    return None


def fetch_sku_day_funnel(
    date_from: date,
    date_to: date,
    limit: int = DEFAULT_LIMIT,
) -> AnalyticsResult:
    """
    Универсально Premium/без Premium:
    - пробуем запросить date_from..date_to
    - если период слишком старый -> сдвигаем date_from вперед и повторяем
    - пагинируем offset
    - нормализуем в строки: date, sku, impressions, views, cart_adds, ordered_units, revenue
    """

    metrics = ["impressions", "views", "cart_adds", "ordered_units", "revenue"]
    dimensions = ["day", "sku"]

    payload_base: Dict[str, Any] = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "metrics": metrics,
        "dimensions": dimensions,
        "limit": limit,
        "offset": 0,
    }

    effective_from = date_from

    try:
        first = _post("/v1/analytics/data", payload_base)
    except Exception as e:
        err_text = str(e)
        min_from = _guess_min_date_from_error(err_text) or (date.today() - timedelta(days=FALLBACK_NON_PREMIUM_DAYS))
        if min_from > date_from:
            effective_from = min_from
            payload_base["date_from"] = effective_from.isoformat()
            first = _post("/v1/analytics/data", payload_base)
        else:
            raise

    raw = _extract_rows(first)

    offset = limit
    while True:
        payload = dict(payload_base)
        payload["offset"] = offset
        resp = _post("/v1/analytics/data", payload)
        batch = _extract_rows(resp)
        if not batch:
            break
        raw.extend(batch)
        offset += limit

    out: List[Dict[str, Any]] = []
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

        def m_int(i: int) -> int:
            if i >= len(mets) or mets[i] is None:
                return 0
            try:
                return int(mets[i])
            except Exception:
                return 0

        def m_num(i: int) -> float:
            if i >= len(mets) or mets[i] is None:
                return 0.0
            try:
                return float(mets[i])
            except Exception:
                return 0.0

        out.append(
            {
                "date": day_val,
                "sku": sku_val,
                "impressions": m_int(0),
                "views": m_int(1),
                "cart_adds": m_int(2),
                "ordered_units": m_int(3),
                "revenue": m_num(4),
            }
        )

    return AnalyticsResult(effective_date_from=effective_from, rows=out)