from __future__ import annotations

from datetime import datetime, timedelta, date
from decimal import Decimal
import time
import random
import requests

from src.core.config import settings
from src.core.db import execute_query
from src.core.db import fetch_one

BASE_URL = "https://api-performance.ozon.ru"

def parse_date_any(s: str | None):
    """Парсим даты из Performance API: '05.12.2025' или '2025-12-05'."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unknown date format: {s}")

def dec_ru(x) -> Decimal:
    """Парсим деньги вида '1811,00' -> Decimal('1811.00')"""
    if x is None:
        return Decimal("0")
    s = str(x).strip().replace(" ", "").replace("\u00a0", "")
    if s == "":
        return Decimal("0")
    return Decimal(s.replace(",", "."))

def resolve_posting_order_id(order_number: str | None) -> tuple[str | None, str | None]:
    """
    order_number из Performance: '47533921-0235'
    В orders лежит posting_number: '47533921-0235-1'
    Возвращаем:
      (order_id_to_save, ext_order_id)
    """
    if not order_number:
        return None, None

    cand = str(order_number).strip()

    row = fetch_one(
        "SELECT order_id FROM orders WHERE order_id LIKE %s ORDER BY order_date DESC LIMIT 1;",
        (cand + "-%",)
    )
    if row and row.get("order_id"):
        return row["order_id"], cand

    return None, cand

def _dec(x) -> Decimal:
    if x is None:
        return Decimal("0")
    return Decimal(str(x).replace(" ", "").replace(",", "."))

def _parse_date(x) -> date | None:
    if not x:
        return None
    s = str(x).strip()

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    # если внезапно пришло "2025-10-24 00:00:00"
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None

def parse_date_any(s: str | None):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unknown date format: {s}")

def _post_json(path: str, payload: dict, token: str) -> dict:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    max_retries = 6
    base_sleep = 1.2

    for attempt in range(max_retries):
        r = requests.post(url, json=payload, headers=headers, timeout=90)
        if r.ok:
            return r.json()
        if r.status_code in (500, 502, 503, 504):
            sleep = base_sleep * (2 ** attempt) + random.uniform(0, 0.5)
            print(f"[perf] {r.status_code} on {path}, retry in {sleep:.1f}s...")
            time.sleep(sleep)
            continue
        raise RuntimeError(f"Performance API error {r.status_code}: {r.text}")

    raise RuntimeError(f"Performance API error: too many retries, last={r.status_code}: {r.text}")

def _get(path: str, token: str) -> dict | str:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=90)
    if not r.ok:
        raise RuntimeError(f"Performance API error {r.status_code}: {r.text}")
    # report может быть CSV/JSON — оставим как текст, если не JSON
    ct = (r.headers.get("Content-Type") or "").lower()
    if "application/json" in ct:
        return r.json()
    return r.text

def get_token() -> str:
    payload = {
        "client_id": settings.OZON_PERF_CLIENT_ID,
        "client_secret": settings.OZON_PERF_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    data = requests.post(f"{BASE_URL}/api/client/token", json=payload, timeout=60)
    data.raise_for_status()
    return data.json()["access_token"]

def order_exists(order_id: str) -> bool:
    return fetch_one("SELECT 1 FROM orders WHERE order_id=%s LIMIT 1;", (order_id,)) is not None

def resolve_order_id(candidate: str | None) -> tuple[str | None, str | None]:
    if not candidate:
        return None, None

    cand = str(candidate).strip()

    if order_exists(cand):
        return cand, None

    row = fetch_one(
        "SELECT order_id FROM orders WHERE order_id LIKE %s ORDER BY order_date DESC LIMIT 1;",
        (cand + "-%",)
    )
    if row and row.get("order_id"):
        return row["order_id"], cand

    return None, cand

def generate_orders_report(date_from: date, date_to: date, token: str) -> str:
    payload = {
        "from": f"{date_from.isoformat()}T00:00:00Z",
        "to":   f"{date_to.isoformat()}T23:59:59Z",
        # по аналогии с твоими запросами: campaigns
        # можно не указывать campaigns, если отчёт отдаётся по всем
        # "campaigns": ["18179987","18179988"]
    }
    data = _post_json("/api/client/statistic/orders/generate/json", payload, token)
    # обычно возвращает {"UUID":"..."}
    return data["UUID"]

def wait_report(uuid: str, token: str, max_wait_sec: int = 180) -> str:
    started = time.time()
    while True:
        st = _get(f"/api/client/statistics/{uuid}", token)
        state = st.get("state")
        if state == "OK":
            return st.get("link") or f"/api/client/statistics/report?UUID={uuid}"
        if state in ("ERROR", "FAILED"):
            raise RuntimeError(f"[perf] report {uuid} failed: {st}")
        if time.time() - started > max_wait_sec:
            raise RuntimeError(f"[perf] report {uuid} timeout, last state={state}")
        time.sleep(1.5)

def fetch_report_json(uuid: str, token: str) -> dict:
    # чаще всего: /api/client/statistics/report?UUID=...
    data = _get(f"/api/client/statistics/report?UUID={uuid}", token)
    if isinstance(data, dict):
        return data
    # если пришёл CSV — тут можно добавить парсер, но начнём с JSON
    raise RuntimeError("Report returned non-JSON (CSV). Use /json endpoint or parse CSV.")

def load_report_rows(rows: list[dict]) -> int:
    matched = 0
    unmatched = 0
    insert_q = """
      INSERT INTO performance_order_attribution (
        campaign_id, campaign_title,
        order_id, ext_order_id,
        sku, offer_id, product_name,
        stat_date,
        price, amount,
        spent, bid, bid_percent, qty,
        source
      )
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'performance_api')
      ON CONFLICT DO NOTHING;
    """

    n = 0
    for r in rows:
        # В orders-report может НЕ быть campaignId — тогда кладём в '0' = UNKNOWN,
        # чтобы пройти NOT NULL в таблице performance_order_attribution.
        campaign_id = str(
            r.get("campaignId") or r.get("campaign_id") or r.get("id") or "0"
        ).strip() or "0"

        campaign_title = (
            r.get("campaignTitle")
            or r.get("campaign_title")
            or r.get("title")
            or r.get("ordersSource")     # полезно для CPO, чтобы понимать источник
            or None
        )

        stat_date = parse_date_any(r.get("date"))

        # В Performance "orderNumber" = корень posting_number (например '47533921-0235')
        raw_order = r.get("orderNumber") or r.get("orderNumberId") or r.get("order_id") or r.get("orderId")
        order_id, ext_order_id = resolve_posting_order_id(raw_order)

        sku = r.get("sku") or r.get("skuId")
        try:
            sku = int(sku) if sku is not None else None
        except:
            sku = None

        offer_id = r.get("offerId") or r.get("offer_id")
        product_name = r.get("name") or r.get("productName")

        price = dec_ru(r.get("price"))
        amount = dec_ru(r.get("amount"))
        spent = dec_ru(r.get("moneySpent") or r.get("bidValue") or r.get("spent") or r.get("expense") or r.get("cost"))
        bid = dec_ru(r.get("bid"))
        bid_percent = dec_ru(r.get("bidPercent"))
        qty = r.get("quantity") or r.get("qty")
        try:
            qty = int(qty) if qty is not None else None
        except:
            qty = None

        if not stat_date:
            continue

        if order_id:
          matched += 1
        else:
          unmatched += 1

        execute_query(insert_q, (
            campaign_id, campaign_title,
            order_id, ext_order_id,
            sku, offer_id, product_name,
            stat_date,
            price, amount,
            spent, bid, bid_percent, qty,
        ))
        n += 1

    print(f"[perf_orders] matched to orders: {matched}, unmatched: {unmatched}")
    return n

def apply_to_orders(date_from: date, date_to: date):
    # 1) проставим campaign_id/title в orders (если по заказу есть единственная кампания — берём max)
    execute_query("""
      UPDATE orders o
      SET
        campaign_id = x.campaign_id,
        campaign_title = x.campaign_title
      FROM (
        SELECT
          order_id,
          MAX(campaign_id) AS campaign_id,
          MAX(campaign_title) AS campaign_title
        FROM performance_order_attribution
        WHERE order_id IS NOT NULL
          AND stat_date >= %s AND stat_date <= %s
        GROUP BY order_id
      ) x
      WHERE o.order_id = x.order_id;
    """, (date_from, date_to))

    # 2) реклама, привязанная к заказу
    execute_query("""
      UPDATE orders o
      SET ozon_ads_attributed = COALESCE(x.spend, 0)
      FROM (
        SELECT order_id, SUM(spent) AS spend
        FROM performance_order_attribution
        WHERE order_id IS NOT NULL
          AND stat_date >= %s AND stat_date <= %s
        GROUP BY order_id
      ) x
      WHERE o.order_id = x.order_id;
    """, (date_from, date_to))

def run(date_from_str: str, date_to_str: str):
    date_from = datetime.strptime(date_from_str, "%Y-%m-%d").date()
    date_to   = datetime.strptime(date_to_str, "%Y-%m-%d").date()

    token = get_token()

    # ограничение окна — да, часто ~62 дня. Поэтому режем.
    window_days = 60
    cur_from = date_from
    total = 0

    while cur_from <= date_to:
        cur_to = min(cur_from + timedelta(days=window_days-1), date_to)
        print(f"[perf_orders] window {cur_from}..{cur_to}")

        uuid = generate_orders_report(cur_from, cur_to, token)
        link = wait_report(uuid, token)

        # если link вернул /api/client/statistics/report?UUID=...
        # вытащим UUID оттуда просто берём uuid
        rep = fetch_report_json(uuid, token)

        rows = rep.get("rows") or rep.get("list") or []
        n = load_report_rows(rows)
        total += n

        cur_from = cur_to + timedelta(days=1)
    
    # print(rows[0])
    print(f"[perf_orders] rows inserted: {total}")
    apply_to_orders(date_from, date_to)
    print("[perf_orders] OK ✅")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m src.performance_orders_etl YYYY-MM-DD YYYY-MM-DD")
    run(sys.argv[1], sys.argv[2])
    