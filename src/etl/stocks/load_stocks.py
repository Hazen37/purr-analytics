from __future__ import annotations

import json
from typing import Any

from src.core.db import execute_query
from src.ozon.seller_api import get_default_seller_client, OzonSellerAPIError


UPSERT_SQL = """
INSERT INTO public.stocks_current
  (sku, warehouse_id, warehouse_name, free_to_sell, reserved, total, updated_at, raw)
VALUES
  (%(sku)s::bigint,
   %(warehouse_id)s::bigint,
   %(warehouse_name)s,
   %(free_to_sell)s::bigint,
   %(reserved)s::bigint,
   %(total)s::bigint,
   now(),
   %(raw)s::jsonb
  )
ON CONFLICT (sku, warehouse_id) DO UPDATE SET
  warehouse_name = EXCLUDED.warehouse_name,
  free_to_sell   = EXCLUDED.free_to_sell,
  reserved       = EXCLUDED.reserved,
  total          = EXCLUDED.total,
  updated_at     = now(),
  raw            = EXCLUDED.raw;
"""


def _safe_int(x: Any) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0


def load_stocks_current() -> None:
    """
    Тянем остатки через /v2/analytics/stock_on_warehouses (offset-based).
    Если у ключа нет роли — мягко пропускаем.
    """
    client = get_default_seller_client()

    limit = 1000
    offset = 0
    total_rows = 0

    try:
        while True:
            resp = client.get_stock_on_warehouses(limit=limit, offset=offset)

            # Варианты формата: result может быть list или dict с items
            result = resp.get("result")
            if result is None:
                items = []
            elif isinstance(result, list):
                items = result
            else:
                items = result.get("items") or result.get("rows") or []

            if not items:
                break

            totals: dict[int, int] = {}  # sku -> sum free_to_sell_amount по всем складам

            for r in items:
                sku = r.get("sku") or r.get("product_id") or r.get("productId")
                if not sku:
                    continue

                sku_i = _safe_int(sku)

                # В твоём реальном ответе поле называется free_to_sell_amount
                free_to_sell = _safe_int(
                    r.get("free_to_sell_amount")
                    or r.get("free_to_sell")
                    or r.get("freeToSell")
                    or r.get("present")
                    or r.get("available")
                )

                totals[sku_i] = totals.get(sku_i, 0) + free_to_sell

            # Пишем агрегированно: 1 строка на SKU
            for sku_i, free_sum in totals.items():
                execute_query(UPSERT_SQL, {
                    "sku": sku_i,
                    "warehouse_id": 0,
                    "warehouse_name": "ALL_FBO",
                    "free_to_sell": free_sum,
                    "reserved": 0,
                    "total": free_sum,
                    "raw": json.dumps({"agg": True, "free_to_sell_amount": free_sum}, ensure_ascii=False),
                })
                total_rows += 1

            offset += limit

        print(f"[stocks] rows upserted: {total_rows}")

    except OzonSellerAPIError as e:
        s = str(e)
        if "missing a required role" in s or '"code":7' in s or "статус 403" in s:
            print("[stocks] WARNING: Api-Key has no required role for stocks endpoint. Skipping stocks загрузку.")
            return
        raise