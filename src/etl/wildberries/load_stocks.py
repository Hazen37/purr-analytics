from __future__ import annotations

import json
from typing import Any, Dict, List

from src.core.config import settings
from src.core.db import execute_query
from src.wildberries.seller_api import get_default_client


def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("stocks"), list):
        return payload["stocks"]
    if isinstance(payload.get("data"), list):
        return payload["data"]
    if isinstance(payload.get("result"), list):
        return payload["result"]
    return []


def load_stocks_current() -> None:
    client = get_default_client()
    analytics_base = settings.WILDBERRIES_ANALYTICS_BASE_URL.rstrip("/")
    mp_base = settings.WILDBERRIES_BASE_URL.rstrip("/")
    candidates = [
        (f"{analytics_base}/api/analytics/v1/stocks-report/wb-warehouses", {}),
        (f"{mp_base}/api/v3/stocks", {"limit": 1000}),
        (f"{mp_base}/api/v1/stocks", {}),
    ]

    rows: List[Dict[str, Any]] = []
    for path, params in candidates:
        try:
            if "/api/analytics/" in path:
                payload = client.request("POST", path, payload={"limit": 1000, "offset": 0})
            else:
                payload = client.request("GET", path, params=params)
            rows = _extract_rows(payload)
            if rows:
                break
        except Exception as exc:
            print(f"[wb_stocks] warn: {path} failed: {exc}")

    if not rows:
        print("[wb_stocks] no rows, skip")
        return

    for row in rows:
        sku = str(row.get("sku") or row.get("nmId") or row.get("barcode") or "")
        warehouse_name = str(row.get("warehouseName") or row.get("warehouse") or "unknown")
        if not sku:
            continue

        execute_query(
            """
            INSERT INTO wb_stocks_current (
                sku, warehouse_name, quantity, in_way_to_client, in_way_from_client, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (sku, warehouse_name) DO UPDATE
            SET quantity = EXCLUDED.quantity,
                in_way_to_client = EXCLUDED.in_way_to_client,
                in_way_from_client = EXCLUDED.in_way_from_client,
                updated_at = now(),
                raw = EXCLUDED.raw;
            """,
            (
                sku,
                warehouse_name,
                row.get("quantity") or row.get("qty") or 0,
                row.get("inWayToClient") or 0,
                row.get("inWayFromClient") or 0,
                json.dumps(row, ensure_ascii=False),
            ),
        )

    print(f"[wb_stocks] upserted={len(rows)}")
