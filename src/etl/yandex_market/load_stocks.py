from __future__ import annotations

import json
from typing import Any, Dict, List

from src.core.config import settings
from src.core.db import execute_query, fetch_all
from src.yandex_market.seller_api import get_default_client


def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("offers"), list):
        return payload["offers"]
    result = payload.get("result") or {}
    if isinstance(result.get("offers"), list):
        return result["offers"]
    if isinstance(result.get("items"), list):
        return result["items"]
    return []


def load_stocks_current() -> None:
    client = get_default_client()
    campaign_id = settings.YANDEXMARKET_CAMPAIGN_ID
    warehouse_id = settings.YANDEXMARKET_WAREHOUSE_ID

    shop_skus_rows = fetch_all(
        """
        SELECT DISTINCT offer_id
        FROM ym_order_items
        WHERE offer_id IS NOT NULL AND offer_id <> ''
        LIMIT 500;
        """
    )
    shop_skus = [str(r[0]) for r in shop_skus_rows if r[0]]

    candidates = [
        (
            "POST",
            f"/v2/campaigns/{campaign_id}/offers/stocks",
            {},
            {"shopSkus": shop_skus, "warehouseId": warehouse_id},
        ),
        (
            "POST",
            f"/v2/campaigns/{campaign_id}/stats/skus",
            {},
            {"warehouseId": warehouse_id, "shopSkus": shop_skus},
        ),
    ]

    rows: List[Dict[str, Any]] = []
    if not shop_skus:
        print("[ym_stocks] no shopSkus yet (empty ym_order_items), skip")
        return

    for method, path, params, payload in candidates:
        try:
            response_payload = client.request(method, path, params=params, payload=payload)
            rows = _extract_rows(response_payload)
            if rows:
                break
        except Exception as exc:
            print(f"[ym_stocks] warn: {path} failed: {exc}")

    if not rows:
        print("[ym_stocks] no rows, skip")
        return

    for row in rows:
        offer_id = str(row.get("offerId") or row.get("shopSku") or "")
        if not offer_id:
            continue
        execute_query(
            """
            INSERT INTO ym_stocks_current (
                offer_id, sku, warehouse_id, warehouse_name, fit, freeze_qty, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (offer_id, warehouse_id) DO UPDATE
            SET sku = EXCLUDED.sku,
                warehouse_name = EXCLUDED.warehouse_name,
                fit = EXCLUDED.fit,
                freeze_qty = EXCLUDED.freeze_qty,
                updated_at = now(),
                raw = EXCLUDED.raw;
            """,
            (
                offer_id,
                str(row.get("sku") or row.get("marketSku") or ""),
                str(row.get("warehouseId") or warehouse_id or "0"),
                row.get("warehouseName"),
                row.get("fit") or row.get("available") or 0,
                row.get("freeze") or row.get("reserved") or 0,
                json.dumps(row, ensure_ascii=False),
            ),
        )

    print(f"[ym_stocks] upserted={len(rows)}")
