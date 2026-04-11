from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List

from src.core.config import settings
from src.core.db import execute_query
from src.yandex_market.seller_api import get_default_client


def _dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value).replace(",", "."))
    except Exception:
        return Decimal("0")


def _dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    for candidate in (s, s.replace(" ", "T")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def _extract_orders(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("orders"), list):
        return payload["orders"]
    result = payload.get("result") or {}
    if isinstance(result.get("orders"), list):
        return result["orders"]
    if isinstance(result.get("items"), list):
        return result["items"]
    return []


def _extract_next_page_token(payload: Dict[str, Any]) -> str | None:
    pager = payload.get("pager") or {}
    token = pager.get("nextPageToken")
    if token:
        return str(token)
    result = payload.get("result") or {}
    pager = result.get("pager") or {}
    token = pager.get("nextPageToken")
    return str(token) if token else None


def _sync_order_items(order_id: str, items: List[Dict[str, Any]]) -> None:
    execute_query("DELETE FROM ym_order_items WHERE order_id = %s;", (order_id,))
    for item in items:
        execute_query(
            """
            INSERT INTO ym_order_items (order_id, offer_id, sku, name, quantity, price, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                order_id,
                str(item.get("offerId") or item.get("shopSku") or ""),
                str(item.get("sku") or item.get("marketSku") or ""),
                item.get("offerName") or item.get("name"),
                _dec(item.get("count") or item.get("quantity") or 1),
                _dec(item.get("buyerPrice") or item.get("price")),
                json.dumps(item, ensure_ascii=False),
            ),
        )


def load_orders(date_from: str, date_to: str) -> None:
    client = get_default_client()
    campaign_id = settings.YANDEXMARKET_CAMPAIGN_ID
    business_id = settings.YANDEXMARKET_BUSINESS_ID
    warehouse_id = settings.YANDEXMARKET_WAREHOUSE_ID

    print(f"[ym_orders] load orders for campaign={campaign_id} range={date_from}..{date_to}")

    next_page_token: str | None = None
    page = 0
    saved = 0

    while True:
        page += 1
        params: Dict[str, Any] = {"limit": 200}
        if next_page_token:
            params["pageToken"] = next_page_token

        payload = client.request("GET", f"/v2/campaigns/{campaign_id}/orders", params=params)
        orders = _extract_orders(payload)
        if not orders:
            break

        for order in orders:
            order_id = str(order.get("id") or order.get("orderId") or "")
            if not order_id:
                continue
            order_dt = _dt(order.get("creationDate") or order.get("createdAt") or order.get("updatedAt"))
            if order_dt:
                if order_dt.date() < datetime.fromisoformat(date_from).date():
                    continue
                if order_dt.date() > datetime.fromisoformat(date_to).date():
                    continue

            total_amount = _dec(order.get("itemsTotal") or order.get("buyerItemsTotal") or order.get("paymentTotal"))
            status = order.get("status")
            currency = order.get("currency")

            execute_query(
                """
                INSERT INTO ym_orders (
                    order_id, campaign_id, business_id, order_date, status, currency, total_amount, warehouse_id, raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (order_id) DO UPDATE
                SET campaign_id = EXCLUDED.campaign_id,
                    business_id = EXCLUDED.business_id,
                    order_date = EXCLUDED.order_date,
                    status = EXCLUDED.status,
                    currency = EXCLUDED.currency,
                    total_amount = EXCLUDED.total_amount,
                    warehouse_id = EXCLUDED.warehouse_id,
                    updated_at = now(),
                    raw = EXCLUDED.raw;
                """,
                (
                    order_id,
                    int(campaign_id) if str(campaign_id).isdigit() else None,
                    int(business_id) if str(business_id).isdigit() else None,
                    order_dt,
                    status,
                    currency,
                    total_amount,
                    warehouse_id,
                    json.dumps(order, ensure_ascii=False),
                ),
            )

            items = order.get("items") if isinstance(order.get("items"), list) else []
            _sync_order_items(order_id, items)
            saved += 1

        next_page_token = _extract_next_page_token(payload)
        if not next_page_token:
            break
        if page >= 200:
            print("[ym_orders] pagination safety stop at 200 pages")
            break

    print(f"[ym_orders] done, upserted={saved}")
