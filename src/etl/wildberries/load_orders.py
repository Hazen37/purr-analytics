from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List

from src.core.db import execute_query
from src.wildberries.seller_api import get_default_client


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
    s = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _extract_orders(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("orders"), list):
        return payload["orders"]
    if isinstance(payload.get("data"), list):
        return payload["data"]
    result = payload.get("result")
    if isinstance(result, list):
        return result
    if isinstance(result, dict) and isinstance(result.get("orders"), list):
        return result["orders"]
    return []


def _sync_item(order_id: str, order: Dict[str, Any]) -> None:
    execute_query("DELETE FROM wb_order_items WHERE order_id = %s;", (order_id,))
    execute_query(
        """
        INSERT INTO wb_order_items (order_id, sku, article, quantity, price, raw)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb);
        """,
        (
            order_id,
            str(order.get("sku") or order.get("nmId") or ""),
            str(order.get("article") or order.get("supplierArticle") or ""),
            _dec(order.get("quantity") or 1),
            _dec(order.get("price") or order.get("salePrice")),
            json.dumps(order, ensure_ascii=False),
        ),
    )


def _upsert_orders(orders: List[Dict[str, Any]], date_from: str, date_to: str) -> int:
    inserted = 0
    from_d = datetime.fromisoformat(date_from).date()
    to_d = datetime.fromisoformat(date_to).date()

    for order in orders:
        order_id = str(order.get("id") or order.get("orderId") or order.get("rid") or "")
        if not order_id:
            continue

        order_dt = _dt(order.get("createdAt") or order.get("dateCreated") or order.get("lastChangeDate"))
        if order_dt:
            if order_dt.date() < from_d or order_dt.date() > to_d:
                continue

        execute_query(
            """
            INSERT INTO wb_orders (
                order_id, order_uid, order_date, status, warehouse_name, article, nm_id, price, sale_price, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (order_id) DO UPDATE
            SET order_uid = EXCLUDED.order_uid,
                order_date = EXCLUDED.order_date,
                status = EXCLUDED.status,
                warehouse_name = EXCLUDED.warehouse_name,
                article = EXCLUDED.article,
                nm_id = EXCLUDED.nm_id,
                price = EXCLUDED.price,
                sale_price = EXCLUDED.sale_price,
                updated_at = now(),
                raw = EXCLUDED.raw;
            """,
            (
                order_id,
                order.get("orderUid") or order.get("rid"),
                order_dt,
                order.get("status"),
                order.get("warehouseName") or order.get("warehouse"),
                order.get("article") or order.get("supplierArticle"),
                order.get("nmId"),
                _dec(order.get("price")),
                _dec(order.get("salePrice") or order.get("convertedPrice")),
                json.dumps(order, ensure_ascii=False),
            ),
        )
        _sync_item(order_id, order)
        inserted += 1
    return inserted


def load_orders(date_from: str, date_to: str) -> None:
    client = get_default_client()
    print(f"[wb_orders] load range={date_from}..{date_to}")

    total = 0

    # Новые заказы (распространённый endpoint маркетплейса WB).
    try:
        payload_new = client.request("GET", "/api/v3/orders/new")
        total += _upsert_orders(_extract_orders(payload_new), date_from, date_to)
    except Exception as exc:
        print(f"[wb_orders] warn: /api/v3/orders/new failed: {exc}")

    # История/выполненные (может быть недоступно для некоторых схем токенов).
    # Для минимизации ложных 400/404 не дёргаем дополнительные endpoints без явной модели.

    print(f"[wb_orders] done, upserted={total}")
