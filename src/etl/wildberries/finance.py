from __future__ import annotations

from src.core.db import execute_query


def run(date_from: str, date_to: str) -> None:
    """
    Минимальный finance-слой для WB:
    формируем базовый факт суммы заказа в wb_finance_items.
    """
    print(f"[wb_finance] rebuild for range={date_from}..{date_to}")

    execute_query(
        """
        DELETE FROM wb_finance_items f
        USING wb_orders o
        WHERE f.order_id = o.order_id
          AND o.order_date::date BETWEEN %s::date AND %s::date;
        """,
        (date_from, date_to),
    )

    execute_query(
        """
        INSERT INTO wb_finance_items (order_id, fee_type, amount, happened_at, raw)
        SELECT
            o.order_id,
            'order_total' AS fee_type,
            COALESCE(NULLIF(o.sale_price, 0), o.price, 0) AS amount,
            o.order_date AS happened_at,
            o.raw
        FROM wb_orders o
        WHERE o.order_date::date BETWEEN %s::date AND %s::date;
        """,
        (date_from, date_to),
    )

    print("[wb_finance] OK ✅")
