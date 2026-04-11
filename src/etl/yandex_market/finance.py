from __future__ import annotations

from src.core.db import execute_query


def run(date_from: str, date_to: str) -> None:
    """
    Минимальный finance-слой для YM:
    раскладываем сумму заказа как базовый факт в ym_finance_items.
    """
    print(f"[ym_finance] rebuild for range={date_from}..{date_to}")

    execute_query(
        """
        DELETE FROM ym_finance_items f
        USING ym_orders o
        WHERE f.order_id = o.order_id
          AND o.order_date::date BETWEEN %s::date AND %s::date;
        """,
        (date_from, date_to),
    )

    execute_query(
        """
        INSERT INTO ym_finance_items (order_id, fee_type, amount, currency, happened_at, raw)
        SELECT
            o.order_id,
            'order_total' AS fee_type,
            COALESCE(o.total_amount, 0) AS amount,
            COALESCE(o.currency, 'RUB') AS currency,
            o.order_date AS happened_at,
            o.raw
        FROM ym_orders o
        WHERE o.order_date::date BETWEEN %s::date AND %s::date;
        """,
        (date_from, date_to),
    )

    print("[ym_finance] OK ✅")
