from __future__ import annotations

from src.core.db import execute_query


def load_sku_day_metrics(date_from: str, date_to: str) -> None:
    """
    Базовая витрина SKU/day для YM из факта заказов.
    Поля impressions/views оставлены нулевыми до подключения профильного API отчётов.
    """
    print(f"[ym_metrics] rebuild range={date_from}..{date_to}")

    execute_query(
        """
        DELETE FROM ym_sku_day_metrics
        WHERE metric_date BETWEEN %s::date AND %s::date;
        """,
        (date_from, date_to),
    )

    execute_query(
        """
        INSERT INTO ym_sku_day_metrics (
            metric_date, sku, impressions, views, ordered_units, revenue
        )
        SELECT
            o.order_date::date AS metric_date,
            COALESCE(i.sku, i.offer_id, 'unknown') AS sku,
            0::bigint AS impressions,
            0::bigint AS views,
            COALESCE(SUM(i.quantity), 0)::bigint AS ordered_units,
            COALESCE(SUM(i.quantity * i.price), 0) AS revenue
        FROM ym_orders o
        JOIN ym_order_items i ON i.order_id = o.order_id
        WHERE o.order_date::date BETWEEN %s::date AND %s::date
        GROUP BY 1, 2
        ON CONFLICT (metric_date, sku) DO UPDATE
        SET ordered_units = EXCLUDED.ordered_units,
            revenue = EXCLUDED.revenue,
            loaded_at = now();
        """,
        (date_from, date_to),
    )

    print("[ym_metrics] OK ✅")
