from __future__ import annotations

from src.core.db import execute_query


def run(date_from: str, date_to: str) -> None:
    """
    Базовый ads daily для WB.
    Пока агрегируем order-based факты в совместимую витрину ads.
    """
    campaign_id = "wb"
    print(f"[wb_ads] rebuild range={date_from}..{date_to}")

    execute_query(
        """
        DELETE FROM wb_ads_campaign_daily
        WHERE stat_date BETWEEN %s::date AND %s::date;
        """,
        (date_from, date_to),
    )

    execute_query(
        """
        INSERT INTO wb_ads_campaign_daily (
            campaign_id, stat_date, impressions, clicks, spend, orders_cnt, orders_amount, raw
        )
        SELECT
            %s AS campaign_id,
            o.order_date::date AS stat_date,
            0::bigint AS impressions,
            0::bigint AS clicks,
            0::numeric AS spend,
            COUNT(*)::bigint AS orders_cnt,
            COALESCE(SUM(COALESCE(NULLIF(o.sale_price, 0), o.price, 0)), 0) AS orders_amount,
            NULL::jsonb AS raw
        FROM wb_orders o
        WHERE o.order_date::date BETWEEN %s::date AND %s::date
        GROUP BY 2
        ON CONFLICT (campaign_id, stat_date) DO UPDATE
        SET impressions = EXCLUDED.impressions,
            clicks = EXCLUDED.clicks,
            spend = EXCLUDED.spend,
            orders_cnt = EXCLUDED.orders_cnt,
            orders_amount = EXCLUDED.orders_amount,
            loaded_at = now();
        """,
        (campaign_id, date_from, date_to),
    )

    print("[wb_ads] OK ✅")
