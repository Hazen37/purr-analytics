-- Metabase native question export
-- card_id: 65
-- card_name: Marketplace / Общее / Сводка по площадкам
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

WITH orders_agg AS (
  SELECT
    marketplace,
    COUNT(DISTINCT order_id) AS orders_total,
    SUM(revenue) AS revenue_total,
    AVG(revenue) AS average_order_value
  FROM public.marketplace_orders
  WHERE status IS DISTINCT FROM 'cancelled'
  GROUP BY marketplace
),
units_agg AS (
  SELECT
    marketplace,
    SUM(quantity) AS units_total
  FROM public.marketplace_order_items
  WHERE status IS DISTINCT FROM 'cancelled'
  GROUP BY marketplace
),
expense_agg AS (
  SELECT marketplace, SUM(amount) AS expense_total
  FROM (
    SELECT marketplace, amount FROM public.marketplace_finance_items
    UNION ALL
    SELECT marketplace, spend AS amount FROM public.marketplace_ads_daily
  ) src
  GROUP BY marketplace
)
SELECT
  INITCAP(REPLACE(f.marketplace, '_', ' ')) AS marketplace,
  COALESCE(o.orders_total, 0) AS orders_total,
  COALESCE(u.units_total, 0) AS units_total,
  COALESCE(o.revenue_total, 0) AS revenue_total,
  ROUND(COALESCE(o.average_order_value, 0), 2) AS average_order_value,
  COALESCE(e.expense_total, 0) AS expense_total,
  f.last_order_at::date AS last_order_date
FROM public.marketplace_data_freshness f
LEFT JOIN orders_agg o ON o.marketplace = f.marketplace
LEFT JOIN units_agg u ON u.marketplace = f.marketplace
LEFT JOIN expense_agg e ON e.marketplace = f.marketplace
ORDER BY marketplace;
