-- Metabase native question export
-- card_id: 156
-- card_name: Прибыль и комиссии по всем площадкам
-- query_type: native
-- display: combo
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Общее`

WITH orders_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mo.order_date
    )::date AS period,
    SUM(mo.revenue) AS revenue
  FROM public.marketplace_orders mo
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
    AND mo.status = 'delivered'
  GROUP BY 1
),
finance_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mfi.happened_at
    )::date AS period,
    SUM(mfi.amount) AS order_commissions
  FROM public.marketplace_finance_items mfi
  WHERE 1=1
    [[AND {{happened_at}}]]
    [[AND {{finance_marketplace}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mfi.order_id
        AND moi.marketplace = mfi.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mfi.order_id
        AND moi.marketplace = mfi.marketplace
        AND {{grams}}
    )]]
  GROUP BY 1
),
periods AS (
  SELECT period FROM orders_agg
  UNION
  SELECT period FROM finance_agg
)
SELECT
  p.period,
  COALESCE(o.revenue, 0) AS revenue,
  COALESCE(o.revenue, 0) - COALESCE(f.order_commissions, 0) AS profit_before_all_fees,
  COALESCE(f.order_commissions, 0) AS order_commissions
FROM periods p
LEFT JOIN orders_agg o ON o.period = p.period
LEFT JOIN finance_agg f ON f.period = p.period
ORDER BY p.period;
