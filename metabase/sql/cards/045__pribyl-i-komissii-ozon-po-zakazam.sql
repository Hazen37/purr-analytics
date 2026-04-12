-- Metabase native question export
-- card_id: 45
-- card_name: Прибыль и комиссии OZON по заказам
-- query_type: native
-- display: combo
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Аналитика заказов`

WITH filtered AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day'   THEN 'day'
        WHEN {{granularity}} = 'week'  THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year'  THEN 'year'
        ELSE 'week'
      END,
      o.order_date
    )::date AS period,
    o.revenue,
    o.ozon_fees_total,
    o.order_id
  FROM public.orders_clean o
  WHERE 1=1
    [[AND {{order_date}}]]
    AND o.status = 'delivered'
    AND EXISTS (
      SELECT 1
      FROM public.order_items oi
      JOIN public.products p ON p.sku = oi.sku
      WHERE oi.order_id = o.order_id
        [[AND {{flavor}}]]
        [[AND {{grams}}]]
    )
),
agg AS (
  SELECT
    period,
    SUM(revenue) AS revenue,
    SUM(ABS(ozon_fees_total)) AS order_commissions,
    SUM(revenue) - SUM(ABS(ozon_fees_total)) AS profit_before_all_fees
  FROM filtered
  GROUP BY 1
),
bounds AS (
  SELECT MIN(period) AS min_period, MAX(period) AS max_period
  FROM agg
),
periods AS (
  SELECT
    generate_series(
      min_period,
      max_period,
      CASE
        WHEN {{granularity}} = 'day'   THEN interval '1 day'
        WHEN {{granularity}} = 'week'  THEN interval '1 week'
        WHEN {{granularity}} = 'month' THEN interval '1 month'
        WHEN {{granularity}} = 'year'  THEN interval '1 year'
        ELSE interval '1 week'
      END
    )::date AS period
  FROM bounds
  WHERE min_period IS NOT NULL AND max_period IS NOT NULL
),
series AS (
  SELECT
    p.period,
    COALESCE(a.revenue, 0) AS revenue,
    COALESCE(a.profit_before_all_fees, 0) AS profit_before_all_fees,
    COALESCE(a.order_commissions, 0) AS order_commissions
  FROM periods p
  LEFT JOIN agg a ON a.period = p.period
),
trim AS (
  SELECT
    MAX(period) FILTER (
      WHERE revenue <> 0 OR profit_before_all_fees <> 0 OR order_commissions <> 0
    ) AS last_nonzero_period
  FROM series
)
SELECT s.*
FROM series s
CROSS JOIN trim t
WHERE t.last_nonzero_period IS NULL OR s.period <= t.last_nonzero_period
ORDER BY s.period;
