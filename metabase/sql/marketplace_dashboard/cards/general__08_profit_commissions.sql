WITH base_orders AS (
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
    mo.marketplace,
    mo.order_id,
    mo.revenue,
    o.ozon_payout,
    o.ozon_fees_total
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
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
),
order_finance AS (
  SELECT
    b.marketplace,
    b.order_id,
    CASE
      WHEN b.marketplace = 'ozon' THEN ABS(COALESCE(MAX(b.ozon_fees_total), 0))
      ELSE COALESCE(SUM(mfi.amount), 0)
    END AS order_commissions
  FROM base_orders b
  LEFT JOIN public.marketplace_finance_items mfi
    ON mfi.marketplace = b.marketplace
   AND mfi.order_id = b.order_id
  GROUP BY 1, 2
),
period_agg AS (
  SELECT
    b.period,
    SUM(b.revenue) AS revenue,
    SUM(
      CASE
        WHEN b.marketplace = 'ozon' THEN b.revenue - ABS(COALESCE(b.ozon_fees_total, 0))
        ELSE b.revenue - COALESCE(f.order_commissions, 0)
      END
    ) AS profit_before_all_fees,
    SUM(
      CASE
        WHEN b.marketplace = 'ozon' THEN ABS(COALESCE(b.ozon_fees_total, 0))
        ELSE COALESCE(f.order_commissions, 0)
      END
    ) AS order_commissions
  FROM base_orders b
  LEFT JOIN order_finance f
    ON f.marketplace = b.marketplace
   AND f.order_id = b.order_id
  GROUP BY 1
),
bounds AS (
  SELECT MIN(period) AS min_period, MAX(period) AS max_period
  FROM period_agg
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
  LEFT JOIN period_agg a ON a.period = p.period
),
trim AS (
  SELECT
    MAX(period) FILTER (
      WHERE revenue <> 0 OR profit_before_all_fees <> 0 OR order_commissions <> 0
    ) AS last_nonzero_period
  FROM series
)
SELECT
  s.period,
  s.revenue,
  s.profit_before_all_fees,
  s.order_commissions
FROM series s
CROSS JOIN trim t
WHERE t.last_nonzero_period IS NULL OR s.period <= t.last_nonzero_period
ORDER BY s.period;
