-- Metabase native question export
-- card_id: 102
-- card_name: Дополнительные услуги OZON
-- query_type: native
-- display: area
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Ozon`

WITH bounds AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day'   THEN 'day'
        WHEN {{granularity}} = 'week'  THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year'  THEN 'year'
        ELSE 'week'
      END,
      MIN(cost_date)
    )::date AS min_period,
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day'   THEN 'day'
        WHEN {{granularity}} = 'week'  THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year'  THEN 'year'
        ELSE 'week'
      END,
      MAX(cost_date)
    )::date AS max_period
  FROM public.finance_period_costs
  WHERE 1=1
    [[AND {{cost_date}}]]
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
fees AS (
  SELECT DISTINCT fee_name
  FROM public.finance_period_costs
  WHERE fee_name IS NOT NULL
),
grid AS (
  SELECT p.period, f.fee_name
  FROM periods p
  CROSS JOIN fees f
),
agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day'   THEN 'day'
        WHEN {{granularity}} = 'week'  THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year'  THEN 'year'
        ELSE 'week'
      END,
      cost_date
    )::date AS period,
    fee_name,
    ABS(SUM(amount)) AS amount
  FROM public.finance_period_costs
  WHERE cost_date IS NOT NULL
    [[AND {{cost_date}}]]
  GROUP BY 1, 2
)
SELECT
  g.period,
  g.fee_name,
  COALESCE(a.amount, 0) AS amount
FROM grid g
LEFT JOIN agg a
  ON a.period = g.period AND a.fee_name = g.fee_name
ORDER BY g.period, amount DESC;
