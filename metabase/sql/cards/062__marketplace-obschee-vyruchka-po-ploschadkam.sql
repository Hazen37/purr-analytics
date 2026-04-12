-- Metabase native question export
-- card_id: 62
-- card_name: Marketplace / Общее / Выручка по площадкам
-- query_type: native
-- display: line
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

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
  marketplace,
  SUM(revenue) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
