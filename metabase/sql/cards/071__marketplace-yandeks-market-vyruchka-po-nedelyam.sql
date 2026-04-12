-- Metabase native question export
-- card_id: 71
-- card_name: Marketplace / Яндекс Маркет / Выручка по неделям
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
  SUM(revenue) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'yandex_market'

GROUP BY 1
ORDER BY 1;
