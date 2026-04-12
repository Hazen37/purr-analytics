-- Metabase native question export
-- card_id: 63
-- card_name: Marketplace / Общее / Заказы по площадкам
-- query_type: native
-- display: bar
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
  COUNT(DISTINCT order_id) AS orders_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
