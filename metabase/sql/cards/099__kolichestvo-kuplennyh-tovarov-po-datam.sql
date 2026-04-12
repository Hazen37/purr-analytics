-- Metabase native question export
-- card_id: 99
-- card_name: Количество купленных товаров по датам
-- query_type: native
-- display: bar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Ozon`

SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day'   THEN 'day'
      WHEN {{granularity}} = 'week'  THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year'  THEN 'year'
      ELSE 'day'
    END,
    o.order_date
  )::date AS period,
  o.status,
  COUNT(DISTINCT o.order_id) AS orders_cnt
FROM public.orders_clean o
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{flavor}}]]
  [[AND {{grams}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
