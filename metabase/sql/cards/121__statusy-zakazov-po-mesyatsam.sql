-- Metabase native question export
-- card_id: 121
-- card_name: Статусы заказов по месяцам
-- query_type: native
-- display: bar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'month'
    END,
    mo.order_date
  )::date AS period,
  COUNT(DISTINCT mo.order_id) FILTER (WHERE mo.status = 'delivered') AS first_orders,
  COUNT(DISTINCT mo.order_id) FILTER (WHERE mo.status IS DISTINCT FROM 'delivered') AS repeat_orders
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'yandex_market'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
GROUP BY 1
ORDER BY 1;
