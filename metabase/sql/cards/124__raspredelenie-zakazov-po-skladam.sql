-- Metabase native question export
-- card_id: 124
-- card_name: Распределение заказов по складам
-- query_type: native
-- display: pie
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

SELECT
  COALESCE(NULLIF(TRIM(warehouse_name), ''), '(не указан)') AS cluster,
  COUNT(DISTINCT order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'yandex_market'
  [[AND {{order_date}}]]
  AND mo.status = 'delivered'
GROUP BY 1
ORDER BY orders_cnt DESC;
