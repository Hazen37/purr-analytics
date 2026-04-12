-- Metabase native question export
-- card_id: 53
-- card_name: Распределение доставленных товаров по городам
-- query_type: native
-- display: pie
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Аналитика заказов`

SELECT
  COALESCE(NULLIF(TRIM(cluster_to), ''), '(не указан)') AS cluster,
  COUNT(*) AS orders_cnt
FROM public.orders_clean
WHERE 1=1
  AND status = 'delivered'
  [[AND {{order_date}}]]
  [[AND {{flavor}}]]
  [[AND {{grams}}]]
GROUP BY 1
ORDER BY orders_cnt DESC;
