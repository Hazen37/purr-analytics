-- Metabase native question export
-- card_id: 141
-- card_name: Статусы заказов - кольцо
-- query_type: native
-- display: pie
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Wildberries`

SELECT
  COALESCE(NULLIF(TRIM(status), ''), '(не указан)') AS status_label,
  COUNT(DISTINCT order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'wildberries'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
GROUP BY 1
ORDER BY orders_cnt DESC;
