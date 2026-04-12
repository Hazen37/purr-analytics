-- Metabase native question export
-- card_id: 133
-- card_name: Всего покупателей
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Wildberries`

SELECT
  COUNT(DISTINCT customer_key) AS customers_total
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'wildberries'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND customer_key IS NOT NULL
  AND TRIM(customer_key) <> '';
