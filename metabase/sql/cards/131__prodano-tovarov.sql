-- Metabase native question export
-- card_id: 131
-- card_name: Продано товаров
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Wildberries`

SELECT
  COALESCE(SUM(quantity), 0) AS units_total
FROM public.marketplace_order_items moi

WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'wildberries'
;
