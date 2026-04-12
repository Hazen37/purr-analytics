-- Metabase native question export
-- card_id: 77
-- card_name: Marketplace / Wildberries / Продано единиц
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  COALESCE(SUM(quantity), 0) AS units_total
FROM public.marketplace_order_items moi

WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'wildberries'
;
