-- Metabase native question export
-- card_id: 85
-- card_name: Marketplace / Ozon / Продано единиц
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  COALESCE(SUM(quantity), 0) AS units_total
FROM public.marketplace_order_items moi

LEFT JOIN public.products p ON p.sku::text = moi.sku
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'ozon'

AND moi.marketplace = 'ozon'
  [[AND {{flavor}}]]
  [[AND {{grams}}]];
