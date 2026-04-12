-- Metabase native question export
-- card_id: 86
-- card_name: Marketplace / Ozon / Средний чек
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  ROUND(COALESCE(AVG(revenue), 0), 2) AS average_order_value
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'ozon'

AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items moi
    LEFT JOIN public.products p ON p.sku::text = moi.sku
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = 'ozon'
      [[AND {{flavor}}]]
      [[AND {{grams}}]]
  );
