-- Metabase native question export
-- card_id: 89
-- card_name: Marketplace / Ozon / Таблица заказов
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  mo.order_date,
  mo.order_id,
  mo.status,
  mo.revenue,
  mo.warehouse_name,
  mo.customer_key
FROM public.marketplace_orders mo
WHERE 1=1
  AND mo.marketplace = 'ozon'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{order_id_search}}, '%')]]
AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items moi
    LEFT JOIN public.products p ON p.sku::text = moi.sku
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = 'ozon'
      [[AND {{flavor}}]]
      [[AND {{grams}}]]
  )
ORDER BY order_date DESC NULLS LAST
LIMIT 100;
