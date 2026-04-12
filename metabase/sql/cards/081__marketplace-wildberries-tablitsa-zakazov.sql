-- Metabase native question export
-- card_id: 81
-- card_name: Marketplace / Wildberries / Таблица заказов
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
  AND mo.marketplace = 'wildberries'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{order_id_search}}, '%')]]

ORDER BY order_date DESC NULLS LAST
LIMIT 100;
