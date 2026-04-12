-- Metabase native question export
-- card_id: 144
-- card_name: Таблица заказов
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Wildberries`

WITH finance_by_order AS (
  SELECT order_id, SUM(amount) AS commission_total
  FROM public.marketplace_finance_items
  WHERE marketplace = 'wildberries'
  GROUP BY 1
)
SELECT
  mo.order_date,
  mo.order_id,
  mo.status,
  mo.warehouse_name,
  mo.customer_key,
  mo.revenue,
  COALESCE(f.commission_total, 0) AS commission_total,
  mo.revenue - COALESCE(f.commission_total, 0) AS profit_total
FROM public.marketplace_orders mo
LEFT JOIN finance_by_order f ON f.order_id = mo.order_id
WHERE mo.marketplace = 'wildberries'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{order_id_search}}, '%')]]
ORDER BY mo.order_date DESC NULLS LAST
LIMIT 200;
