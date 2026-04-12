-- Metabase native question export
-- card_id: 148
-- card_name: Инвентаризация - продажи за месяц
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Wildberries`

SELECT
  moi.warehouse_name,
  moi.sku,
  MAX(moi.product_name) AS product_name,
  SUM(moi.quantity) AS sold_units,
  SUM(moi.item_revenue) AS revenue_total
FROM public.marketplace_order_items moi
WHERE moi.marketplace = 'wildberries'
  [[AND {{month}}]]
  [[AND {{warehouse_name}}]]
GROUP BY 1, 2
ORDER BY sold_units DESC, revenue_total DESC
LIMIT 200;
