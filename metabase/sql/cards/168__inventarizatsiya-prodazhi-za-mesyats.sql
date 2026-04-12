-- Metabase native question export
-- card_id: 168
-- card_name: Инвентаризация - продажи за месяц
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Общее`

SELECT
  CASE
  WHEN moi.marketplace = 'ozon' THEN 'Ozon'
  WHEN moi.marketplace = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN moi.marketplace = 'wildberries' THEN 'Wildberries'
  ELSE moi.marketplace
END AS marketplace,
  moi.warehouse_name,
  moi.sku,
  MAX(moi.product_name) AS product_name,
  SUM(moi.quantity) AS sold_units,
  SUM(moi.item_revenue) AS revenue_total
FROM public.marketplace_order_items_enriched moi
WHERE 1=1
  [[AND {{month}}]]
  [[AND {{warehouse_name}}]]
  [[AND {{items_marketplace}}]]
  [[AND {{flavor}}]]
  [[AND {{grams}}]]
GROUP BY 1, 2, 3
ORDER BY sold_units DESC, revenue_total DESC
LIMIT 200;
