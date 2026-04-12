-- Metabase native question export
-- card_id: 162
-- card_name: Распределение заказов по площадкам
-- query_type: native
-- display: pie
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Общее`

SELECT
  CASE
  WHEN mo.marketplace = 'ozon' THEN 'Ozon'
  WHEN mo.marketplace = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN mo.marketplace = 'wildberries' THEN 'Wildberries'
  ELSE mo.marketplace
END AS marketplace,
  COUNT(DISTINCT mo.order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]]
  AND mo.status = 'delivered'
GROUP BY 1
ORDER BY orders_cnt DESC;
