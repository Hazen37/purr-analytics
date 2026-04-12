SELECT
  COALESCE(NULLIF(TRIM(mo.warehouse_name), ''), '(не указан)') AS cluster,
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
