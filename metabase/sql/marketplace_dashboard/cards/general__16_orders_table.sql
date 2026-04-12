WITH finance_by_order AS (
  SELECT marketplace, order_id, SUM(amount) AS commission_total
  FROM public.marketplace_finance_items
  GROUP BY 1, 2
)
SELECT
  mo.order_date,
  CASE
  WHEN mo.marketplace = 'ozon' THEN 'Ozon'
  WHEN mo.marketplace = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN mo.marketplace = 'wildberries' THEN 'Wildberries'
  ELSE mo.marketplace
END AS marketplace,
  mo.order_id,
  mo.status,
  mo.warehouse_name,
  mo.customer_key,
  mo.revenue,
  COALESCE(f.commission_total, 0) AS commission_total,
  mo.revenue - COALESCE(f.commission_total, 0) AS profit_total
FROM public.marketplace_orders mo
LEFT JOIN finance_by_order f
  ON f.marketplace = mo.marketplace AND f.order_id = mo.order_id
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{marketplace_name}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{order_id_search}}, '%')]]
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
ORDER BY mo.order_date DESC NULLS LAST
LIMIT 200;
