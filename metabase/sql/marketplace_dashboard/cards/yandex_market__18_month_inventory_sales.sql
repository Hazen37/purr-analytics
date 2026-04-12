SELECT
  moi.warehouse_name,
  moi.sku,
  MAX(moi.product_name) AS product_name,
  SUM(moi.quantity) AS sold_units,
  SUM(moi.item_revenue) AS revenue_total
FROM public.marketplace_order_items moi
WHERE moi.marketplace = 'yandex_market'
  [[AND {{month}}]]
  [[AND {{warehouse_name}}]]
GROUP BY 1, 2
ORDER BY sold_units DESC, revenue_total DESC
LIMIT 200;
