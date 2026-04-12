SELECT
  mo.order_date,
  mo.order_id,
  mo.status,
  mo.revenue,
  mo.warehouse_name,
  mo.customer_key
FROM public.marketplace_orders mo
WHERE 1=1
  AND mo.marketplace = 'yandex_market'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{order_id_search}}, '%')]]

ORDER BY order_date DESC NULLS LAST
LIMIT 100;
