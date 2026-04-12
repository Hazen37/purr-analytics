SELECT
  COALESCE(NULLIF(TRIM(warehouse_name), ''), '(не указан)') AS cluster,
  COUNT(DISTINCT order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'yandex_market'
  [[AND {{order_date}}]]
  AND mo.status = 'delivered'
GROUP BY 1
ORDER BY orders_cnt DESC;
