SELECT
  COALESCE(NULLIF(TRIM(status), ''), '(не указан)') AS status_label,
  COUNT(DISTINCT order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'yandex_market'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
GROUP BY 1
ORDER BY orders_cnt DESC;
