SELECT
  COUNT(DISTINCT customer_key) AS customers_total
FROM public.marketplace_orders mo
WHERE mo.marketplace = 'wildberries'
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND customer_key IS NOT NULL
  AND TRIM(customer_key) <> '';
