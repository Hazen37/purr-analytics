SELECT
  ROUND(COALESCE(AVG(revenue), 0), 2) AS average_order_value
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'wildberries'
;
