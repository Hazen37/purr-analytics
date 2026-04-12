SELECT
  COALESCE(SUM(revenue), 0) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]

;
