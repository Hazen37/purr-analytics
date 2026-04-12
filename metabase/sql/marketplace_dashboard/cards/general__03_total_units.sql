SELECT
  COALESCE(SUM(quantity), 0) AS units_total
FROM public.marketplace_order_items moi

WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]

;
