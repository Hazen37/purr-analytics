SELECT
  COALESCE(SUM(quantity), 0) AS units_total
FROM public.marketplace_order_items moi

LEFT JOIN public.products p ON p.sku::text = moi.sku
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'ozon'

AND moi.marketplace = 'ozon'
  [[AND {{flavor}}]]
  [[AND {{grams}}]];
