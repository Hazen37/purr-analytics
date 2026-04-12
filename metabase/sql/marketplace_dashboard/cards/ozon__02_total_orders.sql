SELECT
  COUNT(DISTINCT order_id) AS orders_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'ozon'

AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items moi
    LEFT JOIN public.products p ON p.sku::text = moi.sku
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = 'ozon'
      [[AND {{flavor}}]]
      [[AND {{grams}}]]
  );
