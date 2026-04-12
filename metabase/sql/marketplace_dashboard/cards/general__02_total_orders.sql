WITH base_orders AS (
  SELECT
    mo.marketplace,
    mo.order_id,
    CASE
      WHEN mo.marketplace = 'ozon' THEN COALESCE(o.order_group_id, regexp_replace(mo.order_id::text, '-\d+$', ''))
      ELSE regexp_replace(mo.order_id::text, '[-_]\d+$', '')
    END AS group_key
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{status}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
)
SELECT COUNT(DISTINCT group_key) AS orders_total
FROM base_orders;
