WITH base AS (
  SELECT
    mo.order_id,
    mo.order_date,
    mo.status,
    regexp_replace(mo.order_id::text, '-\d+$', '') AS group_key,
    CASE
      WHEN mo.marketplace = 'ozon' AND mo.customer_key IS NOT NULL AND TRIM(mo.customer_key) <> '' THEN mo.customer_key
      WHEN mo.order_id LIKE '%-%' THEN split_part(regexp_replace(mo.order_id::text, '-\d+$', ''), '-', 1)
      WHEN mo.order_id LIKE '%_%' THEN split_part(regexp_replace(mo.order_id::text, '_\d+$', ''), '_', 1)
      ELSE COALESCE(NULLIF(TRIM(mo.customer_key), ''), mo.order_id)
    END AS customer_guess
  FROM public.marketplace_orders mo
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
),
groups AS (
  SELECT
    customer_guess,
    group_key,
    MIN(order_date) AS group_first_date,
    BOOL_OR(status = 'delivered') AS has_delivered
  FROM base
  WHERE status <> 'cancelled'
  GROUP BY 1, 2
),
delivered_groups AS (
  SELECT
    customer_guess,
    DENSE_RANK() OVER (
      PARTITION BY customer_guess
      ORDER BY group_first_date, group_key
    ) AS delivered_rank
  FROM groups
  WHERE has_delivered
),
customers_max AS (
  SELECT customer_guess, MAX(delivered_rank) AS orders_count
  FROM delivered_groups
  GROUP BY 1
)
SELECT
  orders_count,
  COUNT(*) AS customers
FROM customers_max
GROUP BY 1
ORDER BY 1;
