-- Metabase native question export
-- card_id: 50
-- card_name: Таблица заказов
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Таблица заказов`

-- Metabase native question export
-- card_id: 50
-- card_name: Таблица заказов
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T08:34:21Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Таблица заказов`

WITH base AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    COALESCE(o.order_group_id, o.order_id) AS group_key
  FROM public.orders_clean o
),
groups AS (
  SELECT
    customer_id,
    group_key,
    MIN(order_date) AS group_first_date,
    BOOL_OR(status = 'delivered') AS has_delivered
  FROM base
  WHERE status <> 'cancelled'
  GROUP BY 1, 2
),
delivered_groups AS (
  SELECT
    customer_id,
    group_key,
    group_first_date,
    DENSE_RANK() OVER (
      PARTITION BY customer_id
      ORDER BY group_first_date, group_key
    ) AS delivered_rank
  FROM groups
  WHERE has_delivered
),
timeline AS (
  SELECT
    g.customer_id,
    g.group_key,
    g.group_first_date,
    dg.delivered_rank,
    MAX(dg.delivered_rank) OVER (
      PARTITION BY g.customer_id
      ORDER BY g.group_first_date, g.group_key
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS last_delivered_rank
  FROM groups g
  LEFT JOIN delivered_groups dg
    ON dg.customer_id = g.customer_id
   AND dg.group_key    = g.group_key
),
group_nums AS (
  SELECT
    t.customer_id,
    t.group_key,
    CASE
      WHEN t.delivered_rank IS NOT NULL THEN t.delivered_rank
      ELSE COALESCE(t.last_delivered_rank, 0)
           + SUM(1) OVER (
               PARTITION BY t.customer_id, COALESCE(t.last_delivered_rank, 0)
               ORDER BY t.group_first_date, t.group_key
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             )
    END AS computed_group_num
  FROM timeline t
)

SELECT
  o.order_id,
  o.customer_id,
  o.order_group_id,
  o.order_date,
  o.status,

  -- доставка
  o.delivery_city,
  o.warehouse_name,
  o.cluster_to,

  -- промо/акции
  o.promo_code,
  o.ozon_actions,

  -- финансы
  o.revenue,
  o.ozon_fees_total,
  o.ozon_payout,
  o.campaign,
  UPPER(COALESCE(o.fulfillment_type, 'unknown')) AS fulfillment_type,
  o.order_num_delivered,

  CASE
    WHEN o.status <> 'cancelled'
     AND o.order_group_num_delivered IS NULL
    THEN gn.computed_group_num
    ELSE o.order_group_num_delivered
  END AS order_group_num_delivered,

  -- товары
  oi.sku,
  oi.quantity,
  oi.price,
  oi.revenue AS item_revenue,
  p.flavor,
  p.grams
FROM public.orders_clean o
LEFT JOIN public.order_items oi
  ON oi.order_id = o.order_id
LEFT JOIN public.products p
  ON p.sku = oi.sku
LEFT JOIN group_nums gn
  ON gn.customer_id = o.customer_id
 AND gn.group_key    = COALESCE(o.order_group_id, o.order_id)

WHERE 1=1
  [[AND {{order_date}}]]
  [[AND CAST(o.order_id AS text) ILIKE CONCAT('%', {{order_id_search}}, '%')]]
ORDER BY o.order_date DESC;
