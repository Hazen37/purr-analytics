-- Metabase native question export
-- card_id: 160
-- card_name: Первые и повторные заказы по месяцам
-- query_type: native
-- display: bar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Общее`

WITH base AS (
  SELECT
    mo.marketplace,
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
    group_key,
    group_first_date,
    DENSE_RANK() OVER (
      PARTITION BY customer_guess
      ORDER BY group_first_date, group_key
    ) AS delivered_rank
  FROM groups
  WHERE has_delivered
)
SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'month'
    END,
    group_first_date
  )::date AS period,
  COUNT(*) FILTER (WHERE delivered_rank = 1) AS first_orders,
  COUNT(*) FILTER (WHERE delivered_rank > 1) AS repeat_orders
FROM delivered_groups
GROUP BY 1
ORDER BY 1;
