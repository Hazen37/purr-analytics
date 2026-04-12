-- Metabase native question export
-- card_id: 48
-- card_name: Первые и повторные заказы по месяцам
-- query_type: native
-- display: bar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Аналитика заказов`

SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day'   THEN 'day'
      WHEN {{granularity}} = 'week'  THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year'  THEN 'year'
      ELSE 'month'
    END,
    o.order_date
  )::date AS period,

  COUNT(DISTINCT COALESCE(o.order_group_id, o.order_id))
    FILTER (WHERE o.order_group_num_delivered = 1) AS first_orders,

  COUNT(DISTINCT COALESCE(o.order_group_id, o.order_id))
    FILTER (WHERE o.order_group_num_delivered > 1) AS repeat_orders

FROM public.orders_clean o
WHERE 1=1
  [[AND {{order_date}}]]
  AND o.status <> 'cancelled'

  [[AND EXISTS (
      SELECT 1
      FROM public.order_items oi
      JOIN public.products p ON p.sku = oi.sku
      WHERE oi.order_id = o.order_id
        AND {{flavor}}
  )]]

  [[AND EXISTS (
      SELECT 1
      FROM public.order_items oi
      JOIN public.products p ON p.sku = oi.sku
      WHERE oi.order_id = o.order_id
        AND {{grams}}
  )]]

GROUP BY 1
ORDER BY 1;
