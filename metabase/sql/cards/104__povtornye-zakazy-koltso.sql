-- Metabase native question export
-- card_id: 104
-- card_name: Повторные заказы - Кольцо
-- query_type: native
-- display: pie
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Ozon`

WITH customers_max AS (
  SELECT
    o.customer_id,
    MAX(o.order_num_delivered) AS delivered_orders_cnt
  FROM orders_clean o
  WHERE {{order_date}}
    AND o.status = 'delivered'
    AND o.customer_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM order_items oi
      JOIN products p ON p.sku = oi.sku
      WHERE oi.order_id = o.order_id
        [[AND {{flavor}}]]
        [[AND {{grams}}]]
    )
  GROUP BY o.customer_id
)
SELECT
  delivered_orders_cnt AS orders_count,
  COUNT(*) AS customers
FROM customers_max
GROUP BY delivered_orders_cnt
ORDER BY delivered_orders_cnt;
