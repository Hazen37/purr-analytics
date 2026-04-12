-- Metabase native question export
-- card_id: 57
-- card_name: Инвентаризация - продажи за месяц
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Анализ`

WITH sales AS (
  SELECT
    oi.sku,
    SUM(oi.quantity) AS units_sold
  FROM public.order_items oi
  JOIN public.orders o ON o.order_id = oi.order_id
  WHERE o.status = 'delivered'
    [[AND {{month}}]]
    [[AND {{warehouse_name}}]]
  GROUP BY oi.sku
)

SELECT
  p.flavor,
  p.grams,
  p.name,
  COALESCE(s.units_sold, 0) AS units_sold
FROM public.products p
LEFT JOIN sales s ON s.sku = p.sku
ORDER BY flavor DESC, p.flavor, p.grams, p.name;
