-- Metabase native question export
-- card_id: 87
-- card_name: Marketplace / Ozon / Выручка по неделям
-- query_type: native
-- display: line
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  date_trunc(
    CASE
      WHEN {granularity} = 'day' THEN 'day'
      WHEN {granularity} = 'week' THEN 'week'
      WHEN {granularity} = 'month' THEN 'month'
      WHEN {granularity} = 'year' THEN 'year'
      ELSE 'week'
    END,
    mo.order_date
  )::date AS period,
  SUM(revenue) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {order_date}]]
  [[AND {status}]]
  AND marketplace = 'ozon'
AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items moi
    LEFT JOIN public.products p ON p.sku::text = moi.sku
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = 'ozon'
      [[AND {{flavor}}]]
      [[AND {{grams}}]]
  )
GROUP BY 1
ORDER BY 1;
