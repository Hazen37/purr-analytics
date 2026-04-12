SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'day'
    END,
    moi.order_date
  )::date AS period,
  COALESCE(moi.status, '(не указан)') AS status,
  COALESCE(SUM(moi.quantity), 0) AS orders_cnt
FROM public.marketplace_order_items_enriched moi
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{items_marketplace}}]]
  [[AND {{flavor}}]]
  [[AND {{grams}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
